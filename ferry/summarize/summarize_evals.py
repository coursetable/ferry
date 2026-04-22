"""
Summarize narrative course evaluations using an LLM API.

Uses any OpenAI-compatible chat API (OpenAI, Groq, OpenRouter, Anthropic
compatibility layer, etc.). For each season, reads the parsed evaluation JSON,
groups narrative comments by course and question, and produces a concise
AI-generated summary for each narrative question. Results are written to
`evaluation_summaries/{season}.json`.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, TypedDict

from tqdm import tqdm

from openai import RateLimitError

from ferry.ai import DEFAULT_MODEL, LLMClient
from ferry.crawler.cache import load_cache_json, save_cache_json

# Minimum number of comments required to generate a summary
MIN_COMMENTS_FOR_SUMMARY = 3

# Maximum concurrent API requests to avoid rate-limit pressure.
MAX_CONCURRENT_REQUESTS = 10

SYSTEM_PROMPT = """
You are an expert at synthesizing student course evaluations for publication in a university course catalog. You will receive a set of student comments responding to a single evaluation question for one course.

Your task
Produce a concise summary (2-4 sentences) that accurately represents the aggregate student perspective on the question asked.

Content requirements
- Capture the dominant themes: Identify what most students agree on and lead with that.
- Note meaningful dissent: If a substantial minority holds a different view, include it. Ignore one-off outliers that don't represent a real pattern.
- Reflect sentiment proportionally: If 80% of comments are positive, the summary should read as clearly positive. If reviews are mixed, the summary should feel mixed. Do not soften genuinely negative feedback or inflate lukewarm praise.
- Be specific where possible: Prefer concrete themes ("students found the problem sets challenging but fair") over vague generalities ("students had various opinions").

Style requirements
- Write in the third person, referring to students collectively ("Students reported…", "Many found…", "A minority felt…").
- Use hedged quantifiers that match the actual distribution: "nearly all," "most," "many," "several," "a few." Avoid "some" as it's ambiguous.
- Do not quote comments verbatim or reproduce distinctive phrasing; paraphrase in neutral language.
- Do not name or identify individual students, instructors, or TAs, even if named in comments.
- Remain neutral in tone; do not editorialize or add recommendations.

Output format
Return only the summary text. No preamble, headers, labels, or meta-commentary (e.g., do not write "Summary:" or "Here is the summary:").

Edge cases
- Very few comments (1-3): Still summarize, but use appropriately tentative language ("The few responses received indicated…").
- Contradictory comments: Present the split honestly rather than picking a side.
- Off-topic comments: Ignore comments that don't address the evaluation question.
- Offensive or inappropriate content: Omit it from the summary; do not reproduce or reference it.
"""


class NarrativeSummary(TypedDict):
    question_code: str
    question_text: str
    summary: str


class CourseSummary(TypedDict):
    crn: str
    season: str
    narrative_summaries: list[NarrativeSummary]


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------


async def _summarize_comments(
    llm: LLMClient,
    question_text: str,
    comments: list[str],
    semaphore: asyncio.Semaphore,
) -> str:
    """Call the LLM API to summarize a list of student comments."""
    user_content = (
        f"Evaluation question: {question_text}\n\n"
        + f"Student comments ({len(comments)} total):\n"
        + "\n---\n".join(comments)
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    async with semaphore:
        return await llm.complete(
            messages,
            temperature=0.3,
            max_tokens=300,
        )


async def _summarize_course(
    llm: LLMClient,
    course_eval: dict[str, Any],
    semaphore: asyncio.Semaphore,
) -> CourseSummary | None:
    """Produce summaries for all narrative questions of a single course."""
    narratives: list[dict[str, Any]] = course_eval.get("narratives", [])
    if not narratives:
        return None

    summaries: list[NarrativeSummary] = []

    tasks: list[tuple[str, str, asyncio.Task[str]]] = []
    for narrative in narratives:
        comments: list[str] = narrative.get("comments", [])
        if len(comments) < MIN_COMMENTS_FOR_SUMMARY:
            continue

        question_code: str = narrative["question_code"]
        question_text: str = narrative["question_text"]

        task = asyncio.create_task(
            _summarize_comments(llm, question_text, comments, semaphore)
        )
        tasks.append((question_code, question_text, task))

    if not tasks:
        return None

    for question_code, question_text, task in tasks:
        try:
            summary_text = await task
            summaries.append(
                {
                    "question_code": question_code,
                    "question_text": question_text,
                    "summary": summary_text,
                }
            )
        except RateLimitError as exc:
            logging.warning(
                "Rate limit exceeded after retries for %s/%s crn=%s: %s. "
                + "Skipping this narrative; partial results to be committed.",
                course_eval.get("season"),
                question_code,
                course_eval.get("crn"),
                exc,
            )

    if not summaries:
        return None

    return {
        "crn": str(course_eval["crn"]),
        "season": course_eval["season"],
        "narrative_summaries": summaries,
    }


async def summarize_evals(
    *,
    seasons: list[str],
    data_dir: Path,
    api_key: str,
    model: str = DEFAULT_MODEL,
    base_url: str | None = None,
    max_courses_per_season: int | None = None,
) -> None:
    """
    Summarize narrative evaluations for the given seasons.

    Uses any OpenAI-compatible chat API. Pass ``base_url`` for providers like
    Groq (https://api.groq.com/openai/v1), OpenRouter, etc. Reads from
    ``data_dir/parsed_evaluations/{season}.json`` and writes summaries to
    ``data_dir/evaluation_summaries/{season}.json``.

    When ``max_courses_per_season`` is set, limits how many courses are
    processed per season.
    """
    llm = LLMClient(
        api_key=api_key,
        base_url=base_url,
        model=model,
    )
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    output_dir = data_dir / "evaluation_summaries"
    output_dir.mkdir(parents=True, exist_ok=True)

    base_info = f", base: {base_url}" if base_url else ""
    limit_info = (
        f", max {max_courses_per_season} courses/season"
        if max_courses_per_season
        else ""
    )
    print(
        f"\nSummarizing evaluations for seasons: {seasons} "
        + f"(model: {model}{base_info}{limit_info})"
    )

    for season in seasons:
        parsed_path = data_dir / "parsed_evaluations" / f"{season}.json"
        output_path = output_dir / f"{season}.json"

        # Load existing summaries so we can skip already-summarized courses.
        existing_summaries: list[CourseSummary] = load_cache_json(output_path) or []
        already_done: set[str] = {str(s["crn"]) for s in existing_summaries}

        # Load parsed evaluations for this season.
        course_evals: list[dict[str, Any]] | None = load_cache_json(parsed_path)
        if course_evals is None:
            print(f"No parsed evaluations found for season {season}, skipping.")
            continue

        # Filter to courses that still need summarization.
        to_process = [
            c
            for c in course_evals
            if str(c["crn"]) not in already_done and c.get("narratives")
        ]
        if max_courses_per_season is not None:
            to_process = to_process[:max_courses_per_season]

        if not to_process:
            print(
                f"Season {season}: all {len(existing_summaries)} courses "
                + "already summarized."
            )
            continue

        print(
            f"Season {season}: {len(to_process)} courses to summarize "
            + f"({len(already_done)} already done)."
        )

        new_summaries: list[CourseSummary] = []

        for course_eval in tqdm(to_process, desc=f"Summarizing {season}", leave=True):
            result = await _summarize_course(llm, course_eval, semaphore)
            if result is not None:
                new_summaries.append(result)

        # Merge new results with any existing ones and write back.
        all_summaries = existing_summaries + new_summaries
        all_summaries.sort(key=lambda s: str(s["crn"]))

        save_cache_json(output_path, all_summaries)

        print(
            f"Season {season}: wrote {len(all_summaries)} course summaries "
            + f"({len(new_summaries)} new)."
        )

    print("Evaluation summarization complete. ✔")
