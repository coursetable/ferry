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

from ferry.ai import DEFAULT_MODEL, LLMClient
from ferry.crawler.cache import load_cache_json, save_cache_json

# Minimum number of comments required to generate a summary
MIN_COMMENTS_FOR_SUMMARY = 3

# Maximum concurrent API requests to avoid rate-limit pressure.
MAX_CONCURRENT_REQUESTS = 10

# Cap courses per season for initial testing (set to None for no limit).
MAX_COURSES_PER_SEASON = 100

SYSTEM_PROMPT = """
You are an expert at summarizing student course evaluations for a university
course catalog. You will receive a set of student comments responding to a
specific evaluation question for a single course.

Your task:
- Produce a concise summary (2-4 sentences) that captures the key themes,
  consensus opinions, and notable dissenting views.
- Write in the third person (e.g. "Students felt…", "Many noted…").
- Be objective and balanced — reflect both positive and negative sentiments.
- Do NOT quote individual comments verbatim.
- Do NOT include any preamble or meta-commentary; return only the summary text.
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
        except Exception as exc:
            logging.warning(
                "Failed to summarize %s/%s crn=%s: %s",
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
) -> None:
    """
    Summarize narrative evaluations for the given seasons.

    Uses any OpenAI-compatible chat API. Pass ``base_url`` for providers like
    Groq (https://api.groq.com/openai/v1), OpenRouter, etc. Reads from
    ``data_dir/parsed_evaluations/{season}.json`` and writes summaries to
    ``data_dir/evaluation_summaries/{season}.json``.
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
    print(
        f"\nSummarizing evaluations for seasons: {seasons} "
        + f"(model: {model}{base_info})"
    )

    for season in seasons:
        parsed_path = data_dir / "parsed_evaluations" / f"{season}.json"
        output_path = output_dir / f"{season}.json"

        # Load existing summaries so we can skip already-summarized courses.
        existing_summaries: list[CourseSummary] = (
            load_cache_json(output_path) or []
        )
        already_done: set[str] = {str(s["crn"]) for s in existing_summaries}

        # Load parsed evaluations for this season.
        course_evals: list[dict[str, Any]] | None = load_cache_json(
            parsed_path
        )
        if course_evals is None:
            print(
                f"No parsed evaluations found for season {season}, skipping.")
            continue

        # Filter to courses that still need summarization.
        to_process = [
            c
            for c in course_evals
            if str(c["crn"]) not in already_done and c.get("narratives")
        ]
        if MAX_COURSES_PER_SEASON is not None:
            to_process = to_process[:MAX_COURSES_PER_SEASON]

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

        for course_eval in tqdm(
            to_process, desc=f"Summarizing {season}", leave=False
        ):
            result = await _summarize_course(llm, course_eval, semaphore)
            if result is not None:
                new_summaries.append(result)
                for ns in result["narrative_summaries"]:
                    line = (
                        f"[{result['season']}] crn={result['crn']} "
                        + f"{ns['question_code']}: {ns['summary']}"
                    )
                    print(line)

        # Merge new results with any existing ones and write back.
        all_summaries = existing_summaries + new_summaries
        all_summaries.sort(key=lambda s: str(s["crn"]))

        save_cache_json(output_path, all_summaries)

        print(
            f"Season {season}: wrote {len(all_summaries)} course summaries "
            + f"({len(new_summaries)} new)."
        )

    print("Evaluation summarization complete. ✔")
