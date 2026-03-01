"""
Summarize narrative course evaluations using the OpenAI API.

For each season, reads the parsed evaluation JSON, groups narrative comments
by course and question, and produces a concise AI-generated summary for each
narrative question. Results are written to `evaluation_summaries/{season}.json`.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, TypedDict

import ujson
from tqdm import tqdm

from ferry.crawler.cache import load_cache_json, save_cache_json

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Minimum number of comments required to generate a summary.
# Courses with fewer comments than this are skipped.
MIN_COMMENTS_FOR_SUMMARY = 3

# OpenAI model to use for summarization.
OPENAI_MODEL = "gpt-4.1-mini"

# Maximum concurrent API requests to avoid rate-limit pressure.
MAX_CONCURRENT_REQUESTS = 10

# Cap courses per season for testing (set to None for no limit).
MAX_COURSES_PER_SEASON = 10

SYSTEM_PROMPT = """\
You are an expert at summarizing student course evaluations for a university \
course catalog. You will receive a set of student comments responding to a \
specific evaluation question for a single course.

Your task:
- Produce a concise summary (2-4 sentences) that captures the key themes, \
  consensus opinions, and notable dissenting views.
- Write in the third person (e.g. "Students felt…", "Many noted…").
- Be objective and balanced — reflect both positive and negative sentiments.
- Do NOT quote individual comments verbatim.
- Do NOT include any preamble or meta-commentary; return only the summary text.\
"""


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class NarrativeSummary(TypedDict):
    question_code: str
    question_text: str
    summary: str


class CourseSummary(TypedDict):
    crn: str
    season: str
    narrative_summaries: list[NarrativeSummary]


# ---------------------------------------------------------------------------
# OpenAI helpers
# ---------------------------------------------------------------------------


async def _summarize_comments(
    openai_client: Any,
    question_text: str,
    comments: list[str],
    semaphore: asyncio.Semaphore,
) -> str:
    """Call the OpenAI API to summarize a list of student comments."""
    user_content = (
        f"Evaluation question: {question_text}\n\n"
        f"Student comments ({len(comments)} total):\n"
        + "\n---\n".join(comments)
    )

    async with semaphore:
        response = await openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.3,
            max_tokens=300,
        )

    return response.choices[0].message.content.strip()


async def _summarize_course(
    openai_client: Any,
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
            _summarize_comments(openai_client, question_text, comments, semaphore)
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
        "crn": course_eval["crn"],
        "season": course_eval["season"],
        "narrative_summaries": summaries,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def summarize_evals(
    *,
    seasons: list[str],
    data_dir: Path,
    openai_api_key: str,
) -> None:
    """
    Summarize narrative evaluations for the given seasons.

    Reads from ``data_dir/parsed_evaluations/{season}.json`` and writes
    summaries to ``data_dir/evaluation_summaries/{season}.json``.
    """
    from openai import AsyncOpenAI

    openai_client = AsyncOpenAI(api_key=openai_api_key)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    output_dir = data_dir / "evaluation_summaries"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nSummarizing evaluations for seasons: {seasons}")

    for season in seasons:
        parsed_path = data_dir / "parsed_evaluations" / f"{season}.json"
        output_path = output_dir / f"{season}.json"

        # Load existing summaries so we can skip already-summarised courses.
        existing_summaries: list[CourseSummary] = (
            load_cache_json(output_path) or []
        )
        already_done: set[str] = {s["crn"] for s in existing_summaries}

        # Load parsed evaluations for this season.
        course_evals: list[dict[str, Any]] | None = load_cache_json(parsed_path)
        if course_evals is None:
            print(f"  No parsed evaluations found for season {season}, skipping.")
            continue

        # Filter to courses that still need summarisation.
        to_process = [
            c
            for c in course_evals
            if c["crn"] not in already_done and c.get("narratives")
        ]
        if MAX_COURSES_PER_SEASON is not None:
            to_process = to_process[:MAX_COURSES_PER_SEASON]

        if not to_process:
            print(f"  Season {season}: all {len(existing_summaries)} courses already summarised.")
            continue

        print(
            f"  Season {season}: {len(to_process)} courses to summarise "
            f"({len(already_done)} already done)."
        )

        new_summaries: list[CourseSummary] = []

        for course_eval in tqdm(
            to_process, desc=f"Summarising {season}", leave=False
        ):
            result = await _summarize_course(openai_client, course_eval, semaphore)
            if result is not None:
                new_summaries.append(result)
                for ns in result["narrative_summaries"]:
                    print(f"  [{result['season']}] crn={result['crn']} {ns['question_code']}: {ns['summary']}")

        # Merge new results with any existing ones and write back.
        all_summaries = existing_summaries + new_summaries
        all_summaries.sort(key=lambda s: s["crn"])

        save_cache_json(output_path, all_summaries)

        print(
            f"  Season {season}: wrote {len(all_summaries)} course summaries "
            f"({len(new_summaries)} new)."
        )

    print("Evaluation summarisation complete. ✔")
