"""
Migration utilities for moving from old CourseTable class JSONs.

Used by scripts under /ferry/migration/.
"""

from typing import Any, Dict, List, Tuple


def convert_old_description(old_description: str) -> str:
    """
    Format old course descriptions.

    Parameters
    ----------
    old_description:
        input course description

    Returns
    -------
    description:
        formatted description closer to the new parser
    """
    if old_description[:10] == "Cancelled.":
        old_description = old_description[10:]

    old_description = old_description.replace("&quot;", '"')

    return old_description


def convert_old_time(time: str, revert_12hour=False, truncate_minute=False) -> str:
    """
    Convert previous float-formatted times to 24-hour, full format

    Parameters
    ----------
    time:
        a time from the previous CourseTable format
    revert_12hour:
        whether or not to convert back to 12-hour format
    truncate_minute:
        whether or not to remove the minute if it is :00

    Returns
    -------
    time: string
        formatted time
    """
    if "." in time:

        hour = time.split(".")[0]
        minute = time.split(".")[1]

        if len(minute) == 1:
            minute = minute + "0"

    else:

        hour = time
        minute = "00"

    formatted_time = f"{hour}:{minute}"

    if truncate_minute and minute == "00":
        formatted_time = formatted_time.split(":")[0]

    if revert_12hour:

        hour_num = int(hour)

        if hour_num > 12:
            hour_num = hour_num - 12

            formatted_time = f"{str(hour_num)}:{minute}pm"

        elif hour_num == 12:

            formatted_time = f"{str(hour_num)}:{minute}pm"

        elif hour_num == 0:

            hour_num = 12

            formatted_time = f"{str(hour_num)}:{minute}am"

        else:

            formatted_time = f"{str(hour_num)}:{minute}am"

        if truncate_minute and minute == "00":

            formatted_time = formatted_time.split(":")[0] + formatted_time[-2:]

    return formatted_time


def convert_old_meetings(
    times: Dict[str, Any]
) -> Tuple[str, str, Dict[str, List[List[Any]]]]:
    """
    Convert previous meeting times format to new one

    Parameters
    ----------
    times:
        previous 'times' field, with keys "summary", "long_summary", "by_day"

    Returns
    -------
    new_times_summary:
        reformatted "summary" field
    new_times_long_summary:
        reformatted "long_summary" field
    by_day:
        reformatted "by_day" field
    """
    times_summary = times["summary"]
    times_long_summary = times["long_summary"]
    times_by_day = times["by_day"]

    # ---------------------
    # process times summary
    # ---------------------

    # unknown times
    if times == ["HTBA"] or times_summary in ["1 HTBA", "2 HTBA", "HTBA", ""]:

        return "TBA", "TBA", {}

    times_summary = times_summary.split(" ")
    start_end = times_summary[1].split("-")

    # convert 24-hour float-based time formats to colon-based 12 hour ones
    times_start = convert_old_time(
        start_end[0], revert_12hour=True, truncate_minute=True
    )
    times_end = convert_old_time(start_end[1], revert_12hour=True, truncate_minute=True)

    # reconstruct summary string
    start_end = times_start + "-" + times_end
    times_summary[1] = start_end

    new_times_summary = " ".join(times_summary)

    # -----------------------------------
    # process time-locations long summary
    # -----------------------------------

    split_long_summary = times_long_summary.split(", ")

    new_times_long_summary = []

    for summary in split_long_summary:

        if summary in ["1 HTBA", "2 HTBA", "HTBA", ""]:

            pass

        else:

            summary = summary.split(" ")
            start_end = summary[1].split("-")

            # convert 24-hour float-based time formats to colon-based 12 hour ones
            times_start = convert_old_time(
                start_end[0], revert_12hour=True, truncate_minute=True
            )
            times_end = convert_old_time(
                start_end[1], revert_12hour=True, truncate_minute=True
            )

            # reconstruct summary string
            start_end = times_start + "-" + times_end
            summary[1] = start_end
            summary = " ".join(summary)

            # change location format
            summary = summary.split("(")

            location = summary[-1]

            # remove parentheses and replace with " in "
            if location[-1] == ")":
                location = location[:-1]
                location = f"in {location}"

                summary[-1] = location

            summary = "".join(summary)

            new_times_long_summary.append(summary)

    # change delimiter to newlines
    new_times_long_summary_join = "\n".join(new_times_long_summary)

    # -----------------------------
    # process times_by_day
    # -----------------------------

    new_times_by_day = {}

    # handle cases where misformatted as list
    if times_by_day != []:

        for day, times_locations in times_by_day.items():

            # discard HTBA cases
            if day != "HTBA":

                new_times_by_day[day] = [
                    [convert_old_time(x[0]), convert_old_time(x[1]), x[2]]
                    for x in times_locations
                ]

    return new_times_summary, new_times_long_summary_join, new_times_by_day
