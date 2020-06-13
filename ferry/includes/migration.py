def convert_old_time(time, revert_12hour=False, truncate_minute=False):
    """
    Convert previous float-formatted times to 24-hour, full format

    Parameters
    ----------
    time: string
        a time from the previous CourseTable format
    revert_12hour: bool
        whether or not to convert back to 12-hour format
    truncate_minute: bool
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

        hour = int(hour)

        if hour > 12:
            hour = hour - 12

            formatted_time = f"{str(hour)}:{minute}pm"

        elif hour == 12:

            formatted_time = f"{str(hour)}:{minute}pm"

        elif hour == 0:

            hour = 12

            formatted_time = f"{str(hour)}:{minute}am"

        else:

            formatted_time = f"{str(hour)}:{minute}am"

        if truncate_minute and minute == "00":

            formatted_time = formatted_time.split(":")[0] + formatted_time[-2:]

    return formatted_time


def convert_old_meetings(times):
    """
    Convert previous meeting times format to new one

    Parameters
    ----------
    times: dictionary
        previous 'times' field, with keys "summary", "long_summary", "by_day"

    Returns
    -------
    new_times_summary: string
        reformatted "summary" field
    new_times_long_summary: string
        reformatted "long_summary" field
    by_day: dictionary
        reformatted "by_day" field
    """

    times_summary = times["summary"]
    times_long_summary = times["long_summary"]
    times_by_day = times["by_day"]

    if times == ["HTBA"]:
        return "TBA", "", {}

    # ---------------------
    # process times summary
    # ---------------------

    # unknown times
    if times_summary in ["1 HTBA", "2 HTBA", "HTBA", ""]:

        new_times_summary = "TBA"

    else:

        times_summary = times_summary.split(" ")
        start_end = times_summary[1].split("-")

        # convert 24-hour float-based time formats to colon-based 12 hour ones
        times_start = convert_old_time(
            start_end[0], revert_12hour=True, truncate_minute=True
        )
        times_end = convert_old_time(
            start_end[1], revert_12hour=True, truncate_minute=True
        )

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
    new_times_long_summary = "\n".join(new_times_long_summary)

    # -----------------------------
    # process times_by_day
    # -----------------------------

    new_times_by_day = dict()

    # handle cases where misformatted as list
    if times_by_day != []:

        for day, times_locations in times_by_day.items():

            # discard HTBA cases
            if day != "HTBA":

                new_times_by_day[day] = [
                    [convert_old_time(x[0]), convert_old_time(x[1]), x[2]]
                    for x in times_locations
                ]

    return new_times_summary, new_times_long_summary, new_times_by_day
