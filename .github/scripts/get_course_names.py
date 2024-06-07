import os

import httpx


def get_github_commit_diff(owner, repo, commit_hash, token):
    """Fetches the commit diff from GitHub."""
    url = f"https://api.github.com/repos/{owner}/{repo}/commits/{commit_hash}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.diff",
    }
    response = httpx.get(url, headers=headers)
    return response.text


def extract_new_courses(diff_data):
    """Extracts new course titles from diff data."""
    new_courses = set()
    for line in diff_data.splitlines():
        if line.startswith("+") and '"title":' in line:
            # Extracting the title value assuming the line format is: + "title": "Course Title",
            start_index = line.find('"title":') + len('"title":')
            end_index = line.find(",", start_index)
            title = line[start_index:end_index].strip().strip('"')
            new_courses.add(title)
    return new_courses


def main():
    owner = "coursetable"
    repo = "ferry-data"
    commit_hash = os.getenv(
        "LATEST_COMMIT_SHA"
    )  # Use the fetched commit SHA from ferry-data
    token = os.getenv("TOKEN_FOR_FERRY_DATA")

    # Get commit diff
    diff_data = get_github_commit_diff(owner, repo, commit_hash, token)

    # Extract new course titles
    new_courses = extract_new_courses(diff_data)

    # Write new courses to a file (basic v1 parsing)
    with open("new_courses.txt", "w") as file:
        file.write("New Courses Added:\n")
        for course in new_courses:
            file.write(course + "\n")
        file.write(f"\nTotal New Courses: {len(new_courses)}")


if __name__ == "__main__":
    main()
