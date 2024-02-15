import httpx
import csv
import asyncio
from collections import defaultdict
import json
import time

async def fetch_ivystats_csv(cas_cookie, season):
    cookies = {'JSESSIONID': cas_cookie}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://ivy.yale.edu/course-stats/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    async with httpx.AsyncClient(cookies=cookies, headers=headers) as client:
        response = await client.get('https://ivy.yale.edu/course-stats/course/download')

        if response.status_code == 200:
            decoded_content = response.content.decode('utf-8')
            csv_reader = csv.DictReader(decoded_content.splitlines())
            courses_data = defaultdict(list)

            for row in csv_reader:
                courses_data[row['fullCourse']].append({
                    'date': row['timestamp'],
                    'count': int(row['count'])
                })

            # Aggregate counts for the most recent date
            result = {}

            for course, data in courses_data.items():
                # Find the most recent date for each course
                most_recent_date = max(data, key=lambda x: x['date'])['date']
                total_count = sum(item['count'] for item in data if item['date'] == most_recent_date)
                result[course] = total_count

            # Output the result as JSON
            print(json.dumps(result, indent=4))
        else:
            print(f"Failed to fetch data: {response.status_code}")

async def main():
    cas_cookie = input("Enter JSESSIONID cookie: ")
    season = "202401"  # Adjust as needed
    time_before = time.time()
    await fetch_ivystats_csv(cas_cookie, season)
    time_after = time.time()
    print(f"Time taken: {time_after - time_before:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
