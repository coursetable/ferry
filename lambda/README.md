# AWS Lambda Function - Fetch Rating

This function fetches the HTML for a given course's rating page and returns it. 
Since it is a Lambda function, it effectively acts as a concurrency proxy, allowing us to fetch multiple courses at once.