from bs4 import BeautifulSoup
import httpx
import time

def scrape_ivystats(cas_cookie, season, client: AsyncClient = AsyncClient(timeout=None)):

    cookies = {"JSESSIONID" : cas_cookie}

    option_headings = None

    while option_headings is None: 
        r = httpx.get("https://ivy.yale.edu/course-stats/", cookies = cookies, follow_redirects = True)
        subject_soup = BeautifulSoup(r.text, features = "lxml")
        option_headings = subject_soup.find(id = "subjectCode")
    
    print("option_headings passed")
    options = option_headings.find_all("option") # sometimes this is None i think because of async issues?
        
    subjects = []
    for option in options:
        subjects.append(option.getText().split(" - ")[0])

    courses = []

    for subj in subjects:
        subj_data = {"termCode" : season, "subjectCode" : subj, "statType" : "REGISTERED", "numDays" : "7"}
        
        coursesTable = None 
        counter = 0

        while coursesTable is None and counter < 10:
            subj_ret = httpx.post("https://ivy.yale.edu/course-stats/", cookies = cookies, data = subj_data, follow_redirects = True)
            subj_soup = BeautifulSoup(subj_ret.text, features = "lxml")
            coursesTable = subj_soup.find(id = "coursesTable")
            counter += 1

        if coursesTable is None:
            continue

        rows = coursesTable.find("tbody").find_all("tr")
        for row in rows:
            tds = row.find_all("td")

            courses.append([tds[0]["data-order"], tds[7].text])

            # print("-" * 80)
    
    for course in courses:
        print(course)
    # cpsc_data = {"termCode" : season, "subjectCode" : "CPSC", "statType" : "REGISTERED", "numDays" : "7"}
    # cpsc = httpx.post("https://ivy.yale.edu/course-stats/", cookies = cookies, data = cpsc_data, follow_redirects = True)

    # print(cpsc.text)

    # soup = BeautifulSoup

def fetch_ivystats(cas_cookie, season):
    
    if season < "202103":
        print("Can only fetch from seasons greater than 202103.")
        return
    
    scrape_ivystats(cas_cookie = cas_cookie, season = season)


time1 = time.time()
cookie = input("Enter cookie: ")
fetch_ivystats(cookie, "202401")
time2 = time.time()
print("Time taken: " + str(time2 - time1))