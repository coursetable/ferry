# Modified from Dan Zhao
# Original article: https://yaledailynews.com/blog/2020/01/10/yales-most-popular-courses/
# Github: https://github.com/iamdanzhao/yale-popular-classes
# README: https://github.com/iamdanzhao/yale-popular-classes/blob/master/data-guide/course_data_guide.md

# 1. Import packages -----

from requests import get
from bs4 import BeautifulSoup

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

from ferry import config

startTime = datetime.now()

# 2. Set working directory -----
# Be sure to escape '\' with another '\'
# Be sure to create the folder beforehand — otherwise, Python will return an error
# This is the directory where the CSV files will land

wd = f"{config.DATA_DIR}/demand_stats/"
os.chdir(wd)

# 3. Set semester -----
# Manually input or pass using command line arguments
# Examples: 202001 = 2020 Spring, 201903 = 2019 Fall

# TODO: Before we ship, we'll want to change this to set dynamically to reflect the upcoming term (leaving as spring 2020 now for debugging purposes)
semester = '202001'

if len(sys.argv) > 1:
    semester = str(sys.argv[1])

# 4. Get list of subjects -----

def getSubjects(fileName):
    # get URL and pass to BeautifulSoup
    url = 'https://ivy.yale.edu/course-stats/'
    r = get(url)
    s = BeautifulSoup(r.text, 'html.parser')

    # get all the dropdown options and split into subject code + subject name
    subjects_elems = s.select('#subjectCode option')
    subjects_codes = [elem.text.split(" - ", 2)[0] for elem in subjects_elems[1:]]
    subjects_names = [elem.text.split(" - ", 2)[1] for elem in subjects_elems[1:]]

    # make a dataframe and save it to csv
    pd.DataFrame({
        'subject': subjects_codes, 
        'name': subjects_names
    }).to_csv(fileName, index=False)

    return subjects_codes

subjects = getSubjects('subjects.csv')

# 5. Get the array of dates -----

def getDates(sem):
    # get URL and pass to BeautifulSoup
    # using AMTH as arbitary subject
    url = 'https://ivy.yale.edu/course-stats/?termCode=' + sem + '&subjectCode=AMTH'
    r = get(url)
    s = BeautifulSoup(r.text, 'html.parser')

    # select date elements
    dates_elems = s.select('table table')[0].select('td')
    
    return [date.text.strip() for date in dates_elems]

dates = getDates(semester)

# 6. Scrape courses
# Most of the code here is to deal with cross-listed courses, and to avoid having duplicate data

courses = [] # containers for courses: format is id, code, name
demands = [] # container for demand: format is id, date, count
i = 1 # iterator for assigning course id

for subject in subjects:
    # get URL and pass to BeautifulSoup
    # '.replace("&", "%26")' escapes the ampersand
    url = 'https://ivy.yale.edu/course-stats/?termCode=' + semester + '&subjectCode=' + subject.replace("&", "%26")
    r = get(url)
    s = BeautifulSoup(r.text, 'html.parser')

    # selects all the courses info and demand info
    # each element in course_containers contains code, name, and demand for one course
    course_containers = s.select("div#content > div > table > tbody > tr")

    for container in course_containers:
        # extract name and code
        code = container.select("td a")[0].text.strip().replace(";", "")
        name = container.select("td span")[0].text.strip().replace(";", "")

        # 'code' might be a long list of cross-listed couses (e.g. 'S&DS 262/S&DS 562/CPSC 262'),
        # so we need to split all of the codes and look at them separately
        full_strings_all = code.split("/")

        # sometimes we'll get a course code that isn't actually an academic subject,
        # so this line filters that out
        full_strings = [string for string in full_strings_all if string.split(" ")[0] in subjects]

        # now, we need to identify the course code corresponding to the subject we're working
        # on in the loop — this finds the course code with 'subject' in it
        code_this_subject = [string for string in full_strings if subject in string][0]

        # Test if we've already added the demand for this course (due to cross-listing) into the 
        # data structure. We don't want duplicate data, so if we already have the demand, we simply skip it
        if full_strings.index(code_this_subject) == 0:
            # if this is our first time coming across this course, we need to add all of the
            # cross-listed course numbers into our 'courses' list
            for string in full_strings:
                courses.append([i, string, name])

            # selects each of the individual counts
            # each element in count is one count corresponding to one day
            counts = container.select("td.trendCell")

            # add the count for each into our 'demands' list
            for j in range(len(dates)):
                demands.append([i, dates[j], counts[j].text.strip()])
        
        i += 1

    print('Scraped ' + str(i) + ' courses up to ' + subject + ', ' + str(datetime.now() - startTime) + ' elapsed')

# write courses to csv
pd.DataFrame(
    courses,
    columns = ['id', 'code', 'name']
).to_csv('courses.csv', index = False)

# write demand to csv
pd.DataFrame(
    demands,
    columns = ['id', 'date', 'count']
).to_csv('demand.csv', index = False)

print('Complete, ' + str(datetime.now() - startTime) + ' elapsed')