import requests
import getpass 
import json
import time
from bs4 import BeautifulSoup
from includes.class_processing import *

netid = input("Yale NetID: ")
password = getpass.getpass()
session = requests.Session()

r = session.post("https://secure.its.yale.edu/cas/login?username="+netid+"&password="+password)

print("Cookies: ", session.cookies.get_dict())

url_index = "https://oce.app.yale.edu/oce-viewer/studentSummary/index"
url_show = "https://oce.app.yale.edu/oce-viewer/studentSummary/show"
url_graphData = "https://oce.app.yale.edu/oce-viewer/studentSummary/graphData"

class_info = {
    "crn": 10684,
    "term_code": 201803
}

page_index = session.get(url_index,params = class_info)
page_show = session.get(url_show)
data_show = json.loads(page_show.text)

questionList = data_show['questionList']

questionIds = []
for question in questionList:
    questionIds.append(question['questionId'])

for Id in questionIds:
    millis = int(round(time.time() * 1000))
    question_info = {
        "questionId": Id,
        "_": millis
    }
    page_graphData = session.get(url_graphData, params = question_info)
    data_graphData = json.loads(page_graphData.text)
    print(data_graphData)