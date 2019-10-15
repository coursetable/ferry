import requests
import getpass 

netid = input("Yale NetID: ")
password = getpass.getpass()
session = requests.Session()

r = session.post("https://secure.its.yale.edu/cas/login?username="+netid+"&password="+password)

print("Cookies: ", session.cookies.get_dict())