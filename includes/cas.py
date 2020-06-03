import requests

def create_session_from_credentials(netId, password):
    session = requests.Session()

    # Login to CAS.
    _ = session.post(f"https://secure.its.yale.edu/cas/login?username={netId}&password={password}")
    print("Cookies: ", session.cookies.get_dict())

    # Can't support 2-factor authentication.
    raise NotImplementedError
    #return session

def create_session_from_cookie(castgc):
    session = requests.Session()

    # Manually set cookie.
    cookie = requests.cookies.create_cookie(
        domain='secure.its.yale.edu',
        name='CASTGC',
        value=castgc,
        path='/cas/',
        secure=True,
    )
    session.cookies.set_cookie(cookie)

    return session

if __name__ == '__main__':
    netId = 'hks24'
    with open("private/netid.txt","r") as f:
        password = f.read()
    
    session = create_session_from_credentials(netId, password)
    _ = session
