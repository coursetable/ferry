import fetch from 'node-fetch';

export const handler = async (event) => {
  var auth_header = ""
  try {
    auth_header = event["headers"]["auth_header"];
  }
  catch (e) {}
  if (auth_header != "123") {
    return {
      statusCode: 403,
      body: JSON.stringify("Missing/incorrect auth_header value.")
    };
  }
  
  var res = "";
  
  try {
    const body = JSON.parse(event["body"])
    const cookie = body["cookie"];
    const url = decodeURI(body["url"]);
    console.log(url);
    res = await fetch(url, {headers: {"Cookie": cookie}});
  }
  catch (e) {
    return {
      statusCode: 400,
      body: JSON.stringify(e)
    };
  }
  
  try {
    const html = await res.text();
    return {
      statusCode: res.status,
      body: html
    };
  }
  catch (e) {
    return {
      statusCode: 500,
      body: JSON.stringify(e)
    }
  }
};
