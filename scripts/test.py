from datetime import datetime
from wsgiref.handlers import format_date_time
from time import mktime
import hashlib
import base64
import hmac
from urllib.parse import urlencode
try:
    from urllib3.util import Url
except ImportError:
    # Fallback if urllib3 is not available
    class Url:
        def __init__(self, host, path, schema):
            self.host = host
            self.path = path
            self.schema = schema

def parse_url(request_url):
    """Parse URL using urllib3.util.Url"""
    stidx = request_url.index("://")
    host = request_url[stidx + 3:]
    schema = request_url[:stidx + 3]
    edidx = host.index("/")
    if edidx <= 0:
        raise Exception("invalid request url:" + request_url)
    path = host[edidx:]
    host = host[:edidx]
    u = Url(host=host, path=path, scheme=schema.rstrip('://'))
    return u

def assemble_auth_url(request_url, method="GET", api_key="", api_secret=""):
    """Assemble authentication URL"""
    u = parse_url(request_url)
    host = u.host
    path = u.path
    now = datetime.now()
    date = format_date_time(mktime(now.timetuple()))
    signature_origin = "host: {}\ndate: {}\n{} {} HTTP/1.1".format(host, date, method, path)
    signature_sha = hmac.new(api_secret.encode('utf-8'), signature_origin.encode('utf-8'),
                             digestmod=hashlib.sha256).digest()
    # print("signature_sha", signature_sha)
    print(f"Raw bytes length: {len(signature_sha)}")  # This will print: 32
    signature_sha = base64.b64encode(signature_sha).decode(encoding='utf-8')
    authorization_origin = "api_key=\"{}\", algorithm=\"{}\", headers=\"{}\", signature=\"{}\"".format(
        api_key, "hmac-sha256", "host date request-line", signature_sha)
    authorization = base64.b64encode(authorization_origin.encode('utf-8')).decode(encoding='utf-8')
    values = {
        "host": host,
        "date": date,
        "authorization": authorization
    }

    return request_url + "?" + urlencode(values)


if __name__ == '__main__':
    url = 'wss://avatar.cn-huadong-1.xf-yun.com/v1/interact'
    appId = 'your appId'
    appKey = 'your appKey'
    appSecret = 'your appSecret'
    authUrl = assemble_auth_url(url, 'GET', appKey, appSecret)
    print("authUrl", authUrl)