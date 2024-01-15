
from datetime import datetime
import orjson


WORKER_SIZE = 50
# usually this should be smaller than proxy pool size

FETCHER_RETRY = 3
# if a fetcher swiched the proxy more than this amount of times, he will return None

PROXY_RETRY = 2
# if a checker checked this proxy more than this amount of times, he will not put it back to unchecked proxies

PROXY_TIMEOUT = 10
# if after PROXY_TIMEOUT seconds, proxy still has no response, it will considered invalid

FORCE_RELEASE_LIMIT = 0
# if no items are fetched, force release += 1 and old proxies will be released when force release > FORCE_RELEASE_LIMIT

MAXIMUM = 9999999999
RANGE_FROM = 0
RANGE_TO = MAXIMUM
LIMIT = 1000000
CSV_OUTPUT_FOLDER = "./local_storage/"
ERROR_LOG_NAME = "weibo_error_log.txt"
PROXY_LOG_NAME = "weibo_proxy_log.txt"
LOG_OUTPUT_FOLDER = "./logs/"
MAXIMUM_PROXY_WORKER_COUNT = 800

def date_to_timestamp(date_str):
    try:
        timestamp = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp()
        return timestamp
    except BaseException:
        return 0


async def get_proxy_list_from_localhost(session):
    localhost_proxy_url = "http://127.0.0.1:5010/all/?type=https"
    async with session.get(localhost_proxy_url) as resp:
        text = await resp.text()
        checkproxy_list = orjson.loads(text)
    checkproxy_list = sorted(checkproxy_list, key=lambda x: date_to_timestamp(x['last_time']), reverse=True)
    checkproxy_addr_list = []
    for proxy_datum in checkproxy_list:
        addr = proxy_datum.get("proxy")
        if addr:
            checkproxy_addr_list.append(f"http://{addr}")
    
    return checkproxy_addr_list


GET_PROXY_FUNCTIONS = {
    get_proxy_list_from_localhost
}
# the functions that retrieves a list or set of proxies

