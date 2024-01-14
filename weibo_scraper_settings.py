
from datetime import datetime
import aiohttp
import orjson


WORKER_SIZE = 60
# usually this should be smaller than proxy pool size

FETCHER_RETRY = 3
# if a fetcher swiched the proxy more than this amount of times, he will return None

PROXY_RETRY = 2
# if a checker checked this proxy more than this amount of times, he will not put it back to unchecked proxies

PROXY_TIMEOUT = 10

MAXIMUM = 9999999999
RANGE_FROM = 77886979
RANGE_TO = MAXIMUM
LIMIT = 1000000
CSV_OUTPUT_FOLDER = "/media/scott/ScottTang/weibo_data/"
ERROR_LOG_NAME = "weibo_error_log.txt"
PROXY_LOG_NAME = "weibo_proxy_log.txt"
LOG_OUTPUT_FOLDER = "/media/scott/ScottTang/weibo_data/logs/"
MAXIMUM_PROXY_WORKER_COUNT = 800

# async def zhima_api(session):
#     api_url = "http://http.tiqu.letecs.com/getip3?neek=3d380c10e6b13398&num=100&type=2&pro=0&city=0&yys=0&port=11&time=4&ts=0&ys=0&cs=0&lb=1&sb=0&pb=45&mr=1&regions=310000,320000,330000,340000&gm=4"
#     async with session.get(api_url) as response:
#         text = await response.text()
#     return [f"http://{proxy_info['ip']}:{proxy_info['port']}" for proxy_info in orjson.loads(text)['data']]
    



# async def kuaidaili_api(session):
#     import requests
#     api_url = "https://dps.kdlapi.com/api/getdps/?secret_id=of21w76tet3m6vm82s5u&num=30&signature=n51r5hxbhfuuifvhr3x4jukvk4nao1xy&pt=1&dedup=1&format=json&sep=1"
#     proxy_list = requests.get(api_url).json().get('data').get('proxy_list')
#     full_proxy_list = [f"http://{addr}" for addr in proxy_list]
#     return full_proxy_list


async def get_proxy_list_from_localhost(session):
    def date_to_timestamp(date_str):
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp()
    

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
    get_proxy_list_from_localhost}
# the functions that retrieves a list or set of proxies

