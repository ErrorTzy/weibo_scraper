import asyncio
import re
import sqlite3
import aiohttp
import orjson
from datetime import datetime
from aiocsv import AsyncWriter
import aiofiles
import random
from settings import CHECK_PROXY_VALIDITY_WORKERS_SIZE, WORKER_SIZE
from utils import write_string_to_file, load_json_from_file, extracted_text_from_html



mobile_headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,ja;q=0.6,hy;q=0.5",
    "mweibo-pwa": "1",
    "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "Referer": "https://m.weibo.cn/u/5428731890?tabtype=album&jumpfrom=weibocom",
    "Referrer-Policy": "strict-origin-when-cross-origin"
  }

async def fetch_fid_from_cookie(session: aiohttp.ClientSession, url: str, retry: int, valid_proxy_queue: asyncio.Queue, **kwargs):
    """
    DESCRIPTION:
        fetch lfid or fid in M_WEIBOCN_PARAMS from a response cookie.
        This function also refreshes cookie
        
    INPUT: 
        session - aiohttp.ClientSession. if cookies are enabled, then the cookies will be set
        url - url for fetching
        **kwargs - passed to session.get()

    OUTPUT: 
        lfid or fid for weibo user's initial data fetching
        If both lfid and fid presents in the M_WEIBOCN_PARAMS, this function with return lfid
    """
    if retry < 0:
        return None
    
    try:
        m_weibocn_params = {}
        async with session.get(url, headers=mobile_headers, **kwargs):
            weibo_cookies = session.cookie_jar.filter_cookies('https://m.weibo.cn')
            if "M_WEIBOCN_PARAMS" not in weibo_cookies:
                return False
            for key_value in weibo_cookies["M_WEIBOCN_PARAMS"].value.split('%26'):
                splited_key_value = key_value.split('%3D')
                m_weibocn_params[splited_key_value[0]] = splited_key_value[1]
        if "lfid" in m_weibocn_params:
            return m_weibocn_params['lfid']
        return m_weibocn_params['fid']
    
    except aiohttp.ClientError as e:
        if "proxy" in kwargs:
            print(f"fetch_fid_from_cookie error:{type(e)}:{e}, trying to get another proxy. Current proxy: {kwargs['proxy']}")
            new_proxy = await valid_proxy_queue.get()
            kwargs["proxy"] = new_proxy
            retry -= 1
            return fetch_fid_from_cookie(session, url, retry, **kwargs)


async def get_proxies_from_json_url(session: aiohttp.ClientSession, 
                                    url, 
                                    proxy_local_storage, 
                                    **kwargs):
    checkproxy_list = load_json_from_file(proxy_local_storage)
    if checkproxy_list:
        return checkproxy_list
    
    print("fetching from web")
    async with session.get(url, **kwargs) as response:
        text = await response.text()
        write_string_to_file(text, proxy_local_storage)
        print("list ok")
        return orjson.loads(text)


async def fetch_json_from_url(session: aiohttp.ClientSession, url: str, retry: int, valid_proxy_queue: asyncio.Queue, **kwargs):
    """
    DESCRIPTION:
        fetch the json of a url, usually api

    INPUT: 
        session - aiohttp.ClientSession
        url - url for fetching
        custom_headers - header used for feching url
        **kwargs - passed to session.get()

    OUTPUT: 
        the json object from the page
    """

    if retry < 0:
        return None
    
    try:
        async with session.get(url, **kwargs) as resp:
            html = await resp.text(encoding='utf-8')
            return orjson.loads(html)
    
    except aiohttp.ClientError as e:
        if "proxy" in kwargs:
            print(f"fetch_json_from_url error:{type(e)}:{e}, trying to get another proxy. Current proxy: {kwargs['proxy']}")
            new_proxy = await valid_proxy_queue.get()
            kwargs["proxy"] = new_proxy
            retry -= 1
            return fetch_json_from_url(session, url, retry, valid_proxy_queue, **kwargs)


async def fetch_user_weibo_msgs_list(user_id: int, session: aiohttp.ClientSession, fid: int, valid_proxy_queue: asyncio.Queue, **kwargs):
    """
    DESCRIPTION:
        a fast version of html.text with pure regex

    INPUT: 
        user_id - the "xxxx" part in weibo.com/u/xxxxx
        session - aiohttp.ClientSession for specified cookie
        fid - the initial fid for the user, fetched from cookie

    OUTPUT: 
        all the weibo messages from a user
    """

    has_next = True
    since_id = None
    all_lines = []
    sleep_determinator = 0
    while has_next:
        await asyncio.sleep(0.5 + random.random())
        sleep_determinator += 1
        if sleep_determinator > 4:
            await asyncio.sleep(0.5 + random.random())
            sleep_determinator = 0
        user_info_url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={user_id}&containerid={fid}"
        if since_id:
            user_info_url = user_info_url + f"&since_id={since_id}"
        page_json = await fetch_json_from_url(session, user_info_url, 2, valid_proxy_queue, **kwargs)
        try:
            since_id = page_json['data']['cardlistInfo']['since_id']
            for card in page_json["data"]["cards"]:
                data_line = None
                weibo_text = card["mblog"].get("text")
                match = re.search(HREF_PATTERN, weibo_text)
                if match:
                    href_value = match.group(1)
                    data_json = await fetch_detail_json_from_moble(session, href_value, 2, valid_proxy_queue, **kwargs)
                    if data_json:
                        data_line = get_row_from_mobile_data(data_json["status"])
                if not data_line:
                    data_line = get_row_from_mobile_data(card["mblog"])
                print(f"{data_line[0]}...ok")
                all_lines.append(data_line)
        except KeyError:
            print(user_info_url)
            print(page_json)
            has_next = False
    return all_lines


async def fetch_detail_json_from_moble(session: aiohttp.ClientSession, retry: int, valid_proxy_queue: asyncio.Queue, href_value, **kwargs):
    """
    DESCRIPTION:
        fetch the detail of a weibo messege from var render_data from https://m.weibo.cn/status/

    INPUT: 
        session: aiohttp.ClientSession with cookie
        href_value: the id of the messege
        **kwargs: passed to session.get()
    
    OUTPUT: 
        the json format of render_data; false if could not find a match
    """
    if retry < 0:
        return False
    
    try:
        detail_url = f"https://m.weibo.cn/{href_value}"
        async with session.get(detail_url, **kwargs) as resp_detail:
            page_string = await resp_detail.text()
            pattern = re.compile(r'var \$render_data = \[(.*?)\]\[0\] \|\| \{\};', re.DOTALL)
            match = pattern.search(page_string)
            if match:
                page_json = orjson.loads(match.group(1))
                return page_json
            return False

    except aiohttp.ClientError as e:
        if "proxy" in kwargs:
            print(f"fetch_detail_json_from_moble error:{type(e)}:{e}, trying to get another proxy. Current proxy: {kwargs['proxy']}")
            new_proxy = await valid_proxy_queue.get()
            kwargs["proxy"] = new_proxy
            retry -= 1
            return fetch_detail_json_from_moble(session, retry, valid_proxy_queue, href_value, **kwargs)

def get_row_from_mobile_data(page_json):
    """
    DESCRIPTION:
        formats mobile weibo data to standarized structure

    INPUT: 
        mobile weibo's mblog json dict

    OUTPUT: 
        the standerized list data for csv row.
        data structure:
            url_id, # weibo id
            text, # weibo text
            text_length, # weibo text length
            user_id, # weibo owner
            reposts_count,
            comments_count,
            attitudes_count,
            pic_num,
            pic_id,
            is_retweet, # if this weibo is a retweet
            retweet_id, # if not, then None; if is, then id of the retweeted weibo
            date_string, # eg. Dec 27 14:55:18 +0800 2023
            timestamp
    """

    text = extracted_text_from_html(page_json.get("text"))
    user_json = page_json.get("user")
    url_id = page_json.get("id")
    user_id = user_json.get("id")
    reposts_count = page_json.get("reposts_count")
    comments_count = page_json.get("comments_count")
    attitudes_count = page_json.get("attitudes_count")
    text_length = page_json.get("textLength")
    if not text_length:
        text_length = len(text)
    pic_num = page_json.get("pic_num")
    pic_id = ' '.join(page_json.get("pic_ids"))
    is_retweet = False
    retweet_id = None
    date_string = page_json.get("created_at")
    timestamp = None
    if date_string:
        timestamp = int(datetime.strptime(date_string, WEIBO_DATE_FORMAT).timestamp())
    if "retweeted_status" in page_json:
        is_retweet = True
        retweet_id = page_json["retweeted_status"].get("id")
    new_row = [
                url_id, # weibo id
                text, # weibo text
                text_length, # weibo text length
                user_id, # weibo owner
                reposts_count,
                comments_count,
                attitudes_count,
                pic_num,
                pic_id,
                is_retweet, # if this weibo is a retweet
                retweet_id, # if not, then None; if is, then id of the retweeted weibo
                date_string, # eg. Dec 27 14:55:18 +0800 2023
                timestamp
            ]
    return new_row


async def writer(results_queue: asyncio.Queue, csv_file):
    global workers_jobs_done
    # show results of the tasks as they arrive
    fields = [
                'url_id', # weibo id
                'text', # weibo text
                'text_length', # weibo text length
                'user_id', # weibo owner
                'reposts_count',
                'comments_count',
                'attitudes_count',
                'pic_num',
                'pic_id',
                'is_retweet', # if this weibo is a retweet
                'retweet_id', # if not, then None; if is, then id of the retweeted weibo
                'date_string', # eg. Dec 27 14:55:18 +0800 2023
                'timestamp'
            ]
    async with aiofiles.open(csv_file, 'w') as f:
        writer = AsyncWriter(f)
        await writer.writerow(fields)
        while not (workers_jobs_done == 0 and results_queue.empty()):
            result = await results_queue.get()
            if result:
                print(result)
                await writer.writerows(result)
            results_queue.task_done()
    print("writer jobs done")


workers_jobs_done = WORKER_SIZE
async def mobile_weibo_worker(valid_id_queue: asyncio.Queue,valid_proxy_queue: asyncio.Queue, results_queue: asyncio.Queue):
    global workers_jobs_done, uid_assigner_work_done

    while True:
        proxy_url = await valid_proxy_queue.get()
        if proxy_url:
            break

    print("get proxy::" + proxy_url)
    while not (uid_assigner_work_done and valid_id_queue.empty()):
        user_id = await valid_id_queue.get()
        if not user_id:
            break
        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(), connector=aiohttp.TCPConnector(ssl=False)) as session:
            await fetch_fid_from_cookie(session, 
                                        f"https://m.weibo.cn/u/{user_id}", 
                                        2, 
                                        valid_proxy_queue, 
                                        proxy=proxy_url)
            initial_user_json = await fetch_json_from_url(session, 
                                                          f"https://m.weibo.cn/api/container/getIndex?type=uid&value={user_id}", 
                                                          2,
                                                          valid_proxy_queue, 
                                                          headers=mobile_headers, 
                                                          proxy=proxy_url)
            try:
                weibo_fid = initial_user_json["data"]["tabsInfo"]["tabs"][1]["containerid"]
                user_weibo_msgs_list = await fetch_user_weibo_msgs_list(user_id, session, weibo_fid, valid_proxy_queue, headers=mobile_headers, proxy=proxy_url)
                if user_weibo_msgs_list and len(user_weibo_msgs_list) > 0:
                    await results_queue.put(user_weibo_msgs_list)
            except KeyError:
                continue
            
    
    workers_jobs_done -= 1
    print(f"worker {workers_jobs_done} completed")
    if not workers_jobs_done:
        await results_queue.put(False)
        print("all workers completed")


uid_assigner_work_done = False
async def uid_assigner(valid_id_queue: asyncio.Queue, uid_list):
    global uid_assigner_work_done
    for uid in uid_list:
        await valid_id_queue.put(uid)
    for _ in range(WORKER_SIZE):
        await valid_id_queue.put(False)
    uid_assigner_work_done = True
    print("uid assigner work done")

need_proxy = True
async def valid_proxy_putter(session: aiohttp.ClientSession, valid_proxy_queue: aiohttp.ClientSession):
    global need_proxy

    proxy_list = set()
    while workers_jobs_done:
        if not (need_proxy or valid_proxy_queue.empty()):
            await asyncio.sleep(10)
            continue
        # load proxy json
        new_proxy = set()

        for function in GET_PROXY_FUNCTIONS:
            new_proxy_list = await function(session)
            new_proxy.update(new_proxy_list)

        # new_proxy.difference_update(proxy_list) # if non repetitive, unquote this line
        proxy_list.update(new_proxy)

        new_proxy = proxy_list # if non repetitive, unquote this line

        if len(new_proxy) > 0:
            # gather workers
            check_proxy_workers = []
            check_proxy_queue = asyncio.Queue()
            for _ in range(CHECK_PROXY_VALIDITY_WORKERS_SIZE):
                check_proxy_workers.append(asyncio.create_task(check_proxy_validity_workers(session, check_proxy_queue, valid_proxy_queue)))
            

            for addr in new_proxy:
                print(f"putting addr for checking: {addr}")
                await check_proxy_queue.put(addr)
            
            for _ in range(CHECK_PROXY_VALIDITY_WORKERS_SIZE):
                await check_proxy_queue.put(None)

            await asyncio.gather(*check_proxy_workers)
        
        need_proxy = False
        



check_proxy_validity_workers_jobs_done = CHECK_PROXY_VALIDITY_WORKERS_SIZE
async def check_proxy_validity_workers(session: aiohttp.ClientSession, 
                                       check_proxy_queue: asyncio.Queue, 
                                       valid_proxy_queue: asyncio.Queue):
    global check_proxy_validity_workers_jobs_done

    while True:
        addr = await check_proxy_queue.get()
        if not addr:
            break
        check_url = "https://myip.top/"
        try:
            proxy_full_string = f"http://{addr}"
            async with session.get(check_url, proxy=proxy_full_string) as resp:
                status_code = resp.status
                text = await resp.text()
                print(f"{status_code}...{text}...ok!")
                await valid_proxy_queue.put(proxy_full_string)
        except aiohttp.ClientError as e:
            print(f"{addr} not ok: {e}")

    check_proxy_validity_workers_jobs_done -= 1
    if not check_proxy_validity_workers_jobs_done:
        await valid_proxy_queue.put(None)
        print("all validation workers jobs done")


async def initiator(worker_size, uid_list, csv_file_location):
    valid_id_queue = asyncio.Queue(worker_size * 20)
    results_queue = asyncio.Queue()
    valid_proxy_queue = asyncio.Queue()
    
    mobile_weibo_workers = []
    for _ in range(worker_size):
        mobile_weibo_workers.append(asyncio.create_task(mobile_weibo_worker(valid_id_queue, valid_proxy_queue, results_queue)))
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(), connector=aiohttp.TCPConnector(ssl=False)) as session:
        await asyncio.gather(valid_proxy_putter(session, valid_proxy_queue), 
                             uid_assigner(valid_id_queue, uid_list), 
                             writer(results_queue, csv_file_location), 
                             *mobile_weibo_workers)


def get_uid_list(page, limit):
    return [1]

WEIBO_DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"
MSG_NOT_COMPLETE = '">全文</a>'
HREF_PATTERN = r'<a\s+href="([^"]+)">全文<\/a>$'


if __name__ == '__main__':
    page = 0
    limit = 10
    csv_output = "./output/weibo_test_proxy_databse.csv"
    uid_list = get_uid_list(page, limit)
    print(uid_list)
    asyncio.run(initiator(WORKER_SIZE, uid_list, csv_output))