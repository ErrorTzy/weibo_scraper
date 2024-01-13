import asyncio
from datetime import datetime
import random
import re
import sqlite3
import aiocsv
import aiofiles
import aiohttp
import orjson
from utils import extracted_text_from_html
from settings import WORKER_SIZE, PROXY_RETRY, FETCHER_RETRY, GET_PROXY_FUNCTIONS, get_uid_list



_WEIBO_DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"
_HREF_PATTERN = r'<a\s+href="([^"]+)">全文<\/a>$'
_DETAIL_PAGE_PATTERN = re.compile(r'var \$render_data = \[(.*?)\]\[0\] \|\| \{\};', re.DOTALL)
_CHECKED_PROXY_QUEUE = asyncio.Queue()
_UNCHECKED_PROXY_QUEUE = asyncio.Queue()
_CHECK_URL = "https://myip.top/"
_MOBILE_HEADERS = {
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

async def proxy_boss():
    global _CHECKED_PROXY_QUEUE, workers_still_working
    """
    Description:
        This function is responsible for arranging everying concerning proxies, including:
            1. load unchecked proxies by running all the functions in proxy_getter_set
            2. assemble proxy checkers to check these uncheck proxies and ask them to put proxies to the CHECKED_PROXY_QUEUE

    Life Cycle:
        Proxy boss is gathered by the main boss.
        He will get off work when all the mweibo workers has finished their jobs.
    """

    # record the data that has already been put to the unchecked queue
    previous_proxy_set = set() 

    async with aiohttp.ClientSession() as proxy_boss_session:
        while workers_still_working:
            if not _CHECKED_PROXY_QUEUE.empty():
                await asyncio.sleep(5)
                continue

            # load proxy set
            new_proxy_set = set()
            
            for function in GET_PROXY_FUNCTIONS:
                new_proxy_list = await function(proxy_boss_session)
                new_proxy_set.update(new_proxy_list)

            # delete the old items in new_proxy_set and add new items to previous_proxy_set
            new_proxy_set.difference_update(previous_proxy_set)
            previous_proxy_set.update(new_proxy_set)

            # if there are no new proxies, wait 60s to retry
            worker_count = len(new_proxy_set)
            if worker_count == 0:
                print("no new proxy avalible, sleeping...")
                await asyncio.sleep(60)
                continue

            # gather workers
            check_proxy_workers = []
            for _ in range(worker_count):
                check_proxy_workers.append(asyncio.create_task(proxy_checker(proxy_boss_session)))
            
            for addr in new_proxy_set:
                await _UNCHECKED_PROXY_QUEUE.put((addr, 0))
            
            await asyncio.gather(*check_proxy_workers)
    print("proxy boss done")


async def proxy_checker(session: aiohttp.ClientSession):
    global _CHECKED_PROXY_QUEUE, _UNCHECKED_PROXY_QUEUE, PROXY_RETRY, _CHECK_URL
    """
    Description:
        This function is proxy boss' worker who 
            1. get unchecked proxy and the checked count from _UNCHECKED_PROXY_QUEUE, 
            2. check the proxy if the checked count did nor reach maximum retry, and
            3. put the valid ones to the CHECKED_PROXY_QUEUE and the invalid ones back to the unchecked proxy queue with checked count += 1

    Life Cycle:
        Proxy checkers are gathered by proxy boss.
        They will get off work when the queue is empty.
    """

    while workers_still_working and not _UNCHECKED_PROXY_QUEUE.empty():
        
        # if already has failed checking for $PROXY_RETRY times, the worker will get a new one
        while not _UNCHECKED_PROXY_QUEUE.empty():
            addr, check_count = await _UNCHECKED_PROXY_QUEUE.get()
            if check_count < PROXY_RETRY:
                break
        
        if check_count > PROXY_RETRY - 1 and _UNCHECKED_PROXY_QUEUE.empty():
            break

        # try to check if the proxy is reachable by connecting to _CHECK_URL
        try:
            async with session.get(_CHECK_URL, proxy=addr) as resp:
                status_code = resp.status
                print(f"{status_code}...{addr}")
                await _CHECKED_PROXY_QUEUE.put(addr)

        # put back to the queue
        except aiohttp.ClientError as e:
            print(f"{addr} proxy check failed: {e}, remaining {PROXY_RETRY - check_count}")
            check_count += 1
            await _UNCHECKED_PROXY_QUEUE.put((addr, check_count))
        

            


async def proxy_fetcher(session: aiohttp.ClientSession, url, retry=FETCHER_RETRY, proxy=None, **kwargs):
    global _CHECKED_PROXY_QUEUE, _UNCHECKED_PROXY_QUEUE
    """
    Description:
        This function is the worker who handles all the internet connections.
        fetcher will get a proxy from CHECKED_PROXY_QUEUE if proxy=True
        if proxy failed, it will retry
        the kwargs will be passed to session.get()
        It will return the raw text of the requested html.

    Error handling:
        Upon error, fetcher will put the proxy back to unchecked queue with checked count = 1
    
    Life Cycle:
        Fetchers are called upon use and exit upon return
    """

    # if retry has excceded limit, return
    if retry < 0:
        return None, proxy
    
    # If a proxy is not given or None/False, get a proxy from checked queue. 
    if proxy:
        proxy_addr = proxy
    else:
        proxy_addr = await _CHECKED_PROXY_QUEUE.get()
    try:
        async with session.get(url, proxy=proxy_addr, **kwargs) as resp:
            response_text = await resp.text(encoding='utf8')
            return response_text, proxy_addr
    
    # if the connection throws an error, put it back to the unchecked queue
    except aiohttp.ClientError as e:
        print(f"{url} connection failed to connect {url}:{e}")
        retry -= 1
        await _UNCHECKED_PROXY_QUEUE.put((proxy_addr, 1))
        return await proxy_fetcher(session, url, retry=retry, proxy=None, **kwargs)


workers_still_working = WORKER_SIZE
async def mweibo_worker(id_queue: asyncio.Queue, results_queue: asyncio.Queue):
    global _CHECKED_PROXY_QUEUE, _MOBILE_HEADERS, uid_assigner_work_done, workers_still_working
    """
    Description:
        This function will 
            1. create a session with cookie
            2. fetch cookie from https://m.weibo.cn/u/${user_id} in order to get fid in cookie
            3. fetch json from https://m.weibo.cn/api/container/getIndex?type=uid&value=${user_id} in order to get fid for weibo messeges container
            4. fetch json from https://m.weibo.cn/api/container/getIndex?type=uid&value=${user_id} to get pages of weibo
    
    Life Cycle:
        mweibo workers are gathered by the main boss.
        They get off work when they receive the None sign from assigner
    """

    # the worker should have a consistent proxy unless the proxy is broken. 
    # Though this should be initialized in the first fetch
    private_proxy = None

    while True:
        # if the worker receives the message for taking off, he takes off
        user_id = await id_queue.get()
        if not user_id:
            break

        m_weibo_index_page_url = f"https://m.weibo.cn/u/{user_id}"
        m_weibo_index_json_url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={user_id}"

        # the worker should have a private session cookie
        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(), connector=aiohttp.TCPConnector(ssl=False)) as session:
            
            # visit the index page to get cookie
            _, private_proxy = await proxy_fetcher(session, m_weibo_index_page_url,
                                                   proxy=private_proxy, headers=_MOBILE_HEADERS)

            m_weibo_index_json_text, private_proxy = await proxy_fetcher(session, m_weibo_index_json_url,
                                                                         proxy=private_proxy, headers=_MOBILE_HEADERS)
            
            # try to fetch the message data. If anything goes wrong with operating the json, finish this job
            try:
                m_weibo_index_json = orjson.loads(m_weibo_index_json_text)
                weibo_fid = m_weibo_index_json["data"]["tabsInfo"]["tabs"][1]["containerid"]
                user_weibo_msgs_list, private_proxy = await fetch_weibo_messages(private_proxy, user_id, weibo_fid, session)
                if user_weibo_msgs_list and len(user_weibo_msgs_list) > 0:
                    await results_queue.put(user_weibo_msgs_list)
            except (TypeError, KeyError):
                print(f"page data invalid {m_weibo_index_json_text}")
            finally:
                await asyncio.sleep(random.random())

    workers_still_working -= 1
    print(f"worker {workers_still_working} completed")
    if not workers_still_working:
        await results_queue.put(False)
        print("all workers completed")


async def fetch_weibo_messages(private_proxy, user_id, fid, session):
    global _MOBILE_HEADERS

    has_next = True
    since_id = None
    all_lines = []
    sleep_determinator = 0
    while has_next:
        # sleep around 1s per page, and additional 1s more for every 5 page
        await asyncio.sleep(0.5 + random.random())
        sleep_determinator += 1
        if sleep_determinator > 4:
            await asyncio.sleep(0.5 + random.random())
            sleep_determinator = 0
                    
                    # the first page does not have since_id
        user_info_url = f"https://m.weibo.cn/api/container/getIndex?type=uid&value={user_id}&containerid={fid}"
        if since_id:
            user_info_url = user_info_url + f"&since_id={since_id}"
                    
        weibo_messege_json_text, private_proxy = await proxy_fetcher(session, user_info_url, proxy=private_proxy, headers=_MOBILE_HEADERS)
        try:
            weibo_messege_json = orjson.loads(weibo_messege_json_text)
            since_id = weibo_messege_json['data']['cardlistInfo']['since_id']
            for card in weibo_messege_json["data"]["cards"]:
                data_line, private_proxy = await check_if_has_detail(private_proxy, session, card)
                if not data_line:
                    data_line = get_row_from_mobile_data(card["mblog"])
                print(f"{data_line[0]}...ok")
                all_lines.append(data_line)
        except KeyError:
            print(weibo_messege_json_text)
            has_next = False
    return all_lines, private_proxy


async def check_if_has_detail(private_proxy, session, card):
    weibo_text = card["mblog"].get("text")
    match = re.search(_HREF_PATTERN, weibo_text)
    if match:
        href_value = match.group(1)
        weibo_message_detail_page_url = f"https://m.weibo.cn/{href_value}"
        weibo_messege_page_text, private_proxy = await proxy_fetcher(session, 
                                                                     weibo_message_detail_page_url, 
                                                                     proxy=private_proxy, 
                                                                     headers=_MOBILE_HEADERS)
        match = _DETAIL_PAGE_PATTERN.search(weibo_messege_page_text)
        if match:
            weibo_messege_page_json = orjson.loads(match.group(1))
            return get_row_from_mobile_data(weibo_messege_page_json["status"]), private_proxy
    return None, private_proxy


def get_row_from_mobile_data(page_json):
    global _WEIBO_DATE_FORMAT
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
        timestamp = int(datetime.strptime(date_string, _WEIBO_DATE_FORMAT).timestamp())
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
    global workers_still_working
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
        writer = aiocsv.AsyncWriter(f)
        await writer.writerow(fields)
        while not (workers_still_working == 0 and results_queue.empty()):
            result = await results_queue.get()
            if result:
                print(result)
                await writer.writerows(result)
            results_queue.task_done()
    print("writer jobs done")


uid_assigner_work_done = False
async def uid_assigner(id_queue: asyncio.Queue, uid_list):
    global WORKER_SIZE, uid_assigner_work_done

    for uid in uid_list:
        await id_queue.put(uid)
    for _ in range(WORKER_SIZE):
        await id_queue.put(False)
    uid_assigner_work_done = True
    print("uid assigner work done")


async def initiator(worker_size, uid_list, csv_file_location):
    id_queue = asyncio.Queue(worker_size * 20)
    results_queue = asyncio.Queue()
    
    mobile_weibo_workers = []
    for _ in range(worker_size):
        mobile_weibo_workers.append(asyncio.create_task(mweibo_worker(id_queue, results_queue)))
    
    await asyncio.gather(proxy_boss(), 
                        uid_assigner(id_queue, uid_list), 
                        writer(results_queue, csv_file_location), 
                        *mobile_weibo_workers)



if __name__ == '__main__':
    page = 0
    limit = 10
    csv_output = "./output/weibo_test.csv"
    uid_list = get_uid_list(page, limit)
    print(uid_list)
    asyncio.run(initiator(WORKER_SIZE, uid_list, csv_output))