
def get_uid_list(page, limit):
    # db_file = "/home/scott/Downloads/裤子/weibo.db"
    # offset =  page * limit
    # table_name = "updated_data"
    # sql_fetch_all_uid = f"SELECT uid FROM {table_name} LIMIT {offset},{limit}"
    # conn = sqlite3.connect(db_file)
    # cursor = conn.cursor()
    # cursor.execute(sql_fetch_all_uid)
    # return [row[0] for row in cursor.fetchall()]
    return [1,2,3]


async def test(session):
    return ["http://127.0.0.1:20171","http://127.0.0.1:20172","http://127.0.0.1:20170"]


WORKER_SIZE = 1
# usually this should be smaller than proxy pool size

FETCHER_RETRY = 3
# if a fetcher swiched the proxy more than this amount of times, he will return None

PROXY_RETRY = 3 
# if a checker checked this proxy more than this amount of times, he will not put it back to unchecked proxies

GET_PROXY_FUNCTIONS = {test}
# the functions that retrieves a list or set of proxies

