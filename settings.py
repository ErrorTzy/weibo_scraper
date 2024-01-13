def get_uid_list():
    """
    the function should be implimented by the user
    it should return a list of weibo uids
    """
    return [1,2,3]


async def test(session):
    """
    the function should be implimented by the user
    it should return a list of proxies
    """
    return ["http://127.0.0.1:20171","http://127.0.0.1:20172","http://127.0.0.1:20170"]


WORKER_SIZE = 1
# usually this should be smaller than proxy pool size

FETCHER_RETRY = 3
# if a fetcher swiched the proxy more than this amount of times, he will return None

PROXY_RETRY = 3 
# if a checker checked this proxy more than this amount of times, he will not put it back to unchecked proxies

GET_PROXY_FUNCTIONS = {test}
# the functions that retrieves a list or set of proxies

CSV_OUTPUT = "./local_storage/weibo_test.csv"
# the output csv destination