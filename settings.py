
async def test(session):
    return ["127.0.0.1:20171","127.0.0.1:20172","127.0.0.1:20170"]


OUTPUT_FOLDER = "./local_storage/"
CHECK_PROXY_VALIDITY_WORKERS_SIZE = 100
WORKER_SIZE = 2
GET_PROXY_FUNCTIONS = (
    test,
    # get_proxy_list_from_checkproxy,
    # get_proxy_list_from_localhost
)

