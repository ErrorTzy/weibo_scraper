

from weibo_scraper_settings import LOG_OUTPUT_FOLDER

write_files = {}

async def write_string_to_file(string, filename):
    global write_files
    try:
        aiofiles
    except NameError:
        import aiofiles
    
    if filename not in write_files:
        write_files[filename] = await aiofiles.open(LOG_OUTPUT_FOLDER + filename, 'w+')
    await write_files[filename].write(string)
    await write_files[filename].flush()

append_files = {}

async def append_string_to_file(string, filename):
    global write_files
    try:
        aiofiles
    except NameError:
        import aiofiles
    
    if filename not in write_files:
        write_files[filename] = await aiofiles.open(LOG_OUTPUT_FOLDER + filename, 'a+')
        
    if type(string) is not str:
        string = str(string)
        
    await write_files[filename].write(string)
    await write_files[filename].flush()


log_files = {}

async def append_new_line_to_log(output_string, filename):
    global log_files
    try:
        aiofiles
    except NameError:
        import aiofiles

    try:
        datetime
    except NameError:
        from datetime import datetime

    if filename not in log_files:
        log_files[filename] = await aiofiles.open(LOG_OUTPUT_FOLDER + filename, 'a+')
    await log_files[filename].write(datetime.now().isoformat(' ') + " " + output_string + "\n")
    await log_files[filename].flush()


async def close_files():
    global write_files, log_files

    for fileobj in log_files:
        await log_files[fileobj].close()
    for fileobj in write_files:
        await write_files[fileobj].close()
    for fileobj in append_files:
        await append_files[fileobj].close()


def load_json_from_file(filename):
    try:
        orjson
    except NameError:
        import orjson
    
    try:
        with open(LOG_OUTPUT_FOLDER + filename, 'r') as file:
            data = file.read()
            return orjson.loads(data)
    except FileNotFoundError:
        return None


def dump_json_from_file(data, filename):
    try:
        orjson
    except NameError:
        import orjson
    with open(LOG_OUTPUT_FOLDER + filename, 'w+') as file:
        file.write(orjson.dumps(data))


async def print_writer(results_queue):
    while True:
        result = await results_queue.get()
        print("......print_writer......")
        print(result)
        print("......print_writer......")



def empty_queue(q):
    """
    DESCRIPTION:
        empty all tasks in a queue
        
    INPUT: 
        q: the asyncio queue needed to be emptied

    OUTPUT: 
        none
    """
    try:
        asyncio
    except NameError:
        import asyncio
    while not q.empty():
        # Depending on your program, you may want to
        # catch QueueEmpty
        try:
            q.get_nowait()
            q.task_done()
        except asyncio.QueueEmpty:
            break




def extracted_text_from_html(text_html):
    """
    DESCRIPTION:
        a fast version of html.text with pure regex

    INPUT: 
        html data in string
    
    OUTPUT: 
        html data without tags and &xx; charactors
    """
    try:
        re
    except NameError:
        import re
    return re.sub(r'(&[a-z]*?;)|(<[^>]*>)', '', text_html)

