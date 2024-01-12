

from settings import OUTPUT_FOLDER


def write_string_to_file(string, filename):
    with open(OUTPUT_FOLDER + filename, 'w+') as file:
        file.write(string)


def load_json_from_file(filename):
    try:
        orjson
    except NameError:
        import orjson
    try:
        with open(OUTPUT_FOLDER + filename, 'r') as file:
            data = file.read()
            return orjson.loads(data)
    except FileNotFoundError:
        return None


def dump_json_from_file(data, filename):
    try:
        orjson
    except NameError:
        import orjson
    with open(OUTPUT_FOLDER + filename, 'w+') as file:
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

