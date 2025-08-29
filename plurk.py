import asyncio
import inspect
import logging
from time import sleep

import aiohttp
import base36
import dateutil.parser

logger = logging.getLogger('Plurk')

class convert:
    def to_url(id:int):
        return f"https://www.plurk.com/p/{base36.dumps(int(id))}"

    def to_id(url:str):
        if '/' in url:
            return base36.loads(url.split('https://www.plurk.com/p/')[-1])
        else:
            return base36.loads(url)

async def get_search(query: str, n: int = 30, wait: float = 0, **kwargs) -> list[dict]:
    """
    Search plurk post with query

    Args:
        query (str): content to search

    Returns:
        posts (list): list of posts (author, time, content, post_url)
    """
    search_url = 'https://www.plurk.com/Search/search2'
    post_body = {"query": query}
    posts = list()
    last_id = 0
    logger.info(f'Get Plurk search: {query}')
    async with aiohttp.ClientSession() as session:
        while len(posts) < n:
            try:
                if wait > 0: sleep(wait)
                async with session.post(search_url, data=post_body) as response:
                    if response.content_type != 'application/json':
                        break
                    body = await response.json()
                    users = body['users']
                    if last_id == body['plurks'][-1]['id']: break
                    else: last_id = body['plurks'][-1]['id']
                    for post in body['plurks']:
                        author = users[str(post['user_id'])]['display_name']
                        time = int(dateutil.parser.parse(post['posted']).timestamp())
                        content = post['content_raw']
                        id = post['id']
                        url = f"https://www.plurk.com/p/{base36.dumps(id)}"
                        posts.append({'author': author, 'time': time, 'content': content, 'id': id, 'post_url': url})
                    post_body.update({'after_id': posts[-1]['id']})
            except aiohttp.web.HTTPException as E:
                logger.warning(f'{__name__}@{inspect.stack()[0][3]}(query={query}): {type(E).__name__}: {E}')
                break
    return posts[:n][::-1]


async def get_response(plurk_id, n: int = 400, wait: float = 0):
    if isinstance(plurk_id, str): 
        try:
            plurk_id=int(plurk_id)
        except:
            plurk_id=convert.to_id(plurk_id)
    search_url = 'https://www.plurk.com/Responses/get'
    post_body = {"plurk_id": plurk_id, "from_response_id": 0}
    responses = list()
    last_id = 0
    async with aiohttp.ClientSession() as session:
        while len(responses) < n:
            try:
                if wait > 0: sleep(wait)
                async with session.post(search_url, data=post_body) as response:
                    if response.content_type != 'application/json':
                        break
                    body = await response.json()
                    users = body['users']
                    if len(body['responses'])==0: break
                    if last_id == body['responses'][-1]['id']: break
                    else: last_id = body['responses'][-1]['id']
                    if len(users):
                        for res in body['responses']:
                            try:
                                author = users[str(res['user_id'])]['display_name']
                                time = int(dateutil.parser.parse(res['posted']).timestamp())
                                content = res['content_raw']
                                id = res['id']
                                responses.append({'author': author, 'time': time, 'content': content, 'id': id})
                            except:
                                continue
                    else:
                        for res in body['responses']:
                            try:
                                author = res['handle']
                                time = int(dateutil.parser.parse(res['posted']).timestamp())
                                content = res['content_raw']
                                id = res['id']
                                responses.append({'author': author, 'time': time, 'content': content, 'id': id})
                            except:
                                continue
                    post_body.update({'from_response_id': responses[-1]['id']})
            except aiohttp.web.HTTPException as E:
                break
    return responses[:n]
