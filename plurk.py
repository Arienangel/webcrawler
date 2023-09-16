import logging

import aiohttp
import base36
import dateutil.parser

logger = logging.getLogger('Plurk')


async def get_search(query: str, n: int = 30, **kwargs) -> list[dict]:
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
    async with aiohttp.ClientSession() as session:
        while len(posts) < n:
            try:
                async with session.post(search_url, data=post_body) as response:
                    body = await response.json()
                    users = body['users']
                    for post in body['plurks']:
                        author = users[str(post['user_id'])]['display_name']
                        time = int(dateutil.parser.parse(post['posted']).timestamp())
                        content = post['content_raw']
                        id = post['id']
                        url = f"https://www.plurk.com/p/{base36.dumps(id)}"
                        posts.append({'author': author, 'time': time, 'content': content, 'id': id, 'post_url': url})
                    post_body.update({'after_id': posts[-1]['id']})
            except aiohttp.web.HTTPException as E:
                pass
    return posts[:n][::-1]
