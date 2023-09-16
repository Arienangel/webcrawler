import logging
import math
import re

import aiohttp
import dateutil
from pyquery import PyQuery as pq

logger = logging.getLogger('PTT')


async def get_news(keywords: list[str] = None, n: int = 100, **kwargs) -> tuple[str]:
    """
    Get cna news

    Args:
        keywords (str, optional): search keyword in headline
        n (int, optional): number of posts

    Returns:
        posts (list): list of post urls
    """
    url = 'https://www.cna.com.tw/cna2018api/api/WNewsList'
    posts = list()
    async with aiohttp.ClientSession() as session:
        for x in range(math.ceil(n / 100)):
            try:
                post_body = {"action": "0", "category": "aall", "pagesize": "20", "pageidx": x + 1}
                async with session.post(url, data=post_body) as response:
                    body = await response.json()
                    for i in body['ResultData']['Items']:
                        if keywords:
                            for keyword in keywords:
                                if keyword in i['HeadLine']:
                                    posts.append(i['PageUrl'])
                        else:
                            posts.append(i['PageUrl'])
            except aiohttp.web.HTTPException as E:
                pass
    return posts[:n][::-1]


async def get_post(post_url: str, **kwargs) -> dict:
    """
    Get cna post content

    Args:
        post_url (str): url of the post

    Returns:
        post (dict): title, time, content, post_url
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(post_url) as response:
                body = await response.text()
                r = pq(body)
    except aiohttp.web.HTTPException as E:
        raise E
    title = r('div.centralContent h1').text()
    time = re.match('\d{4}/\d{1,2}/\d{1,2} \d{2}:\d{2}', r('div.centralContent div.timeBox').text()).group(0)
    time = round(dateutil.parser.parse(time).timestamp())
    content = pq(r('div.centralContent div.paragraph')[0]).find('p').text()
    return {'title': title, 'time': time, 'content': content, 'post_url': post_url}
