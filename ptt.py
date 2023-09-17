import inspect
import logging

import aiohttp
from pyquery import PyQuery as pq

logger=logging.getLogger('PTT')

async def get_forum(forum: str, n: int = 30, **kwargs) -> list[str]:
    """
    Get ptt posts in forum

    Args:
        forum (str): forum name
        n (int, optional): number of posts

    Returns:
        posts (list): list of post urls
    """
    domain = 'https://www.ptt.cc'
    forum_url = f'{domain}/bbs/{forum}/index.html'
    posts = list()
    async with aiohttp.ClientSession(headers={"Cookie": "over18=1"}) as session:
        while len(posts) < n:
            try:
                async with session.get(forum_url) as response:
                    logger.info(f'Get PTT forum: {forum}')
                    body = await response.text()
                    r = pq(body)
            except aiohttp.web.HTTPException as E:
                logger.warning(f'{__name__}@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                break
            if r('div.r-list-sep'):  # 移除置底文
                posts.extend([f"{domain}{pq(s).attr('href')}" for s in r('div.r-list-sep').prev_all('div.r-ent').find('div.title a')][::-1])
            else:
                posts.extend([f"{domain}{pq(s).attr('href')}" for s in r('div.r-ent div.title a')][::-1])
            if r('div.btn-group-paging a:nth-child(2)').attr('href'):  # 取得上一頁網址
                forum_url = domain + r('div.btn-group-paging a:nth-child(2)').attr('href')
            else:
                break
    return posts[:n][::-1]


async def get_post(post_url: str, **kwargs) -> dict:
    """
    Get ptt post content

    Args:
        post_url (str): url of the post

    Returns:
        post (dict): forum name, author, title, time, content, post_url
    """
    try:
        async with aiohttp.ClientSession(headers={"Cookie": "over18=1"}) as session:
            async with session.get(post_url) as response:
                logger.info(f'Get PTT post: {post_url}')
                body = await response.text()
                r = pq(body)
    except aiohttp.web.HTTPException as E:
        raise E
    if r('span.article-meta-value'):
        author = pq(r('span.article-meta-value')[0]).text()
        forum = post_url.rsplit('/', 2)[1]
        title = pq(r('span.article-meta-value')[2]).text()
        time = int(post_url.rsplit('.', 4)[1])
        content = r('div#main-content').text(squash_space=False).split('\n\n--\n※ 發信站: 批踢踢實業坊(ptt.cc), 來自')[0].split('\n\n', 4)[-1]
        return {'forum': forum, 'author': author, 'title': title, 'time': time, 'content': content, 'post_url': post_url}
    else:  # forwarded post that does not have metadata
        url = r('div#main-content').text(squash_space=False).split('※ 文章網址: ', 1)[1].split('.html\n※ ', 1)[0] + '.html'
        return await get_post(url)
