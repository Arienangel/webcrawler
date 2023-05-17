import asyncio
import logging
import os
import math
import aiohttp
import aiohttp.web
import aiosqlite
import base36
import dateutil.parser
import discord
import selenium.common.exceptions
import undetected_chromedriver as uc
import yaml
import inspect
from pyquery import PyQuery as pq
from selenium.webdriver.common.by import By

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s')
with open('config.yaml', encoding='utf-8') as f:
    conf = yaml.load(f, yaml.SafeLoader)
if not os.path.exists('data'): os.makedirs('data')
logging_level = conf['app']['logger_level']


async def ptt(forum: str, send: bool = True, **kwargs):
    logger = logging.getLogger(f'PTT/{forum}')
    logger.level = logging_level
    logger.info(f'Start PTT/{forum}')
    base = 'https://www.ptt.cc'

    async def get_forum(forum: str, n: int = 30, **kwargs) -> list[str]:
        forum_url = f'{base}/bbs/{forum}/index.html'
        posts = list()
        while len(posts) < n:
            try:
                async with session.get(forum_url) as response:
                    body = await response.text()
                    r = pq(body)
            except aiohttp.web.HTTPException as E:
                logger.warning(f'PTT/{forum}@{inspect.stack()[0][3]}: {type(E).__name__}')
                break
            if r('div.r-list-sep'):  # 移除置底文
                posts.extend([f"{base}{pq(s).find('div.title a').attr('href')}" for s in r('div.r-list-sep').prev_all('div.r-ent')][::-1])
            else:
                posts.extend([f"{base}{pq(s).attr('href')}" for s in r('div.r-ent div.title a')][::-1])
            if r('div.btn-group-paging a:nth-child(2)').attr('href'):  # 取得上一頁網址
                forum_url = base + r('div.btn-group-paging a:nth-child(2)').attr('href')
            else:
                break
        return posts[:n][::-1]

    async def get_post(post_url: str, **kwargs) -> dict:
        try:
            async with session.get(post_url) as response:
                body = await response.text()
                r = pq(body)
        except aiohttp.web.HTTPException as E:
            logger.warning(f'PTT post@{inspect.stack()[0][3]}: {type(E).__name__}')
            logger.debug(f'{post_url}@{inspect.stack()[0][3]}: {type(E).__name__}')
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

    async def send_webhooks(author, title, time, content, post_url, forum, **kwargs):
        embed = discord.Embed(title=title, url=post_url, description=content)
        embed.set_author(name=author)
        embed.add_field(name='文章網址', value=post_url, inline=False)
        embed.add_field(name='時間', value=f'<t:{time}>', inline=False)
        for webhook in [discord.Webhook.from_url(webhook, session=session) for webhook in conf['ptt']['webhook']]:
            try:
                await webhook.send(username=f'PTT/{forum}', embed=embed, avatar_url=conf['ptt']['avatar'])
            except discord.HTTPException as E:
                logger.warning(f'Discord@{inspect.stack()[0][3]}: {type(E).__name__}')

    async with aiohttp.ClientSession(headers={"Cookie": "over18=1"}) as session:
        posts = await get_forum(forum=forum, n=conf['ptt']['n'], **kwargs)
        async with aiosqlite.connect('data/ptt.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{forum}` ("url" UNIQUE, "author", "title", "time" INTEGER, "content");')
            for post_url in posts:
                cursor = await db.execute(f'SELECT url FROM `{forum}` WHERE url=?;', [post_url])
                if not await cursor.fetchall():
                    try:
                        post = await get_post(post_url, **kwargs)
                        if send: await send_webhooks(**post, **kwargs)
                        await db.execute(f'INSERT INTO `{forum}` VALUES (?,?,?,?,?);',
                                         [post['post_url'], post['author'], post['title'], post['time'], post['content']])
                        await db.commit()
                    except aiohttp.web.HTTPException:
                        continue
                    except Exception as E:
                        logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                        continue
                else:
                    continue
    logger.info(f'Finish PTT {forum}')


async def plurk(query: str, send: bool = True, **kwargs):
    logger = logging.getLogger(f'Plurk/{query}')
    logger.level = logging_level
    logger.info(f'Start Plurk/{query}')

    async def get_search(query: str, n: int = 30, **kwargs) -> tuple[list[dict], dict]:
        search_url = 'https://www.plurk.com/Search/search2'
        post_body = {"query": query}
        plurks = list()
        users = dict()
        while len(plurks) < n:
            try:
                async with session.post(search_url, data=post_body) as response:
                    body = await response.json()
                    plurks.extend(body['plurks'])
                    users.update(body['users'])
                    post_body.update({'after_id': plurks[-1]['id']})
            except aiohttp.web.HTTPException as E:
                logger.warning(f'Plurk/{query}@{inspect.stack()[0][3]}: {type(E).__name__}')
        return plurks[:n][::-1], users

    async def format_plurk(plurk: dict, users: dict, **kwargs) -> dict:
        author = users[str(plurk['user_id'])]['display_name']
        time = int(dateutil.parser.parse(plurk['posted']).timestamp())
        content = plurk['content_raw']
        id = plurk['id']
        url = f"https://www.plurk.com/p/{base36.dumps(id)}"
        return {'author': author, 'time': time, 'content': content, 'id': id, 'post_url': url}

    async def send_webhooks(author, time, content, post_url, query, **kwargs):
        embed = discord.Embed(description=content)
        embed.set_author(name=author, url=post_url)
        embed.add_field(name='文章網址', value=post_url, inline=False)
        embed.add_field(name='時間', value=f'<t:{time}>', inline=False)
        for webhook in [discord.Webhook.from_url(webhook, session=session) for webhook in conf['plurk']['webhook']]:
            try:
                await webhook.send(username=f'Plurk/{query}', embed=embed, avatar_url=conf['plurk']['avatar'])
            except discord.HTTPException as E:
                logger.warning(f'Discord@{inspect.stack()[0][3]}: {type(E).__name__}')

    async with aiohttp.ClientSession() as session:
        plurks, users = await get_search(query=query, n=conf['plurk']['n'], **kwargs)
        async with aiosqlite.connect('data/plurk.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{query}` ("url", "id" INTEGER UNIQUE, "author", "time" INTEGAR, "content");')
            for post in plurks:
                post = await format_plurk(post, users, **kwargs)
                cursor = await db.execute(f'SELECT url FROM `{query}` WHERE url=?;', [post['post_url']])
                if not await cursor.fetchall():
                    try:
                        if send: await send_webhooks(query=query, **post, **kwargs)
                        await db.execute(f'INSERT INTO `{query}` VALUES (?,?,?,?,?);',
                                         [post['post_url'], post['id'], post['author'], post['time'], post['content']])
                        await db.commit()
                    except Exception as E:
                        logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                        continue
                else:
                    continue
    logger.info(f'Finish Plurk {query}')


async def facebook(page: str, send=True, headless=True, delay: int = 3, **kwargs):
    logger = logging.getLogger(f'Facebook/{page}')
    logger.level = logging_level
    logger.info(f'Start Facebook/{page}')

    async def get_page(page: str, n: int = 30, delay: int = 3, **kwargs) -> list[str]:
        try:
            url = f'https://www.facebook.com/{page}'
            driver.get(url)
            await asyncio.sleep(delay)
            try:
                driver.find_element(By.CSS_SELECTOR, 'div[role="dialog"]>div>div>i.x1b0d499').click_safe()  # 移除登入視窗
            except selenium.common.exceptions.NoSuchElementException:
                pass
            prevhigh = driver.execute_script("return document.body.scrollHeight;")
            while len(driver.find_elements(By.CSS_SELECTOR, 'div.xh8yej3>* a.x1heor9g.xt0b8zv.xo1l8bm')) < n:
                if driver.current_url.split('?', 1)[0] == 'https://www.facebook.com/login/':
                    logger.info(f'Facebook/{page}@{inspect.stack()[0][3]}: Require login')
                    logger.debug(f'{url}@{inspect.stack()[0][3]}: Require login')
                    return []
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(delay)
                high = driver.execute_script("return document.body.scrollHeight;")  # scroll to end
                if high == prevhigh: break
                else: prevhigh = driver.execute_script("return document.body.scrollHeight;")
            return [
                post.get_attribute('href').split('?')[0] for post in driver.find_elements(By.CSS_SELECTOR, 'div.xh8yej3>* a.x1heor9g.xt0b8zv.xo1l8bm')
            ][:n][::-1]
        except selenium.common.exceptions.WebDriverException as E:
            logger.warning(f'Facebook/{page}@{inspect.stack()[0][3]}: {type(E).__name__}')
            raise E

    async def get_post(post_url: str, delay: int = 3, **kwargs) -> dict:
        try:
            driver.get(post_url)
        except selenium.common.exceptions.WebDriverException as E:
            logger.warning(f'Facebook post@{inspect.stack()[0][3]}: {type(E).__name__}')
            logger.debug(f'{post_url}@{inspect.stack()[0][3]}: {type(E).__name__}')
            raise E
        if driver.current_url.split('?', 1)[0] == 'https://www.facebook.com/login/':
            logger.info(f'Facebook post@{inspect.stack()[0][3]}: Require login')
            logger.debug(f'{post_url}@{inspect.stack()[0][3]}: Require login')
            raise selenium.common.exceptions.WebDriverException
        base, _, id, _ = driver.find_element(By.CSS_SELECTOR, 'link[rel="canonical"]').get_attribute('href').rsplit('/', 3)
        url = f'{base}/{id}'
        encrypt_url = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:url"]').get_attribute('content')
        avatar = driver.find_element(By.CSS_SELECTOR, 'div._38vo>div>img').get_attribute('src')
        page = driver.find_element(By.CSS_SELECTOR, 'span.fwb>a').text
        time = int(driver.find_element(By.CSS_SELECTOR, 'abbr._5ptz').get_attribute('data-utime'))
        content = driver.find_element(By.CSS_SELECTOR, 'div._5pbx').text
        return {'page': page, 'avatar': avatar, 'time': time, 'content': content, 'id': id, 'post_url': url, 'encrypt_url': encrypt_url}

    async def send_webhooks(page, avatar, time, content, post_url, **kwargs):
        embed = discord.Embed(description=content)
        embed.set_author(name=page, url=post_url)
        embed.add_field(name='文章網址', value=post_url, inline=False)
        embed.add_field(name='時間', value=f'<t:{time}>', inline=False)
        async with aiohttp.ClientSession() as session:
            for webhook in [discord.Webhook.from_url(webhook, session=session) for webhook in conf['facebook']['webhook']]:
                try:
                    await webhook.send(username=f'Facebook/{page}', embed=embed, avatar_url=avatar)
                except discord.HTTPException as E:
                    logger.warning(f'Discord@{inspect.stack()[0][3]}: {type(E).__name__}')

    options = uc.ChromeOptions()
    if headless: options.add_argument('--headless')
    with uc.Chrome(options=options) as driver:
        try:
            posts = await get_page(page=page, n=conf['facebook']['n'], delay=conf['facebook']['delay'], **kwargs)
        except selenium.common.exceptions.WebDriverException:
            return
        async with aiosqlite.connect('data/facebook.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{page}` ("id" INTEGER, "url", "encrypt_url", "time" INTEGAR, "content");')
            for post_url in posts:
                cursor = await db.execute(f'SELECT url, encrypt_url FROM `{page}` WHERE url=? or encrypt_url=?;', [post_url, post_url])
                if not await cursor.fetchall():
                    if '/events/' in post_url or '/videos/' in post_url:  # ignore event and video
                        await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?,?);', [post_url.rsplit('/', 2)[1], post_url, None, None, None])
                        await db.commit()
                        continue
                    try:
                        await asyncio.sleep(delay)
                        post = await get_post(post_url, **kwargs)
                        cursor = await db.execute(f'SELECT id FROM `{page}` WHERE id=?;', [post['id']])
                        if not await cursor.fetchall():
                            if send: await send_webhooks(**post, **kwargs)
                            await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?,?);',
                                             [post['id'], post['post_url'], post['encrypt_url'], post['time'], post['content']])
                            await db.commit()
                        else:
                            await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?,?);',
                                             [post['id'], post['post_url'], post['encrypt_url'], None, None])
                            await db.commit()
                            logger.debug(f'@{inspect.stack()[0][3]}: Repeated post id')
                    except selenium.common.exceptions.WebDriverException:
                        continue
                    except Exception as E:
                        logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                        continue
                else:
                    continue
    logger.info(f'Finish Facebook {page}')


async def dcard(forum: str, send=True, headless=True, delay: int = 3, **kwargs) -> list[str]:
    logger = logging.getLogger(f'Dcard/{forum}')
    logger.level = logging_level
    logger.info(f'Start Dcard/{forum}')

    async def get_forum(forum: str, n: int = 30, delay: int = 3, **kwargs):
        try:
            url = f'https://www.dcard.tw/f/{forum}?latest=true'
            driver.get(url)
            await asyncio.sleep(delay)
            try:
                for _ in range(3):
                    driver.find_element(By.CSS_SELECTOR, '#challenge-stage').click_safe()  # 移除登入視窗
                    await asyncio.sleep(delay)
                else:
                    if driver.find_element(By.CSS_SELECTOR, '#challenge-stage'):
                        logger.info(f'Dcard/{forum}@{inspect.stack()[0][3]}: Captcha failed')
                        logger.debug(f'{url}@{inspect.stack()[0][3]}: Captcha failed')
                        return []
            except selenium.common.exceptions.NoSuchElementException:
                pass
            prevhigh = driver.execute_script("return document.body.scrollHeight;")
            while len(driver.find_elements(By.CSS_SELECTOR, 'a.atm_cs_1urozh')) < n:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(delay)
                high = driver.execute_script("return document.body.scrollHeight;")  # scroll to end
                if high == prevhigh: break
                else: prevhigh = driver.execute_script("return document.body.scrollHeight;")
            return [post.get_attribute('href') for post in driver.find_elements(By.CSS_SELECTOR, 'a.atm_cs_1urozh')][:n][::-1]
        except selenium.common.exceptions.WebDriverException as E:
            logger.warning(f'Dcard forum {forum} {type(E).__name__}')
            raise E

    async def get_post(post_url: str, delay: int = 3, **kwargs) -> dict:
        try:
            driver.get(post_url)
        except selenium.common.exceptions.WebDriverException as E:
            logger.warning(f'Dcard post@{inspect.stack()[0][3]}: {type(E).__name__}')
            logger.debug(f'{post_url}@{inspect.stack()[0][3]}: {type(E).__name__}')
            raise E
        _, forum, _, id = post_url.rsplit('/', 3)
        author = driver.find_element(By.CSS_SELECTOR, 'div.a12lr2bo').text
        try:
            avatar = driver.find_element(By.CSS_SELECTOR, 'div.a1vkrnev > div > span > img').get_attribute('src').split('?')[0]
        except selenium.common.exceptions.NoSuchElementException:
            avatar = None
        title = driver.find_element(By.CSS_SELECTOR, 'article > div.atm_9s_1txwivl > div > h1').text
        time = int(
            dateutil.parser.parse(
                driver.find_element(By.CSS_SELECTOR,
                                    'article > div.atm_c8_3rwk2t  > div.atm_7l_1w35wrm > time').get_attribute('datetime')).timestamp())
        content = driver.find_element(By.CSS_SELECTOR, 'article > div.atm_lo_c0ivcw').text
        return {'forum': forum, 'author': author, 'avatar': avatar, 'title': title, 'time': time, 'content': content, 'id': id, 'post_url': post_url}

    async def send_webhooks(forum, author, avatar, title, time, content, post_url, **kwargs):
        embed = discord.Embed(title=title, url=post_url, description=content)
        embed.set_author(name=author, url=post_url, icon_url=avatar)
        embed.add_field(name='文章網址', value=post_url, inline=False)
        embed.add_field(name='時間', value=f'<t:{time}>', inline=False)
        async with aiohttp.ClientSession() as session:
            for webhook in [discord.Webhook.from_url(webhook, session=session) for webhook in conf['dcard']['webhook']]:
                try:
                    await webhook.send(username=f'Dcard/{forum}', embed=embed)
                except discord.HTTPException as E:
                    logger.warning(f'Discord@{inspect.stack()[0][3]}: {type(E).__name__}')

    options = uc.ChromeOptions()
    if headless: options.add_argument('--headless')
    with uc.Chrome(options=options) as driver:
        try:
            posts = await get_forum(forum=forum, n=conf['dcard']['n'], delay=conf['dcard']['delay'], **kwargs)
        except selenium.common.exceptions.WebDriverException:
            return
        async with aiosqlite.connect('data/dcard.db') as db:
            await db.execute(
                f'CREATE TABLE IF NOT EXISTS `{forum}` ("id" INTEGER UNIQUE, "url", "author", "avatar", "title", "time" INTEGAR, "content");')
            for post_url in posts:
                cursor = await db.execute(f'SELECT url FROM `{forum}` WHERE url=?;', [post_url])
                if not await cursor.fetchall():
                    try:
                        await asyncio.sleep(delay)
                        post = await get_post(post_url, **kwargs)
                        if send: await send_webhooks(**post, **kwargs)
                        await db.execute(f'INSERT INTO `{forum}` VALUES (?,?,?,?,?,?,?);',
                                         [post['id'], post['post_url'], post['author'], post['avatar'], post['title'], post['time'], post['content']])
                        await db.commit()
                    except selenium.common.exceptions.WebDriverException:
                        continue
                    except Exception as E:
                        logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                        continue
                else:
                    continue
    logger.info(f'Finish Dcard {forum}')


async def cna(send: bool = True, **kwargs):
    logger = logging.getLogger(f'CNA')
    logger.level = logging_level
    logger.info(f'Start CNA')

    async def get_category(keywords: list[str], n: int = 100, **kwargs) -> tuple[str]:
        url = 'https://www.cna.com.tw/cna2018api/api/WNewsList'
        page = list()
        for x in range(math.ceil(n / 100)):
            try:
                post_body = {"action": "0", "category": "aall", "pagesize": "20", "pageidx": x + 1}
                async with session.post(url, data=post_body) as response:
                    body = await response.json()
                    for i in body['ResultData']['Items']:
                        for keyword in keywords:
                            if keyword in i['HeadLine']:
                                page.append(i['PageUrl'])
            except aiohttp.web.HTTPException as E:
                logger.warning(f'CNA@{inspect.stack()[0][3]}: {type(E).__name__}')
        return page

    async def get_page(page_url: str, **kwargs) -> dict:
        try:
            async with session.get(page_url) as response:
                body = await response.text()
                r = pq(body)
        except aiohttp.web.HTTPException as E:
            logger.warning(f'CNA page@{inspect.stack()[0][3]}: {type(E).__name__}')
            logger.debug(f'{page_url}@{inspect.stack()[0][3]}: {type(E).__name__}')
            raise E
        title = r('div.centralContent h1').text()
        time = round(dateutil.parser.parse(r('div.centralContent div.timeBox').text()).timestamp())
        content = pq(r('div.centralContent div.paragraph')[0]).find('p').text()
        return {'title': title, 'time': time, 'content': content, 'page_url': page_url}

    async def send_webhooks(title, time, content, page_url, **kwargs):
        embed = discord.Embed(title=title, description=content, url=page_url)
        embed.add_field(name='文章網址', value=page_url, inline=False)
        embed.add_field(name='時間', value=f'<t:{time}>', inline=False)
        for webhook in [discord.Webhook.from_url(webhook, session=session) for webhook in conf['cna']['webhook']]:
            try:
                await webhook.send(username=f'中央社', embed=embed, avatar_url=conf['cna']['avatar'])
            except discord.HTTPException as E:
                logger.warning(f'Discord@{inspect.stack()[0][3]}: {type(E).__name__}')

    async with aiohttp.ClientSession() as session:
        pages = await get_category(keywords=conf['cna']['keywords'], n=conf['cna']['n'], **kwargs)
        async with aiosqlite.connect('data/cna.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `news` ("url" UNIQUE, "title", "time" INTEGAR, "content");')
            for page_url in pages:
                cursor = await db.execute(f'SELECT url FROM `news` WHERE url=?;', [page_url])
                if not await cursor.fetchall():
                    try:
                        post = await get_page(page_url, **kwargs)
                        if send: await send_webhooks(**post, **kwargs)
                        await db.execute(f'INSERT INTO `news` VALUES (?,?,?,?);', [post['page_url'], post['title'], post['time'], post['content']])
                        await db.commit()
                    except Exception as E:
                        logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                        continue
                else:
                    continue
    logger.info(f'Finish CNA')


async def main():
    logger = logging.getLogger('app')
    logger.level = logging_level
    logger.info(f'Start webcrawling')

    coro = list()
    if conf['ptt']['run']: coro.extend([ptt(forum=forum, send=conf['ptt']['send']) for forum in conf['ptt']['forum']])
    if conf['plurk']['run']: coro.extend([plurk(query=query, send=conf['plurk']['send']) for query in conf['plurk']['query']])
    if conf['facebook']['run']:
        coro.extend([
            facebook(page=page, send=conf['facebook']['send'], headless=conf['facebook']['headless'], delay=conf['facebook']['delay'])
            for page in conf['facebook']['page']
        ])
    if conf['dcard']['run']:
        coro.extend([
            dcard(forum=forum, send=conf['facebook']['send'], headless=conf['facebook']['headless'], delay=conf['facebook']['delay'])
            for forum in conf['dcard']['forum']
        ])
    if conf['cna']['run']: coro.extend([cna(send=conf['cna']['send'])])

    R = await asyncio.gather(*coro, return_exceptions=True)
    for i, E in enumerate(R):
        if isinstance(E, Exception):
            logger.critical(f'app@{coro[i]}: {type(E).__name__}: {E}')
    logger.info('Finish webcrawling')


if __name__ == '__main__':
    asyncio.run(main())
