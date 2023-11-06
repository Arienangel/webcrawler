import asyncio
import inspect
import logging
import os
import re

import aiosqlite
import yaml

with open('config.yaml', encoding='utf-8') as f:
    conf = yaml.load(f, yaml.SafeLoader)
if not os.path.exists('data'): os.makedirs('data')

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', level=conf['app']['logging_level'])
logger = logging.getLogger('App')


async def run_ptt(forums: list | str, **kwargs):
    import ptt
    import webhook
    if isinstance(forums, str): forums = [forums]
    for forum in forums:
        logger.info(f'Start PTT crawling: {forum}')
        try:
            posts = await ptt.get_forum(forum, n=conf['ptt']['n'], **kwargs)
            await asyncio.sleep(conf['ptt']['delay'])
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}(forum={forum}): {type(E).__name__}: {E}')
            continue
        async with aiosqlite.connect('data/ptt.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{forum}` ("url" TEXT UNIQUE, "author" TEXT, "title" TEXT, "time" INTEGER, "content" TEXT);')
            for post_url in posts:
                try:
                    cursor = await db.execute(f'SELECT url FROM `{forum}` WHERE url=?;', [post_url])
                    if not await cursor.fetchall():
                        await asyncio.sleep(delay=conf['ptt']['delay'])
                        post = await ptt.get_post(post_url, **kwargs)
                        for webhook_url in conf['webhook']['discord']['ptt']:
                            await webhook.send_discord(webhook_url, post['author'], post['title'], post['time'], post['content'][:conf['webhook']['max_length']], post['post_url'])
                        await db.execute(f'INSERT INTO `{forum}` VALUES (?,?,?,?,?);', [post['post_url'], post['author'], post['title'], post['time'], post['content']])
                        await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}(url={post_url}): {type(E).__name__}: {E}')
                    continue


async def run_plurk(querys: list | str, **kwargs):
    import plurk
    import webhook
    if isinstance(querys, str): querys = [querys]
    posts = []
    for query in querys:
        logger.info(f'Start Plurk crawling: {query}')
        try:
            posts.extend(await plurk.get_search(query, n=conf['plurk']['n'], **kwargs))
            await asyncio.sleep(conf['plurk']['delay'])
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}(query={query}): {type(E).__name__}: {E}')
            continue
    if posts:
        async with aiosqlite.connect('data/plurk.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `posts` ("url" TEXT UNIQUE, "author" TEXT, "time" INTEGAR, "content" TEXT);')
            for post in sorted(posts, key=lambda p: p['time']):
                try:
                    cursor = await db.execute(f'SELECT url FROM `posts` WHERE url=?;', [post['post_url']])
                    if not await cursor.fetchall():
                        for webhook_url in conf['webhook']['discord']['plurk']:
                            await webhook.send_discord(webhook_url, post['author'], None, post['time'], post['content'][:conf['webhook']['max_length']], post['post_url'])
                        await db.execute(f'INSERT INTO `posts` VALUES (?,?,?,?);', [post['post_url'], post['author'], post['time'], post['content']])
                        await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}(url={post["post_url"]}): {type(E).__name__}: {E}')
                    continue


async def run_dcard(forums: list | str = [], topics: list | str = [], **kwargs):
    import dcard
    import webhook
    if isinstance(forums, str): forums = [forums]
    if isinstance(topics, str): topics = [topics]
    posts = []
    for forum in forums:
        logger.info(f'Start Dcard crawling: {forum}')
        try:
            posts.extend(await dcard.get_forum(forum, n=conf['dcard']['n'], delay=conf['dcard']['delay'], retry=conf['dcard']['retry'], **kwargs))
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}(forum={forum}): {type(E).__name__}: {E}'.split('\n')[0])
            continue
    for topic in topics:
        logger.info(f'Start Dcard crawling: {topic}')
        try:
            posts.extend(await dcard.get_topic(topic, n=conf['dcard']['n'], delay=conf['dcard']['delay'], retry=conf['dcard']['retry'], **kwargs))
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}(forum={forum}): {type(E).__name__}: {E}'.split('\n')[0])
            continue
    if posts:
        async with aiosqlite.connect('data/dcard.db') as db:
            for post_url, forum, id in sorted(map(lambda url: re.search(r'https://www.dcard.tw/f/(\w+)/p/(\d+)', url).group(0, 1, 2), posts), key=lambda x: x[-1]):
                await db.execute(f'CREATE TABLE IF NOT EXISTS `{forum}` ("url" TEXT UNIQUE, "author" TEXT, "title" TEXT, "time" INTEGAR, "content" TEXT);')
                try:
                    cursor = await db.execute(f'SELECT url FROM `{forum}` WHERE url=?;', [post_url])
                    if not await cursor.fetchall():
                        post = await dcard.get_post(post_url, delay=conf['dcard']['delay'], retry=conf['dcard']['retry'], **kwargs)
                        for webhook_url in conf['webhook']['discord']['dcard']:
                            await webhook.send_discord(webhook_url, post['author'], post['title'], post['time'], post['content'][:conf['webhook']['max_length']], post['post_url'])
                        await db.execute(f'INSERT INTO `{forum}` VALUES (?,?,?,?,?);', [post['post_url'], post['author'], post['title'], post['time'], post['content']])
                        await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}(url={post_url}): {type(E).__name__}: {E}'.split('\n')[0])
                    continue


async def run_facebook(pages: list | str, **kwargs):
    import facebook
    import webhook
    if isinstance(pages, str): pages = [pages]
    for page in pages:
        logger.info(f'Start Facebook crawling: {page}')
        try:
            posts = await facebook.get_page(page, n=conf['facebook']['n'], delay=conf['facebook']['delay'], **kwargs)
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}(page={page}): {type(E).__name__}: {E}'.split('\n')[0])
            continue
        async with aiosqlite.connect('data/facebook.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{page}` ("url" TEXT, "encrypt_url" TEXT, "time" INTEGAR, "content" TEXT);')
            for post_url in posts:
                try:
                    cursor = await db.execute(f'SELECT url, encrypt_url FROM `{page}` WHERE url=? or encrypt_url=?;', [post_url, post_url])
                    if not await cursor.fetchall():
                        if '/events/' in post_url or '/videos/' in post_url or '/reel/' in post_url:  # ignore event and video
                            raise NotImplementedError
                        else:
                            post = await facebook.get_post(post_url, **kwargs)
                            cursor = await db.execute(f'SELECT url FROM `{page}` WHERE url=?;', [post['post_url']])
                            if not await cursor.fetchall():
                                for webhook_url in conf['webhook']['discord']['facebook']:
                                    await webhook.send_discord(webhook_url, post['page'], None, post['time'], post['content'][:conf['webhook']['max_length']], post['post_url'])
                                await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [post['post_url'], post['encrypt_url'], post['time'], post['content']])
                                await db.commit()
                            else:
                                await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [post['post_url'], post['encrypt_url'], None, None])
                                await db.commit()
                except NotImplementedError:
                    if 'pfbid' in post_url:
                        await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [None, post_url, None, None])
                    else:
                        await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [post_url, None, None, None])
                    await db.commit()
                    continue
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}(url={post_url}): {type(E).__name__}: {E}'.split('\n')[0])
                    continue


async def run_cna(keywords: list | str, **kwargs):
    import cna
    import webhook
    if isinstance(keywords, str): keywords = [keywords]
    logger.info(f'Start CNA crawling')
    try:
        posts = await cna.get_news(keywords, n=conf['cna']['n'], **kwargs)
    except Exception as E:
        logger.error(f'@{inspect.stack()[0][3]}(keywords={keywords}): {type(E).__name__}: {E}')
    async with aiosqlite.connect('data/cna.db') as db:
        await db.execute(f'CREATE TABLE IF NOT EXISTS `news` ("url" TEXT UNIQUE, "title" TEXT, "time" INTEGAR, "content" TEXT);')
        for post_url in posts:
            try:
                cursor = await db.execute(f'SELECT url FROM `news` WHERE url=?;', [post_url])
                if not await cursor.fetchall():
                    post = await cna.get_post(post_url, **kwargs)
                    for webhook_url in conf['webhook']['discord']['cna']:
                        await webhook.send_discord(webhook_url, None, post['title'], post['time'], post['content'][:conf['webhook']['max_length']], post['post_url'])
                    await db.execute(f'INSERT INTO `news` VALUES (?,?,?,?);', [post['post_url'], post['title'], post['time'], post['content']])
                    await db.commit()
            except Exception as E:
                logger.error(f'@{inspect.stack()[0][3]}(url={post_url}): {type(E).__name__}: {E}')
                continue


async def main():
    jobs = [
        run_ptt(conf['ptt']['forum']),
        run_plurk(conf['plurk']['query']),
        run_dcard(conf['dcard']['forum'], conf['dcard']['topic']),
        run_facebook(conf['facebook']['page']),
        run_cna(conf['cna']['keywords']),
    ]
    logger.info(f'Start webcrawling')
    res = await asyncio.gather(*jobs, return_exceptions=True)
    for i, E in enumerate(res):
        if isinstance(E, Exception):
            logger.critical(f'app@{jobs[i]}: {type(E).__name__}: {E}')
    logger.info('Finish webcrawling')


if __name__ == '__main__':
    asyncio.run(main())
