import asyncio
import inspect
import logging
import os

import aiosqlite
import yaml

with open('config.yaml', encoding='utf-8') as f:
    conf = yaml.load(f, yaml.SafeLoader)
if not os.path.exists('data'): os.makedirs('data')

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', level=conf['app']['logging_level'])
logger = logging.getLogger(f'App')


async def run_ptt(forums: list):
    import ptt
    import webhook
    for forum in forums:
        logger.info(f'Start PTT crawling: {forum}')
        try:
            posts = await ptt.get_forum(forum, n=conf['ptt']['n'])
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
            continue
        async with aiosqlite.connect('data/ptt.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{forum}` ("url" TEXT UNIQUE, "author" TEXT, "title" TEXT, "time" INTEGER, "content" TEXT);')
            for post_url in posts:
                try:
                    cursor = await db.execute(f'SELECT url FROM `{forum}` WHERE url=?;', [post_url])
                    if not await cursor.fetchall():
                        post = await ptt.get_post(post_url)
                        for webhook_url in conf['webhook']['discord']['ptt']:
                            await webhook.send_discord(webhook_url, post['author'], post['title'], post['time'], post['content'], post['post_url'])
                        await db.execute(f'INSERT INTO `{forum}` VALUES (?,?,?,?,?);', [post['post_url'], post['author'], post['title'], post['time'], post['content']])
                        await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                    continue


async def run_plurk(querys: list):
    import plurk
    import webhook
    for query in querys:
        logger.info(f'Start Plurk crawling: {query}')
        try:
            posts = await plurk.get_search(query, n=conf['plurk']['n'])
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
            continue
        async with aiosqlite.connect('data/plurk.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `posts` ("url" TEXT UNIQUE, "author" TEXT, "time" INTEGAR, "content" TEXT);')
            for post in posts:
                try:
                    cursor = await db.execute(f'SELECT url FROM `posts` WHERE url=?;', [post['post_url']])
                    if not await cursor.fetchall():
                        for webhook_url in conf['webhook']['discord']['plurk']:
                            await webhook.send_discord(webhook_url, post['author'], None, post['time'], post['content'], post['post_url'])
                        await db.execute(f'INSERT INTO `posts` VALUES (?,?,?,?);', [post['post_url'], post['author'], post['time'], post['content']])
                        await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                    continue


async def run_dcard(forums: list):
    import dcard
    import webhook
    for forum in forums:
        logger.info(f'Start Dcard crawling: {forum}')
        try:
            posts = await dcard.get_forum(forum, n=conf['dcard']['n'], delay=conf['dcard']['delay'])
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
            continue
        async with aiosqlite.connect('data/dcard.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{forum}` ("url" TEXT UNIQUE, "author" TEXT, "title" TEXT, "time" INTEGAR, "content" TEXT);')
            for post_url in posts:
                try:
                    cursor = await db.execute(f'SELECT url FROM `{forum}` WHERE url=?;', [post_url])
                    if not await cursor.fetchall():
                        post = await dcard.get_post(post_url)
                        for webhook_url in conf['webhook']['discord']['dcard']:
                            await webhook.send_discord(webhook_url, post['author'], post['title'], post['time'], post['content'], post['post_url'])
                        await db.execute(f'INSERT INTO `{forum}` VALUES (?,?,?,?,?);', [post['post_url'], post['author'], post['title'], post['time'], post['content']])
                        await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                    continue


async def run_facebook(pages: list):
    import facebook
    import webhook
    for page in pages:
        logger.info(f'Start Facebook crawling: {page}')
        try:
            posts = await facebook.get_page(page, n=conf['facebook']['n'], delay=conf['facebook']['delay'])
        except Exception as E:
            logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
            continue
        async with aiosqlite.connect('data/facebook.db') as db:
            await db.execute(f'CREATE TABLE IF NOT EXISTS `{page}` ("url" TEXT, "encrypt_url" TEXT, "time" INTEGAR, "content" TEXT);')
            for post_url in posts:
                try:
                    cursor = await db.execute(f'SELECT url, encrypt_url FROM `{page}` WHERE url=? or encrypt_url=?;', [post_url, post_url])
                    if not await cursor.fetchall():
                        if '/events/' in post_url or '/videos/' in post_url:  # ignore event and video
                            await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [post_url.rsplit('/', 2)[1], post_url, None, None])
                            await db.commit()
                            continue
                        else:
                            post = await facebook.get_post(post_url)
                            cursor = await db.execute(f'SELECT url FROM `{page}` WHERE url=?;', [post['post_url']])
                            if not await cursor.fetchall():
                                for webhook_url in conf['webhook']['discord']['facebook']:
                                    await webhook.send_discord(webhook_url, post['page'], None, post['time'], post['content'], post['post_url'])
                                await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [post['post_url'], post['encrypt_url'], post['time'], post['content']])
                                await db.commit()
                            else:
                                await db.execute(f'INSERT INTO `{page}` VALUES (?,?,?,?);', [post['post_url'], post['encrypt_url'], None, None])
                                await db.commit()
                except Exception as E:
                    logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                    continue


async def run_cna(keywords: list):
    import cna
    import webhook
    logger.info(f'Start CNA crawling')
    try:
        posts = await cna.get_news(keywords, n=conf['cna']['n'])
    except Exception as E:
        logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
    async with aiosqlite.connect('data/cna.db') as db:
        await db.execute(f'CREATE TABLE IF NOT EXISTS `news` ("url" TEXT UNIQUE, "title" TEXT, "time" INTEGAR, "content" TEXT);')
        for post_url in posts:
            try:
                cursor = await db.execute(f'SELECT url FROM `news` WHERE url=?;', [post_url])
                if not await cursor.fetchall():
                    post = await cna.get_post(post_url)
                    for webhook_url in conf['webhook']['discord']['cna']:
                        await webhook.send_discord(webhook_url, None, post['title'], post['time'], post['content'], post['post_url'])
                    await db.execute(f'INSERT INTO `news` VALUES (?,?,?,?);', [post['post_url'], post['title'], post['time'], post['content']])
                    await db.commit()
            except Exception as E:
                logger.error(f'@{inspect.stack()[0][3]}: {type(E).__name__}: {E}')
                continue


async def main():
    jobs = [
        run_ptt(conf['ptt']['forum']),
        run_plurk(conf['plurk']['query']),
        run_dcard(conf['dcard']['forum']),
        run_facebook(conf['facebook']['page']),
        run_cna(conf['cna']['keyword']),
    ]
    logger.info(f'Start webcrawling')
    res = await asyncio.gather(*jobs, return_exceptions=True)
    for i, E in enumerate(res):
        if isinstance(E, Exception):
            logger.critical(f'app@{jobs[i]}: {type(E).__name__}: {E}')
    logger.info('Finish webcrawling')


if __name__ == '__main__':
    asyncio.run(main())