import logging
import queue
import sqlite3
import threading
import time

import requests
import yaml

from webcrawler import dcard, facebook, plurk, ptt
from webcrawler.webdriver import ChromeProcess


class dcard_crawler:
    _logger = logging.getLogger('Dcard crawler')

    def __init__(self, browser: ChromeProcess = None, chromeprocess_kwargs: dict = {}, do_forum_get: bool = True, do_post_get: bool = True):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or ChromeProcess(**chromeprocess_kwargs)
        self.do_forum_get = do_forum_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.stop()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | dcard.Forum], forum_get_kwargs: dict = {}, post_get_kwargs: dict = {}, posts_db_path: str = '', comments_db_path: str = ''):
        self.stop_thread = False
        threading.Thread(target=self.run_crawler, args=[forums, forum_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if posts_db_path:
                    if len(self.queue_posts.queue): self.write_posts_db(self.queue_posts.get(), posts_db_path)
                if comments_db_path:
                    if len(self.queue_comments.queue): self.write_comments_db(self.queue_comments.get(), comments_db_path)
        finally:
            self.exit()

    def run_crawler(self, forums: list[str | dcard.Forum], forum_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        used_posts = set()
        try:
            self.browser.get('https://www.dcard.tw/f', referrer='https://www.google.com/')
            time.sleep(20)
            for forum in forums:
                if isinstance(forum, str): forum = dcard.Forum(alias=forum)
                if self.do_forum_get:
                    try:
                        self._logger.info(f'Get {forum.__repr__()}')
                        forum.get(self.browser, **forum_get_kwargs)
                        time.sleep(8)
                    except Exception as E:
                        self._logger.warning(f'Get forum failed: {type(E)}:{E.args}: {forum.__repr__()}')
                        continue
                if self.do_post_get:
                    for post in forum.posts[::-1]:
                        if post.id in used_posts:
                            continue
                        else:
                            used_posts.add(post.id)
                        try:
                            self._logger.info(f'Get {post.__repr__()}')
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_posts.put([post])
                            self.queue_comments.put(post.comments)
                            time.sleep(8)
                        except Exception as E:
                            self._logger.warning(f'Get post failed: {type(E)}:{E.args}: {post.__repr__()}')
                            continue
                else:
                    self.queue_posts.put(forum.posts[::-1])
        finally:
            self.stop_thread = True

    def write_posts_db(self, posts: list[dcard.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.forum.alias}` ("id" INTEGAR UNIQUE, "created_time" INTEGAR, "author_school" TEXT, "author_department" TEXT, "title" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.forum.alias}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.forum.alias}` VALUES (?,?,?,?,?,?);', [post.id, int(post.created_time.timestamp()), post.author.school, post.author.department, post.title, post.content])
                        self._logger.debug(f'Write db: {post.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {post.__repr__()}')
                    continue

    def write_comments_db(self, comments: list[dcard.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGAR UNIQUE, "created_time" INTEGAR, "author_school" TEXT, "author_department" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?);', [comment.floor, int(comment.created_time.timestamp()), comment.author.school, comment.author.department, comment.content])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class facebook_crawler:
    _logger = logging.getLogger('Facebook crawler')

    def __init__(self, browser: ChromeProcess = None, chromeprocess_kwargs: dict = {}, do_page_get: bool = True, do_post_get: bool = True):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or ChromeProcess(**chromeprocess_kwargs)
        self.do_page_get = do_page_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.stop()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, pages: list[str | facebook.Page], page_get_kwargs: dict = {}, post_get_kwargs: dict = {}, posts_db_path: str = '', comments_db_path: str = ''):
        self.stop_thread = False
        threading.Thread(target=self.run_crawler, args=[pages, page_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if posts_db_path:
                    if len(self.queue_posts.queue): self.write_posts_db(self.queue_posts.get(), posts_db_path)
                if comments_db_path:
                    if len(self.queue_comments.queue): self.write_comments_db(self.queue_comments.get(), comments_db_path)
        finally:
            self.exit()

    def run_crawler(self, pages: list[str | facebook.Page], page_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        used_posts = set()
        try:
            self.browser.get('https://www.facebook.com/', referrer='https://www.google.com/')
            time.sleep(20)
            for page in pages:
                if isinstance(page, str): page = facebook.Page(alias=page)
                if self.do_page_get:
                    try:
                        self._logger.info(f'Get {page.__repr__()}')
                        page.get(self.browser, **page_get_kwargs)
                        time.sleep(5)
                    except Exception as E:
                        self._logger.warning(f'Get page failed: {type(E)}:{E.args}: {page.__repr__()}')
                        continue
                if self.do_post_get:
                    for post in page.posts[::-1]:
                        if post.id in used_posts:
                            continue
                        else:
                            used_posts.add(post.id)
                        try:
                            self._logger.info(f'Get {post.__repr__()}')
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_posts.put([post])
                            self.queue_comments.put(post.comments)
                            time.sleep(5)
                        except Exception as E:
                            self._logger.warning(f'Get post failed: {type(E)}:{E.args}: {post.__repr__()}')
                            continue
                else:
                    self.queue_posts.put(page.posts[::-1])
        finally:
            self.stop_thread = True

    def write_posts_db(self, posts: list[facebook.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.page.id}` ("id" INTEGAR UNIQUE, "pfbid" TEXT, "created_time" INTEGAR, "title" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.page.id}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.page.id}` VALUES (?,?,?,?,?);', [post.id, post.pfbid, int(post.created_time.timestamp()), post.title, post.content])
                        self._logger.debug(f'Write db: {post.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {post.__repr__()}')
                    continue

    def write_comments_db(self, comments: list[facebook.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("id" INTEGAR UNIQUE, "created_time" INTEGAR, "author_id" INTEGAR, "author_alias" TEXT, "author_name" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{comment.post.id}` WHERE id=?;', [comment.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?,?);', [comment.id, int(comment.created_time.timestamp()), comment.author.id, comment.author.alias, comment.author.name, comment.content])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class plurk_crawler:
    _logger = logging.getLogger('Plurk crawler')

    def __init__(self, browser: requests.Session = None, do_search_get: bool = True, do_post_get: bool = False):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or requests.Session()
        self.do_search_get = do_search_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.close()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, searches: list[str | plurk.Search], search_get_kwargs: dict = {}, post_get_kwargs: dict = {}, posts_db_path: str = '', comments_db_path: str = ''):
        self.stop_thread = False
        threading.Thread(target=self.run_crawler, args=[searches, search_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if posts_db_path:
                    if len(self.queue_posts.queue): self.write_posts_db(self.queue_posts.get(), posts_db_path)
                if comments_db_path:
                    if len(self.queue_comments.queue): self.write_comments_db(self.queue_comments.get(), comments_db_path)
        finally:
            self.exit()

    def run_crawler(self, searches: list[str | plurk.Search], search_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        used_posts = set()
        try:
            for search in searches:
                if isinstance(search, str): search = plurk.Search(query=search)
                if self.do_search_get:
                    try:
                        self._logger.info(f'Get {search.__repr__()}')
                        search.get(self.browser, **search_get_kwargs)
                        time.sleep(16)
                    except Exception as E:
                        self._logger.warning(f'Get search failed: {type(E)}:{E.args}: {search.__repr__()}')
                        continue
                if self.do_post_get:
                    for post in search.posts[::-1]:
                        if post.id in used_posts:
                            continue
                        else:
                            used_posts.add(post.id)
                        try:
                            self._logger.info(f'Get {post.__repr__()}')
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_posts.put([post])
                            self.queue_comments.put(post.comments)
                            time.sleep(16)
                        except Exception as E:
                            self._logger.warning(f'Get post failed: {type(E)}:{E.args}: {post.__repr__()}')
                            continue
                else:
                    self.queue_posts.put(search.posts[::-1])
        finally:
            self.stop_thread = True

    def write_posts_db(self, posts: list[plurk.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.query}` ("id" INTEGAR UNIQUE, "time" INTEGAR, "author_id" INTEGAR, "author_nickname" TEXT, "author_displayname" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.query}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.query}` VALUES (?,?,?,?,?,?);', [post.id, int(post.created_time.timestamp()), post.author.id, post.author.nickname, post.author.display_name, post.content_raw])
                        self._logger.debug(f'Write db: {post.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {post.__repr__()}')
                    continue

    def write_comments_db(self, comments: list[plurk.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGAR UNIQUE, "id" INTEGAR, "time" INTEGAR, "author_id" INTEGAR, "author_nickname" TEXT, "author_displayname" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?,?,?);', [comment.floor, comment.id, int(comment.created_time.timestamp()), comment.author.id, comment.author.nickname, comment.author.display_name, comment.content_raw])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class ptt_crawler:
    _logger = logging.getLogger('PTT crawler')

    def __init__(self, browser: requests.Session = None, do_forum_get: bool = True, do_post_get: bool = True):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or requests.Session()
        self.do_forum_get = do_forum_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.close()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | ptt.Forum], forum_get_kwargs: dict = {}, post_get_kwargs: dict = {}, posts_db_path: str = '', comments_db_path: str = ''):
        self.stop_thread = False
        threading.Thread(target=self.run_crawler, args=[forums, forum_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if posts_db_path:
                    if len(self.queue_posts.queue): self.write_posts_db(self.queue_posts.get(), posts_db_path)
                if comments_db_path:
                    if len(self.queue_comments.queue): self.write_comments_db(self.queue_comments.get(), comments_db_path)
        finally:
            self.exit()

    def run_crawler(self, forums: list[str | ptt.Forum], forum_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        used_posts = set()
        try:
            for forum in forums:
                if isinstance(forum, str): forum = ptt.Forum(name=forum)
                if self.do_forum_get:
                    try:
                        self._logger.info(f'Get {forum.__repr__()}')
                        forum.get(self.browser, **forum_get_kwargs)
                        time.sleep(5)
                    except Exception as E:
                        self._logger.warning(f'Get forum failed: {type(E)}:{E.args}: {forum.__repr__()}')
                        continue
                if self.do_post_get:
                    for post in forum.posts[::-1]:
                        if post.id in used_posts:
                            continue
                        else:
                            used_posts.add(post.id)
                        try:
                            self._logger.info(f'Get {post.__repr__()}')
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_posts.put([post])
                            self.queue_comments.put(post.comments)
                            time.sleep(5)
                        except Exception as E:
                            self._logger.warning(f'Get post failed: {type(E)}:{E.args}: {post.__repr__()}')
                            continue
                else:
                    self.queue_posts.put(forum.posts[::-1])
        finally:
            self.stop_thread = True

    def write_posts_db(self, posts: list[ptt.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.forum.name}` ("id" TEXT UNIQUE, "time" INTEGAR, "author_id" TEXT, "author_name" TEXT, "title" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.forum.name}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.forum.name}` VALUES (?,?,?,?,?,?);', [post.id, int(post.time.timestamp()), post.author.id, post.author.name, post.title, post.content])
                        self._logger.debug(f'Write db: {post.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {post.__repr__()}')
                    continue

    def write_comments_db(self, comments: list[ptt.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGAR UNIQUE, "time" INTEGAR, "reaction" TEXT, "author_id" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?);', [comment.floor, int(comment.time.timestamp()), comment.reaction, comment.author.id, comment.content])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


if __name__ == '__main__':
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', type=str, default="config.yaml")
    parser.add_argument('--log-level', type=int, default=30)
    args = parser.parse_args()
    with open(args.f, encoding='utf-8') as f:
        config = yaml.load(f, yaml.SafeLoader)
    logging.basicConfig(
        format=config['logging']['format'] or '%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=config['logging']['level'] or args.log_level,
    )
    logger = logging.getLogger('App')
    logger.debug(f'GIL enabled: {sys._is_gil_enabled()}')
    logger.info(f'Config file: {args.f}')
    jobs = []
    if config['webcrawler']['dcard']['enable']:
        chromeprocess_kwargs = config['webdriver']['chromeprocess']
        chromeprocess_kwargs.update(config['webcrawler']['dcard']['webdriver']['chromeprocess'])
        jobs.append(threading.Thread(target=dcard_crawler(
            chromeprocess_kwargs=chromeprocess_kwargs,
            do_forum_get=config['webcrawler']['dcard']['do_forum_get'],
            do_post_get=config['webcrawler']['dcard']['do_post_get'],
        ).start_thread, args=[
            config['webcrawler']['dcard']['forums'],
            config['webcrawler']['dcard']['forum_get'],
            config['webcrawler']['dcard']['post_get'],
            config['webcrawler']['dcard']['db']['posts'],
            config['webcrawler']['dcard']['db']['comments'],
        ]))
        logger.info(f"Add dcard job: {config['webcrawler']['dcard']['forums']}")
    if config['webcrawler']['facebook']['enable']:
        chromeprocess_kwargs = config['webdriver']['chromeprocess']
        chromeprocess_kwargs.update(config['webcrawler']['facebook']['webdriver']['chromeprocess'])
        jobs.append(threading.Thread(target=facebook_crawler(
            chromeprocess_kwargs=chromeprocess_kwargs,
            do_page_get=config['webcrawler']['facebook']['do_page_get'],
            do_post_get=config['webcrawler']['facebook']['do_post_get'],
        ).start_thread, args=[
            config['webcrawler']['facebook']['pages'],
            config['webcrawler']['facebook']['page_get'],
            config['webcrawler']['facebook']['post_get'],
            config['webcrawler']['facebook']['db']['posts'],
            config['webcrawler']['facebook']['db']['comments'],
        ]))
        logger.info(f"Add facebook job: {config['webcrawler']['facebook']['pages']}")
    if config['webcrawler']['plurk']['enable']:
        jobs.append(threading.Thread(target=plurk_crawler(
            do_search_get=config['webcrawler']['plurk']['do_search_get'],
            do_post_get=config['webcrawler']['plurk']['do_post_get'],
        ).start_thread, args=[
            config['webcrawler']['plurk']['searches'],
            config['webcrawler']['plurk']['search_get'],
            config['webcrawler']['plurk']['post_get'],
            config['webcrawler']['plurk']['db']['posts'],
            config['webcrawler']['plurk']['db']['comments'],
        ]))
        logger.info(f"Add plurk job: {config['webcrawler']['plurk']['searches']}")
    if config['webcrawler']['ptt']['enable']:
        jobs.append(threading.Thread(target=ptt_crawler(
            do_forum_get=config['webcrawler']['ptt']['do_forum_get'],
            do_post_get=config['webcrawler']['ptt']['do_post_get'],
        ).start_thread, args=[
            config['webcrawler']['ptt']['forums'],
            config['webcrawler']['ptt']['forum_get'],
            config['webcrawler']['ptt']['post_get'],
            config['webcrawler']['ptt']['db']['posts'],
            config['webcrawler']['ptt']['db']['comments'],
        ]))
        logger.info(f"Add ptt job: {config['webcrawler']['ptt']['forums']}")
    try:
        [job.start() for job in jobs]
        [job.join() for job in jobs]
    finally:
        sys.exit()
