import logging
import queue
import random
import sqlite3
import threading
import time

import pytz
import requests
import yaml

from webcrawler import dcard, facebook, plurk, ptt
from webcrawler.webdriver import ChromeProcess


class dcard_crawler:
    _logger = logging.getLogger('Dcard crawler')

    def __init__(self, browser: ChromeProcess = None, chromeprocess_kwargs: dict = None, do_forum_get: bool = True, do_post_get: bool = True, notifier: Notifier = None, notify_tz=pytz.UTC):
        chromeprocess_kwargs = chromeprocess_kwargs or {}
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or ChromeProcess(**chromeprocess_kwargs)
        self.do_forum_get = do_forum_get
        self.do_post_get = do_post_get
        self.notifier = notifier
        self.notify_tz = notify_tz
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()
        self.used_posts = set()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.stop()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | dcard.Forum], forum_get_kwargs: dict = None, post_get_kwargs: dict = None, posts_db_path: str = '', comments_db_path: str = '', get_repeated_posts: bool = True):
        forum_get_kwargs = forum_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
        self._logger.info('Start dcard crawler')
        self.stop_thread = False
        if (not get_repeated_posts) and posts_db_path:
            with sqlite3.connect(posts_db_path) as db:
                tables = {row[0] for row in db.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()}
                for table in tables:
                    self.used_posts.update({row[0] for row in db.execute(f'SELECT id from `{table}`;').fetchall()})
        threading.Thread(target=self.run_crawler, args=[forums, forum_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if len(self.queue_posts.queue):
                    posts = self.queue_posts.get()
                    if posts_db_path: self.write_posts_db(posts, posts_db_path)
                    if self.notifier: self.notify(posts, tz=self.notify_tz)
                if len(self.queue_comments.queue):
                    comments = self.queue_comments.get()
                    if comments_db_path: self.write_comments_db(comments, comments_db_path)
        finally:
            self.exit()
            self._logger.info('Finish dcard crawler')

    def run_crawler(self, forums: list[str | dcard.Forum], forum_get_kwargs: dict = None, post_get_kwargs: dict = None):
        forum_get_kwargs = forum_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
        try:
            self.browser.get('https://www.dcard.tw/f', referrer='https://www.google.com/', blocking=True, timeout=10)
            time.sleep(10)
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
                        if post.id in self.used_posts:
                            continue
                        else:
                            self.used_posts.add(post.id)
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

    def notify(self, posts: list[dcard.Post], tz=pytz.UTC):
        for post in posts:
            self.notifier.send(tag=['default', 'dcard', f'dcard/{post.forum.alias}'], title=f'[Dcard] {post.forum.name or post.forum.alias}', body=f'{post.author.school} {post.author.department}\n---\n{post.title}\n{post.content}\n---\n{post.created_time.astimezone(tz).strftime("%Y/%m/%d %H:%M:%S %:z")}\n{post.url}')

    def write_posts_db(self, posts: list[dcard.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.forum.alias}` ("id" INTEGER UNIQUE, "created_time" INTEGER, "author_school" TEXT, "author_department" TEXT, "title" TEXT, "content" TEXT);')
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
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGER UNIQUE, "created_time" INTEGER, "author_school" TEXT, "author_department" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?);', [comment.floor, int(comment.created_time.timestamp()), comment.author.school, comment.author.department, comment.content])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class facebook_crawler:
    _logger = logging.getLogger('Facebook crawler')

    def __init__(self, browser: ChromeProcess = None, chromeprocess_kwargs: dict = None, do_page_get: bool = True, do_post_get: bool = True, notifier: Notifier = None, notify_tz=pytz.UTC):
        chromeprocess_kwargs = chromeprocess_kwargs or {}
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or ChromeProcess(**chromeprocess_kwargs)
        self.do_page_get = do_page_get
        self.do_post_get = do_post_get
        self.notifier = notifier
        self.notify_tz = notify_tz
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()
        self.used_posts = set()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.stop()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()
        self.used_posts = set()

    def start_thread(self, pages: list[str | facebook.Page], page_get_kwargs: dict = None, post_get_kwargs: dict = None, posts_db_path: str = '', comments_db_path: str = '', get_repeated_posts: bool = True):
        page_get_kwargs = page_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
        self._logger.info('Start facebook crawler')
        self.stop_thread = False
        if (not get_repeated_posts) and posts_db_path:
            with sqlite3.connect(posts_db_path) as db:
                tables = {row[0] for row in db.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()}
                for table in tables:
                    self.used_posts.update({row[0] for row in db.execute(f'SELECT id from `{table}`;').fetchall()})
        threading.Thread(target=self.run_crawler, args=[pages, page_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if len(self.queue_posts.queue):
                    posts = self.queue_posts.get()
                    if posts_db_path: self.write_posts_db(posts, posts_db_path)
                    if self.notifier: self.notify(posts, tz=self.notify_tz)
                if len(self.queue_comments.queue):
                    comments = self.queue_comments.get()
                    if comments_db_path: self.write_comments_db(comments, comments_db_path)
        finally:
            self.exit()
            self._logger.info('Finish facebook crawler')

    def run_crawler(self, pages: list[str | facebook.Page], page_get_kwargs: dict = None, post_get_kwargs: dict = None):
        page_get_kwargs = page_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
        try:
            self.browser.get('https://www.facebook.com/', referrer='https://www.google.com/', blocking=True, timeout=10)
            time.sleep(10)
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
                        if post.id in self.used_posts:
                            continue
                        else:
                            self.used_posts.add(post.id)
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

    def notify(self, posts: list[facebook.Post], tz=pytz.UTC):
        for post in posts:
            self.notifier.send(tag=['default', 'facebook', f'facebook/{post.page.id}', f'facebook/{post.page.alias}'], title=f'[Facebook] {post.page.name or post.page.alias}', body=f'{post.title}\n{post.content}\n---\n{post.created_time.astimezone(tz).strftime("%Y/%m/%d %H:%M:%S %:z")}\n{post.url}')

    def write_posts_db(self, posts: list[facebook.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.page.id}` ("id" INTEGER UNIQUE, "pfbid" TEXT, "created_time" INTEGER, "title" TEXT, "content" TEXT);')
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
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("id" INTEGER UNIQUE, "created_time" INTEGER, "author_id" INTEGER, "author_alias" TEXT, "author_name" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{comment.post.id}` WHERE id=?;', [comment.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?,?);', [comment.id, int(comment.created_time.timestamp()), comment.author.id, comment.author.alias, comment.author.name, comment.content])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class plurk_crawler:
    _logger = logging.getLogger('Plurk crawler')

    def __init__(self, browser: requests.Session = None, do_search_get: bool = True, do_post_get: bool = False, notifier: Notifier = None, notify_tz=pytz.UTC):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or requests.Session()
        self.do_search_get = do_search_get
        self.do_post_get = do_post_get
        self.notifier = notifier
        self.notify_tz = notify_tz
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()
        self.used_posts = set()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.close()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, searches: list[str | plurk.Search], search_get_kwargs: dict = None, post_get_kwargs: dict = None, posts_db_path: str = '', comments_db_path: str = '', get_repeated_posts: bool = True):
        search_get_kwargs = search_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
        self.stop_thread = False
        self._logger.info('Start plurk crawler')
        if (not get_repeated_posts) and posts_db_path:
            with sqlite3.connect(posts_db_path) as db:
                tables = {row[0] for row in db.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()}
                for table in tables:
                    self.used_posts.update({row[0] for row in db.execute(f'SELECT id from `{table}`;').fetchall()})
        threading.Thread(target=self.run_crawler, args=[searches, search_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if len(self.queue_posts.queue):
                    posts = self.queue_posts.get()
                    if posts_db_path: self.write_posts_db(posts, posts_db_path)
                    if self.notifier: self.notify(posts, tz=self.notify_tz)
                if len(self.queue_comments.queue):
                    comments = self.queue_comments.get()
                    if comments_db_path: self.write_comments_db(comments, comments_db_path)
        finally:
            self.exit()
            self._logger.info('Finish plurk crawler')

    def run_crawler(self, searches: list[str | plurk.Search], search_get_kwargs: dict = None, post_get_kwargs: dict = None):
        search_get_kwargs = search_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
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
                        if post.id in self.used_posts:
                            continue
                        else:
                            self.used_posts.add(post.id)
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

    def notify(self, posts: list[plurk.Post], tz=pytz.UTC):
        for post in posts:
            self.notifier.send(tag=['default', 'plurk', f'plurk/{post.query}'], title=f'[Plurk] {post.query}', body=f'{post.author.display_name}\n---\n{post.content_raw}\n---\n{post.created_time.astimezone(tz).strftime("%Y/%m/%d %H:%M:%S %:z")}\n{post.url}')

    def write_posts_db(self, posts: list[plurk.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.query}` ("id" INTEGER UNIQUE, "time" INTEGER, "author_id" INTEGER, "author_nickname" TEXT, "author_displayname" TEXT, "content" TEXT);')
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
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGER UNIQUE, "id" INTEGER, "time" INTEGER, "author_id" INTEGER, "author_nickname" TEXT, "author_displayname" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?,?,?);', [comment.floor, comment.id, int(comment.created_time.timestamp()), comment.author.id, comment.author.nickname, comment.author.display_name, comment.content_raw])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class ptt_crawler:
    _logger = logging.getLogger('PTT crawler')

    def __init__(self, browser: requests.Session = None, do_forum_get: bool = True, do_post_get: bool = True, notifier: Notifier = None, notify_tz=pytz.UTC):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or requests.Session()
        self.do_forum_get = do_forum_get
        self.do_post_get = do_post_get
        self.notifier = notifier
        self.notify_tz = notify_tz
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()
        self.used_posts = set()

    def exit(self):
        if self._stop_browser_atexit:
            self.browser.close()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | ptt.Forum], forum_get_kwargs: dict = None, post_get_kwargs: dict = None, posts_db_path: str = '', comments_db_path: str = '', get_repeated_posts: bool = True):
        forum_get_kwargs = forum_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
        self.stop_thread = False
        self._logger.info('Start ptt crawler')
        if (not get_repeated_posts) and posts_db_path:
            with sqlite3.connect(posts_db_path) as db:
                tables = {row[0] for row in db.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()}
                for table in tables:
                    self.used_posts.update({row[0] for row in db.execute(f'SELECT id from `{table}`;').fetchall()})
        threading.Thread(target=self.run_crawler, args=[forums, forum_get_kwargs, post_get_kwargs]).start()
        try:
            while (not self.stop_thread) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
                time.sleep(0.01)
                if len(self.queue_posts.queue):
                    posts = self.queue_posts.get()
                    if posts_db_path: self.write_posts_db(posts, posts_db_path)
                    if self.notifier: self.notify(posts, tz=self.notify_tz)
                if len(self.queue_comments.queue):
                    comments = self.queue_comments.get()
                    if comments_db_path: self.write_comments_db(comments, comments_db_path)
        finally:
            self.exit()
            self._logger.info('Finish ptt crawler')

    def run_crawler(self, forums: list[str | ptt.Forum], forum_get_kwargs: dict = None, post_get_kwargs: dict = None):
        forum_get_kwargs = forum_get_kwargs or {}
        post_get_kwargs = post_get_kwargs or {}
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
                        if post.id in self.used_posts:
                            continue
                        else:
                            self.used_posts.add(post.id)
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

    def notify(self, posts: list[ptt.Post], tz=pytz.UTC):
        for post in posts:
            self.notifier.send(tag=['default', 'ptt', f'ptt/{post.forum.name}'], title=f'[PTT] {post.forum.name}', body=f'{post.author.id} ({post.author.name})\n---\n{post.title}\n{post.content}\n---\n{post.time.astimezone(tz).strftime("%Y/%m/%d %H:%M:%S %:z")}\n{post.url}')

    def write_posts_db(self, posts: list[ptt.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.forum.name}` ("id" TEXT UNIQUE, "time" INTEGER, "author_id" TEXT, "author_name" TEXT, "title" TEXT, "content" TEXT);')
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
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGER UNIQUE, "time" INTEGER, "reaction" TEXT, "author_id" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?);', [comment.floor, int(comment.time.timestamp()), comment.reaction, comment.author.id, comment.content])
                        self._logger.debug(f'Write db: {comment.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Write db failed: {type(E)}:{E.args}: {comment.__repr__()}')
                    continue


class Notifier:
    _logger = logging.getLogger('Notifier')

    def __init__(self, config_file: str):
        import apprise
        self.apobj = apprise.Apprise()
        self.config = apprise.AppriseConfig()
        self.config.add(config_file)
        self.apobj.add(self.config)
        self._logger.info(f"Apprise config file: {config_file}")

    def send(self, **kwargs):
        try:
            self.apobj.notify(**kwargs)
        except Exception as E:
            self._logger.warning(f'Send notify failed: {type(E)}:{E.args}: {kwargs}')


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
    if config['notify']['enable']:
        notifier = Notifier(config['notify']['config'])
        notify_tz = pytz.timezone(config['notify']['timezone'])
    jobs = []
    if config['webcrawler']['dcard']['enable']:
        try:
            forums = config['webcrawler']['dcard']['forums']
            random.shuffle(forums)
            chromeprocess_kwargs = config['webdriver']['chromeprocess'].copy()
            chromeprocess_kwargs.update(config['webcrawler']['dcard']['webdriver']['chromeprocess'])
            jobs.append(threading.Thread(target=dcard_crawler(
                chromeprocess_kwargs=chromeprocess_kwargs,
                do_forum_get=config['webcrawler']['dcard']['do_forum_get'],
                do_post_get=config['webcrawler']['dcard']['do_post_get'],
                notifier=notifier if (config['notify']['enable'] and config['webcrawler']['dcard']['notify']['enable']) else None,
                notify_tz=notify_tz,
            ).start_thread, args=[
                forums,
                config['webcrawler']['dcard']['forum_get'],
                config['webcrawler']['dcard']['post_get'],
                config['webcrawler']['dcard']['db']['posts'],
                config['webcrawler']['dcard']['db']['comments'],
                config['webcrawler']['dcard']['get_repeat_posts'],
            ]))
            logger.info(f"Add dcard job: {forums}")
        except Exception as E:
            logger.warning(f'Add dcard job failed: {type(E)}:{E.args}')
    if config['webcrawler']['facebook']['enable']:
        try:
            pages = config['webcrawler']['facebook']['pages']
            random.shuffle(pages)
            chromeprocess_kwargs = config['webdriver']['chromeprocess'].copy()
            chromeprocess_kwargs.update(config['webcrawler']['facebook']['webdriver']['chromeprocess'])
            jobs.append(threading.Thread(target=facebook_crawler(
                chromeprocess_kwargs=chromeprocess_kwargs,
                do_page_get=config['webcrawler']['facebook']['do_page_get'],
                do_post_get=config['webcrawler']['facebook']['do_post_get'],
                notifier=notifier if (config['notify']['enable'] and config['webcrawler']['facebook']['notify']['enable']) else None,
                notify_tz=notify_tz,
            ).start_thread, args=[
                pages,
                config['webcrawler']['facebook']['page_get'],
                config['webcrawler']['facebook']['post_get'],
                config['webcrawler']['facebook']['db']['posts'],
                config['webcrawler']['facebook']['db']['comments'],
                config['webcrawler']['facebook']['get_repeat_posts'],
            ]))
            logger.info(f"Add facebook job: {pages}")
        except Exception as E:
            logger.warning(f'Add facebook job failed: {type(E)}:{E.args}')
    if config['webcrawler']['plurk']['enable']:
        try:
            searches = config['webcrawler']['plurk']['searches']
            random.shuffle(searches)
            jobs.append(threading.Thread(target=plurk_crawler(
                do_search_get=config['webcrawler']['plurk']['do_search_get'],
                do_post_get=config['webcrawler']['plurk']['do_post_get'],
                notifier=notifier if (config['notify']['enable'] and config['webcrawler']['plurk']['notify']['enable']) else None,
                notify_tz=notify_tz,
            ).start_thread, args=[
                searches,
                config['webcrawler']['plurk']['search_get'],
                config['webcrawler']['plurk']['post_get'],
                config['webcrawler']['plurk']['db']['posts'],
                config['webcrawler']['plurk']['db']['comments'],
                config['webcrawler']['plurk']['get_repeat_posts'],
            ]))
            logger.info(f"Add plurk job: {searches}")
        except Exception as E:
            logger.warning(f'Add plurk job failed: {type(E)}:{E.args}')
    if config['webcrawler']['ptt']['enable']:
        try:
            forums = config['webcrawler']['ptt']['forums']
            random.shuffle(forums)
            jobs.append(threading.Thread(target=ptt_crawler(
                do_forum_get=config['webcrawler']['ptt']['do_forum_get'],
                do_post_get=config['webcrawler']['ptt']['do_post_get'],
                notifier=notifier if (config['notify']['enable'] and config['webcrawler']['ptt']['notify']['enable']) else None,
                notify_tz=notify_tz,
            ).start_thread, args=[
                forums,
                config['webcrawler']['ptt']['forum_get'],
                config['webcrawler']['ptt']['post_get'],
                config['webcrawler']['ptt']['db']['posts'],
                config['webcrawler']['ptt']['db']['comments'],
                config['webcrawler']['ptt']['get_repeat_posts'],
            ]))
            logger.info(f"Add ptt job: {forums}")
        except Exception as E:
            logger.warning(f'Add ptt job failed: {type(E)}:{E.args}')
    try:
        [job.start() for job in jobs]
        [job.join() for job in jobs]
    finally:
        sys.exit()
