import queue
import sqlite3
import threading
import time

import requests
import yaml

from webcrawler import dcard, facebook, plurk, ptt
from webcrawler.webdriver import ChromeProcess


class dcard_crawler:

    def __init__(self, browser: ChromeProcess = None, chromeprocess_kwargs: dict = {}, do_forum_get: bool = True, do_post_get: bool = True):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or ChromeProcess(**chromeprocess_kwargs)
        self.do_forum_get = do_forum_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def __del__(self):
        if self._stop_browser_atexit:
            self.browser.stop()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | dcard.Forum], forum_get_kwargs: dict, post_get_kwargs: dict, posts_db_path: str, comments_db_path: str):
        self.stop = False
        threading.Thread(target=self.run_crawler, args=[forums, forum_get_kwargs, post_get_kwargs]).start()
        while (not self.stop) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
            if len(self.queue_posts.queue):
                self.write_posts_db(self.queue_posts.get(), posts_db_path)
            if len(self.queue_comments.queue):
                self.write_comments_db(self.queue_comments.get(), comments_db_path)
        return

    def run_crawler(self, forums: list[str | dcard.Forum], forum_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        try:
            for forum in forums:
                if isinstance(forum, str): forum = dcard.Forum(alias=forum)
                if self.do_forum_get:
                    forum.get(self.browser, **forum_get_kwargs)
                for post in forum.posts[::-1]:
                    if self.do_post_get:
                        try:
                            time.sleep(5)
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_comments.put(post.comments)
                        except:
                            continue
                    self.queue_posts.put([post])
        finally:
            self.stop = True

    def write_posts_db(self, posts: list[dcard.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.forum.alias}` ("id" INTEGAR UNIQUE, "created_time" INTEGAR, "author_school" TEXT, "author_department" TEXT, "title" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.forum.alias}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.forum.alias}` VALUES (?,?,?,?,?,?);', [post.id, int(post.created_time.timestamp()), post.author.school, post.author.department, post.title, post.content])
                except:
                    continue

    def write_comments_db(self, comments: list[dcard.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGAR UNIQUE, "created_time" INTEGAR, "author_school" TEXT, "author_department" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?);', [comment.floor, int(comment.created_time.timestamp()), comment.author.school, comment.author.department, comment.content])
                except:
                    continue


class facebook_crawler:

    def __init__(self, browser: ChromeProcess = None, chromeprocess_kwargs: dict = {}, do_page_get: bool = True, do_post_get: bool = True):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or ChromeProcess(**chromeprocess_kwargs)
        self.do_page_get = do_page_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def __del__(self):
        if self._stop_browser_atexit:
            self.browser.stop()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, pages: list[str | facebook.Page], page_get_kwargs: dict, post_get_kwargs: dict, posts_db_path: str, comments_db_path: str):
        self.stop = False
        threading.Thread(target=self.run_crawler, args=[pages, page_get_kwargs, post_get_kwargs]).start()
        while (not self.stop) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
            if len(self.queue_posts.queue):
                self.write_posts_db(self.queue_posts.get(), posts_db_path)
            if len(self.queue_comments.queue):
                self.write_comments_db(self.queue_comments.get(), comments_db_path)
        return

    def run_crawler(self, pages: list[str | facebook.Page], page_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        try:
            for page in pages:
                if isinstance(page, str): page = facebook.Page(alias=page)
                if self.do_page_get:
                    page.get(self.browser, **page_get_kwargs)
                for post in page.posts[::-1]:
                    if self.do_post_get:
                        try:
                            time.sleep(5)
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_comments.put(post.comments)
                        except:
                            continue
                    self.queue_posts.put([post])
        finally:
            self.stop = True

    def write_posts_db(self, posts: list[facebook.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.page.id}` ("id" INTEGAR UNIQUE, "pfbid" TEXT, "created_time" INTEGAR, "title" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.page.id}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.page.id}` VALUES (?,?,?,?,?);', [post.id, post.pfbid, int(post.created_time.timestamp()), post.title, post.content])
                except:
                    continue

    def write_comments_db(self, comments: list[facebook.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("id" INTEGAR UNIQUE, "created_time" INTEGAR, "author_id" INTEGAR, "author_alias" TEXT, "author_name" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{comment.post.id}` WHERE id=?;', [comment.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?,?);', [comment.id, int(comment.created_time.timestamp()), comment.author.id, comment.author.alias, comment.author.name, comment.content])
                except:
                    continue


class plurk_crawler:

    def __init__(self, browser: requests.Session = None, do_search_get: bool = True, do_post_get: bool = False):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or requests.Session()
        self.do_search_get = do_search_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def __del__(self):
        if self._stop_browser_atexit:
            self.browser.close()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | plurk.Search], search_get_kwargs: dict, post_get_kwargs: dict, posts_db_path: str, comments_db_path: str):
        self.stop = False
        threading.Thread(target=self.run_crawler, args=[forums, search_get_kwargs, post_get_kwargs]).start()
        while (not self.stop) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
            if len(self.queue_posts.queue):
                self.write_posts_db(self.queue_posts.get(), posts_db_path)
            if len(self.queue_comments.queue):
                self.write_comments_db(self.queue_comments.get(), comments_db_path)
        return

    def run_crawler(self, searches: list[str | plurk.Search], search_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        try:
            for search in searches:
                if isinstance(search, str): search = plurk.Search(query=search)
                if self.do_search_get:
                    search.get(self.browser, **search_get_kwargs)
                for post in search.posts[::-1]:
                    if self.do_post_get:
                        try:
                            time.sleep(16)
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_comments.put(post.comments)
                        except:
                            continue
                    self.queue_posts.put([post])
        finally:
            self.stop = True

    def write_posts_db(self, posts: list[plurk.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.query}` ("id" INTEGAR UNIQUE, "time" INTEGAR, "author_id" INTEGAR, "author_nickname" TEXT, "author_displayname" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.query}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.query}` VALUES (?,?,?,?,?,?);', [post.id, int(post.created_time.timestamp()), post.author.id, post.author.nickname, post.author.display_name, post.content_raw])
                except:
                    continue

    def write_comments_db(self, comments: list[plurk.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGAR UNIQUE, "id" INTEGAR, "time" INTEGAR, "author_id" INTEGAR, "author_nickname" TEXT, "author_displayname" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?,?,?);', [comment.floor, comment.id, int(comment.created_time.timestamp()), comment.author.id, comment.author.nickname, comment.author.display_name, comment.content])
                except:
                    continue


class ptt_crawler:

    def __init__(self, browser: requests.Session = None, do_forum_get: bool = True, do_post_get: bool = True):
        self._stop_browser_atexit = False if browser else True
        self.browser = browser or requests.Session()
        self.do_forum_get = do_forum_get
        self.do_post_get = do_post_get
        self.queue_posts = queue.Queue()
        self.queue_comments = queue.Queue()

    def __del__(self):
        if self._stop_browser_atexit:
            self.browser.close()
        self.queue_posts.shutdown()
        self.queue_comments.shutdown()

    def start_thread(self, forums: list[str | ptt.Forum], forum_get_kwargs: dict, post_get_kwargs: dict, posts_db_path: str, comments_db_path: str):
        self.stop = False
        threading.Thread(target=self.run_crawler, args=[forums, forum_get_kwargs, post_get_kwargs]).start()
        while (not self.stop) or len(self.queue_posts.queue) or len(self.queue_comments.queue):
            if len(self.queue_posts.queue):
                self.write_posts_db(self.queue_posts.get(), posts_db_path)
            if len(self.queue_comments.queue):
                self.write_comments_db(self.queue_comments.get(), comments_db_path)
        return

    def run_crawler(self, forums: list[str | ptt.Forum], forum_get_kwargs: dict = {}, post_get_kwargs: dict = {}):
        try:
            for forum in forums:
                if isinstance(forum, str): forum = ptt.Forum(name=forum)
                if self.do_forum_get:
                    forum.get(self.browser, **forum_get_kwargs)
                for post in forum.posts[::-1]:
                    if self.do_post_get:
                        try:
                            time.sleep(2)
                            post.get(self.browser, **post_get_kwargs)
                            self.queue_comments.put(post.comments)
                        except:
                            continue
                    self.queue_posts.put([post])
        finally:
            self.stop = True

    def write_posts_db(self, posts: list[ptt.Post], db_path: str):
        with sqlite3.connect(db_path) as db:
            for post in posts:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{post.forum.name}` ("id" TEXT UNIQUE, "time" INTEGAR, "author" TEXT, "title" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT id FROM `{post.forum.name}` WHERE id=?;', [post.id])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{post.forum.name}` VALUES (?,?,?,?,?);', [post.id, int(post.time.timestamp()), post.author, post.title, post.content])
                except:
                    continue

    def write_comments_db(self, comments: list[plurk.Comment], db_path: str):
        with sqlite3.connect(db_path) as db:
            for comment in comments:
                try:
                    db.execute(f'CREATE TABLE IF NOT EXISTS `{comment.post.id}` ("floor" INTEGAR UNIQUE, "time" INTEGAR, "reaction" TEXT, "author" TEXT, "content" TEXT);')
                    cursor = db.execute(f'SELECT floor FROM `{comment.post.id}` WHERE floor=?;', [comment.floor])
                    if not cursor.fetchall():
                        db.execute(f'INSERT INTO `{comment.post.id}` VALUES (?,?,?,?,?);', [comment.floor, int(comment.time.timestamp()), comment.reaction, comment.author, comment.content])
                except:
                    continue


if __name__ == '__main__':
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', type=str, default="config.yaml")
    args = parser.parse_args()
    with open(args.f, encoding='utf-8') as f:
        config = yaml.load(f, yaml.SafeLoader)
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
    try:
        [job.start() for job in jobs]
        [job.join() for job in jobs]
    finally:
        sys.exit()
