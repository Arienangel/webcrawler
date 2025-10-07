import datetime
import logging
import queue
import threading
import time

import base36
import dateutil
import requests


def convert_to_id(b36: str):
    return base36.loads(b36)


def convert_to_b36(id: int):
    return base36.dumps(int(id))


class Search:

    def __init__(self, query: str):
        self.query: str = query
        self.api_url: str = 'https://www.plurk.com/Search/search2'
        self.api_body: dict = {"query": query}
        self.posts: list[Post] = []
        self.users: dict[int, User] = {}
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Plurk search: {self.query}>'

    @property
    def url(self):
        return f'https://www.plurk.com/search?q={self.query}'

    def get(self, session: requests.Session, min_count: int = 30, time_until: datetime.datetime = None, timeout: float = 35):

        def load_page():
            while not stop:
                response = session.post(self.api_url, self.api_body, timeout=timeout)
                self._logger.info(f'Get: {self.api_url} {self.api_body}')
                if response.headers['Content-Type'] != 'application/json':
                    self._logger.warning(f'Not a json file: {self.api_url}')
                    time.sleep(16)
                    continue
                response_queue.put(response.json())
                time.sleep(16)
                try:
                    self.api_body.update({'after_id': next_id.get(timeout=timeout)})
                except queue.ShutDown:
                    return

        def read_received():
            nonlocal stop
            while not stop:
                if len(response_queue.queue):
                    response = response_queue.get()
                    if response['users']:
                        for u in response['users'].values():
                            try:
                                user = User(u['id'])
                                user.nickname = u['nick_name']
                                user.display_name = u['display_name']
                                user.avatar = u['avatar']
                                user.premium = u['premium']
                                if u['date_of_birth']:
                                    user.brithday = dateutil.parser.parse(u['date_of_birth'])
                                user.status = u['status']
                                user.name_color = u['name_color']
                                user.brithday_privacy = u['bday_privacy']
                                user.has_profile_image = u['has_profile_image']
                                user.timeline_privacy = u['timeline_privacy']
                                user.gender = u['gender']
                                user.karma = u['karma']
                                user.verified_account = u['verified_account']
                                user.dateformat = u['dateformat']
                                user.default_lang = u['default_lang']
                                user.friend_list_privacy = u['friend_list_privacy']
                                user.show_location = u['show_location']
                                user.full_name = u['full_name']
                                user.relationship = u['relationship']
                                user.location = u['location']
                                user.timezone = u['timezone']
                                user.email_confirmed = u['email_confirmed']
                                user.phone_verified = u['phone_verified']
                                user.pinned_plurk_id = u['pinned_plurk_id']
                                user.background_id = u['background_id']
                                user.recruited = u['recruited']
                                user.show_ads = u['show_ads']
                                self.users.update({user.id: user})
                            except:
                                continue
                    for p in response['plurks']:
                        if len(self.posts):
                            if p['id'] == self.posts[-1].id:
                                stop = True
                                return
                        try:
                            post = Post(p['id'])
                            post.query = self.query
                            post.author = self.users[p['user_id']]
                            post.qualifier = p['qualifier']
                            post.content = p['content']
                            post.content_raw = p['content_raw']
                            post.lang = p['lang']
                            post.response_count = p['response_count']
                            post.responses_seen = p['responses_seen']
                            post.limited_to = p['limited_to']
                            post.excluded = p['excluded']
                            post.no_comments = p['no_comments']
                            post.plurk_type = p['plurk_type']
                            post.is_unread = p['is_unread']
                            post.created_time = dateutil.parser.parse(p['posted'])
                            if p['last_edited']:
                                post.modified_time = dateutil.parser.parse(p['last_edited'])
                            post.coins = p['coins']
                            post.has_gift = p['has_gift']
                            post.porn = p['porn']
                            post.publish_to_followers = p['publish_to_followers']
                            post.with_poll = p['with_poll']
                            post.anonymous = p['anonymous']
                            post.replurkable = p['replurkable']
                            post.replurker_id = p['replurker_id']
                            post.replurked = p['replurked']
                            post.replurkers = [self.users[uid] if uid in self.users else User(uid) for uid in p['replurkers']]
                            post.replurkers_count = p['replurkers_count']
                            post.favorers = [self.users[uid] if uid in self.users else User(uid) for uid in p['favorers']]
                            post.favorite_count = p['favorite_count']
                            post.mentioned = p['mentioned']
                            post.responded = p['responded']
                            post.favorite = p['favorite']
                            self.posts.append(post)
                            self._logger.debug(f'Extract post: {post.__repr__()}')
                        except Exception as E:
                            self._logger.warning(f'Extract post failed: {type(E)}:{E.args()}: {p}')
                            continue
                    try:
                        next_id.put(self.posts[-1].id)
                    except queue.ShutDown:
                        return

        stop = False
        end_time = time.time() + timeout
        response_queue = queue.Queue()
        next_id = queue.Queue()
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.posts) >= min_count else False,
                        True if time_until is None else False if len(self.posts) == 0 else True if self.posts[-1].created_time <= time_until else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True
            next_id.shutdown(immediate=True)


class Post:

    def __init__(self, id: int = None, b36: str = None):
        if id: self.id: int = id
        else: self.id: int = convert_to_id(b36)
        self.api_url: str = 'https://www.plurk.com/Responses/get'
        self.api_body: dict = {"plurk_id": self.id, "from_response_id": 0}
        self.comments: list[Comment] = []
        self.users: dict[int, User] = {}
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Plurk post: {self.id} ({self.b36})>'

    @property
    def b36(self):
        return convert_to_b36(self.id)

    @property
    def url(self):
        return f'https://www.plurk.com/p/{self.b36}'

    def get(self, session: requests.Session, min_count: int = 30, timeout: float = 35):

        def load_page():
            while not stop:
                response = session.post(self.api_url, self.api_body, timeout=timeout)
                self._logger.info(f'Get: {self.api_url} {self.api_body}')
                if response.headers['Content-Type'] != 'application/json':
                    self._logger.warning(f'Not a json file: {self.api_url}')
                    time.sleep(16)
                    continue
                response_queue.put(response.json())
                time.sleep(16)
                try:
                    self.api_body.update({'from_response_id': next_id.get(timeout=timeout)})
                except queue.ShutDown:
                    return

        def read_received():
            nonlocal stop
            floor = 1
            while not stop:
                if len(response_queue.queue):
                    response = response_queue.get()
                    if response['users']:
                        for u in response['users'].values():
                            try:
                                user = User(u['id'])
                                user.nickname = u['nick_name']
                                user.display_name = u['display_name']
                                user.avatar = u['avatar']
                                user.premium = u['premium']
                                if u['date_of_birth']:
                                    user.brithday = dateutil.parser.parse(u['date_of_birth'])
                                user.status = u['status']
                                user.name_color = u['name_color']
                                user.brithday_privacy = u['bday_privacy']
                                user.has_profile_image = u['has_profile_image']
                                user.timeline_privacy = u['timeline_privacy']
                                user.gender = u['gender']
                                user.karma = u['karma']
                                user.verified_account = u['verified_account']
                                user.dateformat = u['dateformat']
                                user.default_lang = u['default_lang']
                                user.friend_list_privacy = u['friend_list_privacy']
                                user.show_location = u['show_location']
                                self.users.update({user.id: user})
                            except:
                                continue
                    for c in response['responses']:
                        if len(self.comments):
                            if c['id'] == self.comments[-1].id:
                                stop = True
                                return
                        try:
                            comment = Comment(self, c['id'], floor)
                            if len(response['users']) == 0:
                                if c['handle'] in self.users:
                                    comment.author = self.users[c['handle']]
                                else:
                                    comment.author = User(99999)
                                    comment.author.handle = c['handle']
                                    self.users.update({comment.author.handle: comment.author})
                            else:
                                comment.author = self.users[c['user_id']]
                            comment.content = c['content']
                            comment.content_raw = c['content_raw']
                            comment.qualifier = c['qualifier']
                            comment.created_time = dateutil.parser.parse(c['posted'])
                            if c['last_edited']:
                                comment.modified_time = dateutil.parser.parse(c['last_edited'])
                            comment.lang = c['lang']
                            comment.coins = c['coins']
                            comment.editability = c['editability']
                            self.comments.append(comment)
                            self._logger.debug(f'Extract comment: {comment.__repr__()}')
                        except Exception as E:
                            self._logger.warning(f'Extract comment failed: {type(E)}:{E.args()}: {c}')
                            continue
                        finally:
                            floor += 1
                    try:
                        if len(self.comments):
                            next_id.put(self.comments[-1].id)
                        else:
                            return
                    except queue.ShutDown:
                        return

        stop = False
        end_time = time.time() + timeout
        response_queue = queue.Queue()
        next_id = queue.Queue()
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.comments) >= min_count else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True
            next_id.shutdown(immediate=True)


class Comment:

    def __init__(self, post: Post, id: int, floor: int):
        self.post: Post = post
        self.id: int = id
        self.floor: int = floor
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Plurk comment: {self.post.id} ({self.post.b36}):b{self.floor}>'


class User:

    def __init__(self, id: int):
        self.id: int = id
