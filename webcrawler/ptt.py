import json
import re
import time

import dateutil
import requests
from bs4 import BeautifulSoup


class Forum:

    def __init__(self, name: str, page: int = None):
        self.name: str = name
        self.page: int = page
        self.posts: list[Post] = []

    def __repr__(self):
        return f'<PTT forum: {self.name}>'

    @property
    def url(self):
        if self.page: return f'https://www.ptt.cc/bbs/{self.name}/index{self.page}.html'
        else: return f'https://www.ptt.cc/bbs/{self.name}/index.html'

    def get_posts(self, session: requests.Session, min_count: int = 10, timeout: float = 5):
        url = self.url
        session.headers.update({"Cookie": "over18=1"})
        end_time = time.time() + timeout
        while (time.time() < end_time) and (len(self.posts) < min_count):
            response = BeautifulSoup(session.get(url, timeout=timeout).text, features="html.parser")
            posts = []
            for p in response.select('div.r-list-container > div'):
                if p.get('class')[0] == 'search-bar':
                    continue
                elif p.get('class')[0] == 'r-list-sep':
                    break
                elif p.get('class')[0] == 'r-ent':
                    post = Post(self, p.select('div.title a')[0].get('href').rstrip('.html').split('/')[-1])
                    post.author = p.select('div.author')[0].text
                    post.title = p.select('div.title a')[0].text
                    reaction_count = p.select('div.nrec')[0].text
                    post.reaction_count = int(reaction_count) if reaction_count else 0
                    posts.append(post)
            self.posts.extend(posts[::-1])
            next_url = response.select('div#action-bar-container > div.action-bar > div.btn-group-paging')[0].find('a', string='‹ 上頁').get('href')
            if next_url:
                url = f'https://www.ptt.cc{next_url}'
            else:
                return

    def export(self, attributes: list[str], post_attributes: list[str]):
        data = {a: getattr(self, a, None) for a in attributes}
        if 'posts' in attributes:
            data.update({'posts': {post.id: {a: getattr(post, a, None) for a in post_attributes} for post in self.posts}})
        return json.dumps(data, ensure_ascii=False)


class Post:

    def __init__(self, forum: Forum, id: str):
        self.forum: Forum = forum
        self.id: str = id
        self.comments: list[Post] = []

    def __repr__(self):
        return f'<PTT post: {self.forum.name}:{self.id}>'

    @property
    def url(self):
        return f'https://www.ptt.cc/bbs/{self.forum.name}/{self.id}.html'

    def get_post(self, session: requests.Session, timeout: float = 5):
        session.headers.update({"Cookie": "over18=1"})
        response = BeautifulSoup(session.get(self.url, timeout=timeout).text, features="html.parser")
        header = response.select('div#main-content > div.article-metaline')
        self.author, self.nickname = header[0].select('span.article-meta-value')[0].text.split(' ', maxsplit=1)
        self.title = header[1].select('span.article-meta-value')[0].text
        self.time = dateutil.parser.parse(header[2].select('span.article-meta-value')[0].text)
        text = response.select('div#main-content')[0].text
        self.content = text.split('\n', maxsplit=1)[1].split('\n\n--\n※ 發信站: 批踢踢實業坊(ptt.cc)', maxsplit=1)[0]
        location=re.search(r'※ 發信站: 批踢踢實業坊\(ptt\.cc\), 來自: (.+) \((.+)\)\s※ 文章網址', text)
        if location:
            self.ip, self.country = location.groups()
        year, month = self.time.year, self.time.month
        for floor, c in enumerate(response.select('div#main-content > div.push'), 1):
            comment = Comment(self.forum, self, floor)
            comment.reaction = c.select('span.push-tag')[0].text.strip()
            comment.author = c.select('span.push-userid')[0].text
            comment.content = c.select('span.push-content')[0].text.lstrip(': ')
            time = c.select('span.push-ipdatetime')[0].text.strip()
            if int(time[:2]) < month: year += 1
            comment.time = dateutil.parser.parse(f'{year}/{time}')
            month = comment.time.year
            self.comments.append(comment)

    def export(self, attributes: list[str], comment_attributes: list[str]):
        data = {a: getattr(self, a, None) for a in attributes}
        if 'comments' in attributes:
            data.update({'comments': {comment.id: {a: getattr(comment, a, None) for a in comment_attributes} for comment in self.comments}})
        return json.dumps(data, ensure_ascii=False)


class Comment:

    def __init__(self, forum: Forum, post: Post, floor: int):
        self.forum: Forum = forum
        self.post: Post = post
        self.floor: int = floor

    def __repr__(self):
        return f'<PTT comment: {self.forum.name}:{self.post.id}:b{self.floor}>'
