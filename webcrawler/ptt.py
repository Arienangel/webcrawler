import re
import time

import dateutil
import requests
from bs4 import BeautifulSoup


class Forum:

    def __init__(self, name: str):
        self.name: str = name
        self.posts: list[Post] = []

    def __repr__(self):
        return f'<PTT forum: {self.name}>'

    @property
    def url(self):
        return f'https://www.ptt.cc/bbs/{self.name}/index.html'

    def get(self, session: requests.Session, min_count: int = 10, timeout: float = 30, start_page: int = None):
        url = f'https://www.ptt.cc/bbs/{self.name}/index{start_page}.html' if start_page else f'https://www.ptt.cc/bbs/{self.name}/index.html'
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
                    post.content = None
                    reaction_count = p.select('div.nrec')[0].text
                    post.reaction_count = int(reaction_count) if reaction_count else 0
                    posts.append(post)
            self.posts.extend(posts[::-1])
            next_url = response.select('div#action-bar-container > div.action-bar > div.btn-group-paging')[0].find('a', string='‹ 上頁').get('href')
            if next_url:
                url = f'https://www.ptt.cc{next_url}'
                time.sleep(5)
            else:
                return


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

    def get(self, session: requests.Session, timeout: float = 10):
        session.headers.update({"Cookie": "over18=1"})
        response = BeautifulSoup(session.get(self.url, timeout=timeout).text, features="html.parser")
        header = response.select('div#main-content > div.article-metaline')
        self.author, self.nickname = header[0].select('span.article-meta-value')[0].text.split(' ', maxsplit=1)
        self.title = header[1].select('span.article-meta-value')[0].text
        self.time = dateutil.parser.parse(header[2].select('span.article-meta-value')[0].text)
        text = response.select('div#main-content')[0].text
        self.content = text.split('\n', maxsplit=1)[1].split('\n\n--\n※ 發信站: 批踢踢實業坊(ptt.cc)', maxsplit=1)[0]
        location = re.search(r'※ 發信站: 批踢踢實業坊\(ptt\.cc\), 來自: (.+) \((.+)\)\s※ 文章網址', text)
        if location:
            self.ip, self.country = location.groups()
        year, month = self.time.year, self.time.month
        for floor, c in enumerate(response.select('div#main-content > div.push'), 1):
            try:
                comment = Comment(self.forum, self, floor)
                comment.reaction = c.select('span.push-tag')[0].text.strip()
                comment.author = c.select('span.push-userid')[0].text
                comment.content = c.select('span.push-content')[0].text.lstrip(': ')
                time = c.select('span.push-ipdatetime')[0].text.strip()
                if int(time[:2]) < month: year += 1
                comment.time = dateutil.parser.parse(f'{year}/{time}')
                month = comment.time.year
                self.comments.append(comment)
            except:
                continue


class Comment:

    def __init__(self, forum: Forum, post: Post, floor: int):
        self.forum: Forum = forum
        self.post: Post = post
        self.floor: int = floor

    def __repr__(self):
        return f'<PTT comment: {self.forum.name}:{self.post.id}:b{self.floor}>'
