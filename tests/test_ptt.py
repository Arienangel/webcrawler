import requests

from webcrawler import ptt


def test_ptt_forum():
    forum = ptt.Forum('transgender')
    assert forum.url == 'https://www.ptt.cc/bbs/transgender/index.html'
    with requests.Session() as session:
        forum.get(session, min_count=30, timeout=20)
    assert len(forum.posts) >= 30


def test_ptt_post():
    forum = ptt.Forum('transgender')
    post = ptt.Post(forum, 'M.1323013579.A.C29')
    assert post.url == 'https://www.ptt.cc/bbs/transgender/M.1323013579.A.C29.html'
    with requests.Session() as session:
        post.get(session, timeout=10)
    assert post.author is not None
    assert post.title is not None
    assert post.time is not None
    assert len(post.comments) >= 150
    comment = post.comments[0]
    assert comment.reaction is not None
    assert comment.author is not None
    assert comment.content is not None
    assert comment.time is not None
