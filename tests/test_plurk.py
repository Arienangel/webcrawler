import requests

from webcrawler import plurk


def test_plurk_search():
    search = plurk.Search('跨性')
    assert search.url == 'https://www.plurk.com/search?q=跨性'
    with requests.session() as session:
        search.get_posts(session, min_count=60, timeout=20)
    assert len(search.posts) >= 60


def test_plurk_post():
    post = plurk.Post(b36='3hmtkof49b')
    assert post.id == 354427127069519
    assert post.url == 'https://www.plurk.com/p/3hmtkof49b'
    with requests.session() as session:
        post.get_comments(session, min_count=300, timeout=20)
    assert len(post.comments) >= 300
    comment = post.comments[0]
    assert comment.user is not None
    assert comment.created_at is not None
    assert comment.content_raw is not None
