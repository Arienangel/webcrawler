from webcrawler import dcard
from webcrawler.webdriver import ChromeProcess


def test_dcard_forum(request):
    browser_path = request.config.getoption("--chrome")
    forum = dcard.Forum(alias='trans')
    assert forum.url == 'https://www.dcard.tw/f/trans?tab=latest'
    with ChromeProcess(browser_path) as browser:
        forum.get(browser, min_count=20, timeout=20)
    assert len(forum.posts) >= 20


def test_dcard_post(request):
    browser_path = request.config.getoption("--chrome")
    forum = dcard.Forum(alias='trans')
    post = dcard.Post(forum, 242721628)
    assert post.url == 'https://www.dcard.tw/f/trans/p/242721628'
    with ChromeProcess(browser_path) as browser:
        post.get(browser, 20, 15)
    assert post.title is not None
    assert post.content is not None
    assert post.created_time is not None
    assert len(post.comments) >= 20
    comment = post.comments[0]
    assert comment.created_time is not None
