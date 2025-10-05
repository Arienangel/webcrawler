from webcrawler import facebook
from webcrawler.webdriver import ChromeProcess


def test_facebook_page(request):
    browser_path = request.config.getoption("--chrome")
    page = facebook.Page(alias='tapcpr')
    assert page.url == 'https://www.facebook.com/tapcpr'
    with ChromeProcess(browser_path) as browser:
        page.get_posts(browser, min_count=5, timeout=15)
    assert page.id == 100064903653573
    assert len(page.posts) >= 5


def test_facebook_post(request):
    browser_path = request.config.getoption("--chrome")
    page = facebook.Page(alias='tapcpr')
    post = facebook.Post(page, pfbid='pfbid02vBJGo3oHaRjw8RjFpizMMEUpyaPX1HhmhxSyHQpTebBkq2TAxCozMMAD2X8CYepMl')
    assert post.url == 'https://www.facebook.com/tapcpr/posts/pfbid02vBJGo3oHaRjw8RjFpizMMEUpyaPX1HhmhxSyHQpTebBkq2TAxCozMMAD2X8CYepMl'
    with ChromeProcess(browser_path) as browser:
        post.get_post(browser, 20, 15)
    assert post.id == 10158333393125965
    assert (post.text is not None) or (post.title is not None)
    assert post.creation_time is not None
    assert len(post.comments) >= 10
    comment = post.comments[0]
    assert comment.author is not None
    assert comment.text is not None
    assert comment.creation_time is not None

