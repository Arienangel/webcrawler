import asyncio
import logging

import selenium.common.exceptions
import undetected_chromedriver as uc
import yaml
from selenium.webdriver.common.by import By

logger = logging.getLogger('Facebook')
with open('config.yaml', encoding='utf-8') as f:
    conf = yaml.load(f, yaml.SafeLoader)

options = uc.ChromeOptions()
driver = uc.Chrome(options=options, version_main=conf['webdriver']['version'], headless=conf['webdriver']['headless'])


async def get_page(page: str, n: int = 30, delay: float = 3, **kwargs) -> list:
    """
    Get facebook posts in page

    Args:
        page (str): page name
        n (int, optional): number of posts
        delay (float,optional): time to pause before webpage load 

    Returns:
        posts (list): list of post urls
    """
    try:
        url = f'https://www.facebook.com/{page}'
        driver.get(url)
        await asyncio.sleep(delay)
        try:
            driver.find_element(By.CSS_SELECTOR, 'div[role="dialog"]>div>div>i.x1b0d499').click_safe()  # 移除登入視窗
        except selenium.common.exceptions.NoSuchElementException:
            pass
        prevhigh = driver.execute_script("return document.body.scrollHeight;")
        while len(driver.find_elements(By.CSS_SELECTOR, 'div.xh8yej3>* a.x1heor9g.xt0b8zv.xo1l8bm')) < n:
            if driver.current_url.split('?', 1)[0] == 'https://www.facebook.com/login/':
                return []
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(delay)
            high = driver.execute_script("return document.body.scrollHeight;")  # scroll to end
            if high == prevhigh: break
            else: prevhigh = driver.execute_script("return document.body.scrollHeight;")
        return [post.get_attribute('href').split('?')[0] for post in driver.find_elements(By.CSS_SELECTOR, 'div.xh8yej3>* a.x1heor9g.xt0b8zv.xo1l8bm')][:n][::-1]
    except selenium.common.exceptions.WebDriverException as E:
        raise E


async def get_post(post_url: str, **kwargs) -> dict:
    """
    Get facebook post content

    Args:
        post_url (str): url of the post

    Returns:
        post (dict): page name, time, content, post_url, encrypt_url
    """
    try:
        driver.get(post_url)
        driver.find_element(By.CSS_SELECTOR, 'div[role="dialog"]>div>div>i.x1b0d499').click_safe()  # 移除登入視窗
    except selenium.common.exceptions.NoSuchElementException:
        pass
    if driver.current_url.split('?', 1)[0] == 'https://www.facebook.com/login/':
        raise selenium.common.exceptions.WebDriverException
    base, _, id, _ = driver.find_element(By.CSS_SELECTOR, 'link[rel="canonical"]').get_attribute('href').rsplit('/', 3)
    url = f'{base}/{id}'
    encrypt_url = driver.find_element(By.CSS_SELECTOR, 'span.x1qrby5j>a').get_attribute('href')
    page = driver.find_element(By.CSS_SELECTOR, 'a.x1i10hfl>strong>span').text
    time = int(driver.find_element(By.CSS_SELECTOR, 'div.x6s0dn4.x78zum5>form>input[name="lgnjs"]').get_attribute('value'))
    content = driver.find_element(By.CSS_SELECTOR, 'span.xzsf02u.x1yc453h').text
    return {'page': page, 'time': time, 'content': content, 'post_url': url, 'encrypt_url': encrypt_url}
