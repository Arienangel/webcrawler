import asyncio
import logging

import dateutil
import selenium.common.exceptions
import undetected_chromedriver as uc
import yaml
from selenium.webdriver.common.by import By

logger = logging.getLogger('Dcard')
with open('config.yaml', encoding='utf-8') as f:
    conf = yaml.load(f, yaml.SafeLoader)

options = uc.ChromeOptions()
driver = uc.Chrome(options=options, version_main=conf['webdriver']['version'], headless=conf['webdriver']['headless'])


async def get_forum(forum: str, n: int = 30, delay: float = 3, **kwargs) -> list:
    """
    Get dcard posts in forum

    Args:
        forum (str): forum name
        n (int, optional): number of posts
        delay (float,optional): time to pause before webpage load 

    Returns:
        posts (list): list of post urls
    """
    try:
        url = f'https://www.dcard.tw/f/{forum}?tab=latest'
        driver.get(url)
        await asyncio.sleep(delay)
        try:
            for _ in range(10):
                driver.find_element(By.CSS_SELECTOR, '#challenge-stage').click_safe()  # 移除登入視窗
                await asyncio.sleep(delay)
            else:
                if driver.find_element(By.CSS_SELECTOR, '#challenge-stage'):
                    return []
        except selenium.common.exceptions.NoSuchElementException:
            pass
        prevhigh = driver.execute_script("return document.body.scrollHeight;")
        while len(driver.find_elements(By.CSS_SELECTOR, 'a.atm_cs_1urozh')) < n:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(delay)
            high = driver.execute_script("return document.body.scrollHeight;")  # scroll to end
            if high == prevhigh: break
            else: prevhigh = driver.execute_script("return document.body.scrollHeight;")
        return [post.get_attribute('href') for post in driver.find_elements(By.CSS_SELECTOR, 'a.atm_cs_1urozh')][:n][::-1]
    except selenium.common.exceptions.WebDriverException as E:
        raise E


async def get_post(post_url: str, **kwargs) -> dict:
    """
    Get dcard post content

    Args:
        post_url (str): url of the post

    Returns:
        post (dict): forum name, author, title, time, content, post_url
    """
    try:
        driver.get(post_url)
    except selenium.common.exceptions.WebDriverException as E:
        raise E
    forum = post_url.rsplit('/', 3)[1]
    author = driver.find_element(By.CSS_SELECTOR, 'div.a12lr2bo').text
    title = driver.find_element(By.CSS_SELECTOR, 'article > div.atm_9s_1txwivl > div > h1').text
    time = int(dateutil.parser.parse(driver.find_element(By.CSS_SELECTOR, 'article > div.atm_c8_3rwk2t  > div.atm_7l_1w35wrm > time').get_attribute('datetime')).timestamp())
    content = driver.find_element(By.CSS_SELECTOR, 'article > div.atm_lo_c0ivcw').text
    return {'forum': forum, 'author': author, 'title': title, 'time': time, 'content': content, 'post_url': post_url}
