import asyncio
import inspect
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
options.add_argument('--disable-gpu')
options.add_argument(f'--user-agent={conf["webdriver"]["user_agent"]}') # https://github.com/ultrafunkamsterdam/undetected-chromedriver/issues/1577#issuecomment-1741737531
driver = uc.Chrome(options=options, version_main=conf['webdriver']['version'], headless=conf['webdriver']['headless'])

logger.info(f'{__name__}: Start webdriver')

async def challenge(delay: float = 3, retry: int = 10)->None:
    try:
        for _ in range(retry):
            driver.find_element(By.CSS_SELECTOR, '#challenge-stage').click_safe() 
            await asyncio.sleep(delay)
        else:
            if driver.find_element(By.CSS_SELECTOR, '#challenge-stage'):
                raise Exception(f'Dcard challenge failed (url={driver.current_url})')
            else: return
    except selenium.common.exceptions.NoSuchElementException:
        return


async def get_forum(forum: str, n: int = 30, delay: float = 3, retry: int = 10, **kwargs) -> list:
    """
    Get dcard posts in forum

    Args:
        forum (str): forum name
        n (int, optional): number of posts
        delay (float,optional): time to pause before webpage load 

    Returns:
        posts (list): list of post urls
    """
    url = f'https://www.dcard.tw/f/{forum}?tab=latest'
    driver.get(url)
    logger.info(f'Get Dcard forum: {forum}')
    await asyncio.sleep(delay)
    await challenge(delay, retry)
    posts=set()
    try:
        while len(posts) < n:
            _posts=set([post.get_attribute('href') for post in driver.find_elements(By.CSS_SELECTOR, 'a.atm_cs_1urozh')])
            if _posts:
                posts.update(_posts)
                await asyncio.sleep(delay)
                driver.execute_script(f"document.querySelectorAll('a.atm_cs_1urozh')[{len(_posts)-1}].scrollIntoView();")  # scroll to last element
                logger.debug(f'Dcard forum scrolling: {forum}')
            else: break
        return sorted(posts, key=lambda url: int(url.rsplit('/p/', maxsplit=1)[-1]))[-n:]
    except Exception as E:
        logger.warning(f'{__name__}@{inspect.stack()[0][3]}(forum={forum}): {type(E).__name__}: {E}'.split('\n')[0])
        return [list(posts)]


async def get_topic(topic: str, n: int = 30, delay: float = 3, retry: int = 10, **kwargs) -> list:
    """
    Get dcard posts in topic

    Args:
        topic (str): topic name
        n (int, optional): number of posts
        delay (float,optional): time to pause before webpage load 

    Returns:
        posts (list): list of post urls
    """
    url = f'https://www.dcard.tw/topics/{topic}?latest=true&forums=all'
    driver.get(url)
    logger.info(f'Get Dcard topic: {topic}')
    await asyncio.sleep(delay)
    await challenge(delay, retry)
    posts=set()
    try:
        while len(posts) < n:
            _posts=set([post.get_attribute('href') for post in driver.find_elements(By.CSS_SELECTOR, 'a.atm_cs_1urozh')])
            if _posts:
                posts.update(_posts)
                await asyncio.sleep(delay)
                driver.execute_script(f"document.querySelectorAll('a.atm_cs_1urozh')[{len(_posts)-1}].scrollIntoView();")  # scroll to last element
                logger.debug(f'Dcard topic scrolling: {topic}')
            else: break
        return sorted(posts, key=lambda url: int(url.rsplit('/p/', maxsplit=1)[-1]))[-n:]
    except Exception as E:
        logger.warning(f'{__name__}@{inspect.stack()[0][3]}(topic={topic}): {type(E).__name__}: {E}'.split('\n')[0])
        return list(posts)

async def get_post(post_url: str, delay: float = 3, retry: int = 10,  **kwargs) -> dict:
    """
    Get dcard post content

    Args:
        post_url (str): url of the post

    Returns:
        post (dict): forum name, author, title, time, content, post_url
    """
    driver.get(post_url)
    logger.info(f'Get Dcard post: {post_url}')
    await challenge(delay, retry)
    forum = post_url.rsplit('/', 3)[1]
    author = driver.find_element(By.CSS_SELECTOR, 'div.a12lr2bo').text
    title = driver.find_element(By.CSS_SELECTOR, 'article > div.atm_9s_1txwivl > div > h1').text
    time = int(dateutil.parser.parse(driver.find_element(By.CSS_SELECTOR, 'article > div.atm_c8_3rwk2t  > div.atm_7l_1w35wrm > time').get_attribute('datetime')).timestamp())
    content = driver.find_element(By.CSS_SELECTOR, 'article > div.atm_lo_c0ivcw').text
    return {'forum': forum, 'author': author, 'title': title, 'time': time, 'content': content, 'post_url': post_url}
