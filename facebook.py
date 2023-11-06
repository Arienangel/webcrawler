import asyncio
import inspect
import logging

import selenium.common.exceptions
import undetected_chromedriver as uc
import yaml
from selenium.webdriver.common.by import By

logger = logging.getLogger('Facebook')
with open('config.yaml', encoding='utf-8') as f:
    conf = yaml.load(f, yaml.SafeLoader)

options = uc.ChromeOptions()
options.add_argument('--disable-gpu')
options.add_argument(f'--user-agent={conf["webdriver"]["user_agent"]}')
driver = uc.Chrome(options=options, version_main=conf['webdriver']['version'], headless=conf['webdriver']['headless'])

logger.info(f'{__name__}: Start webdriver')

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
        logger.info(f'Get Facebook page: {page}')
        await asyncio.sleep(delay)
        try:
            driver.find_element(By.CSS_SELECTOR, 'div[role="dialog"]>div>div>i.x1b0d499').click_safe()  # 移除登入視窗
        except selenium.common.exceptions.NoSuchElementException:
            pass
        prevhigh = driver.execute_script("return document.body.scrollHeight;")
        try:
            while len(driver.find_elements(By.CSS_SELECTOR, 'div.xh8yej3>* a.x1heor9g.xt0b8zv.xo1l8bm')) < n:
                if driver.current_url.split('?', 1)[0] == 'https://www.facebook.com/login/':
                    raise Exception(f'Facebook page require login: {page}')
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # scroll to end
                logger.debug(f'Facebook page scrolling: {page}')
                await asyncio.sleep(delay)
                high = driver.execute_script("return document.body.scrollHeight;")
                if high == prevhigh: break
                else: prevhigh = driver.execute_script("return document.body.scrollHeight;")
            return [post.get_attribute('href').split('?')[0] for post in driver.find_elements(By.CSS_SELECTOR, 'div.xz9dl7a > * span.x1qrby5j > a.x1heor9g.xt0b8zv.xo1l8bm')][:n][::-1]
        except selenium.common.exceptions.NoSuchElementException:
            return []
    except Exception as E:
            logger.warning(f'{__name__}@{inspect.stack()[0][3]}(page={page}): {type(E).__name__}: {E}'.split('\n')[0])
            return []



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
        logger.info(f'Get Facebook post: {post_url}')
        driver.find_element(By.CSS_SELECTOR, 'div[role="dialog"]>div>div>i.x1b0d499').click_safe()  # 移除登入視窗
    except selenium.common.exceptions.NoSuchElementException:
        pass
    if driver.current_url.split('?', 1)[0] == 'https://www.facebook.com/login/':
        raise Exception(f'Facebook post require login: {post_url}')
    if '/events/' in driver.current_url or '/videos/' in driver.current_url or '/reel/' in driver.current_url:
        raise NotImplementedError
    base, _, id, _ = driver.find_element(By.CSS_SELECTOR, 'link[rel="canonical"]').get_attribute('href').rsplit('/', 3)
    url = f'{base}/{id}'
    encrypt_url = driver.find_element(By.CSS_SELECTOR, 'span.x1qrby5j>a').get_attribute('href').split('?')[0]
    page = driver.find_element(By.CSS_SELECTOR, 'a.x1s688f').text
    time = int(driver.find_element(By.CSS_SELECTOR, 'div.x6s0dn4.x78zum5>form>input[name="lgnjs"]').get_attribute('value'))
    content = driver.find_element(By.CSS_SELECTOR, 'span.xzsf02u.x1yc453h').text
    return {'page': page, 'time': time, 'content': content, 'post_url': url, 'encrypt_url': encrypt_url}
