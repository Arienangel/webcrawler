from webcrawler import webdriver


def test_chromedriver(request):
    browser_path = request.config.getoption("--chromedriver")
    with webdriver.ChromeDriver(browser_path) as browser:
        cdp = browser.cdp
        assert browser.service.process.returncode is None
        assert int(cdp.get_received_by_id(cdp.send('Runtime.evaluate', expression='1+2'))['result']['result']['value']) == 3


def test_chromeprocess(request):
    browser_path = request.config.getoption("--chrome")
    with webdriver.ChromeProcess(browser_path) as browser:
        cdp = browser.cdp
        assert browser.process.poll() is None
        assert int(cdp.get_received_by_id(cdp.send('Runtime.evaluate', expression='1+2'))['result']['result']['value']) == 3
