import atexit
import json
import logging
import random
import re
import subprocess
import threading
import time
import uuid
from collections import defaultdict

import requests
import websocket
from selenium import webdriver


class ChromeDriver(webdriver.Chrome):
    _logger = logging.getLogger('ChromeDriver')

    def __init__(
            self,
            chromedriver_path: str = 'chromedriver',
            user_data_dir: str = None,
            profile_directory: str = None,
            headless: bool = False,
            window_size: tuple = (1280, 720),
            remote_debugging_host: str = '127.0.0.1',
            remote_debugging_port: int = 9222,
            service_kwargs: dict = None,
            chrome_options: list = None,
            experimental_options: dict = None,
    ):
        self.window_size = window_size
        service_kwargs = service_kwargs or {},
        chrome_options = chrome_options or [],
        experimental_options = experimental_options or {},
        self.service = webdriver.ChromeService(executable_path=chromedriver_path, **service_kwargs)
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--disable-popup-blocking')
        self.options.add_argument(f'--window-size={self.window_size[0]},{self.window_size[1]}')
        if headless:
            self.options.add_argument('--headless')
        if remote_debugging_port:
            self.options.add_argument(f'--remote-debugging-port={remote_debugging_port}')
            self.options.add_argument(f'--remote-allow-origins=http://{remote_debugging_host}:{remote_debugging_port}')
        if user_data_dir:
            self.options.add_argument(f'--user-data-dir={user_data_dir}')
        if profile_directory:
            self.options.add_argument(f'--profile-directory={profile_directory}')
        for option in chrome_options:
            self.options.add_argument(option)
        for name, value in experimental_options.items():
            self.options.add_experimental_option(name, value)
        self._logger.debug(f'Chrome options: {" ".join(self.options.arguments)}')
        super().__init__(options=self.options, service=self.service)
        self._logger.debug(f'ChromeDriver process: {" ".join(self.service.process.args)}')
        if remote_debugging_port:
            self.cdp = CDP(remote_debugging_port=remote_debugging_port)
            self.cdp.send('Network.enable')
        atexit.register(self.stop)

    def scroll(self, x: int, y: int, x_distance: int = 0, y_distance: int = 0, speed: int = 800, count: int = 1, repeat_delay: float = 0.25):
        self.cdp.send('Input.synthesizeScrollGesture', x=x, y=y, xDistance=x_distance, yDistance=y_distance, speed=speed, repeatCount=count - 1, repeatDelayMs=int(repeat_delay * 1000))
        time.sleep(max(abs(x_distance), abs(y_distance)) / speed * count + repeat_delay * (count - 1))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def stop(self):
        if hasattr(self, 'cdp'):
            self.cdp.stop()
        if self.service.process.returncode is None:
            self.quit()


class ChromeProcess:
    _logger = logging.getLogger('ChromeProcess')

    def __init__(
            self,
            chrome_path: str = 'chrome',
            user_data_dir: str = None,
            profile_directory: str = None,
            incognito: bool = True,
            headless: bool = False,
            window_size: tuple = (1280, 720),
            remote_debugging_host: str = '127.0.0.1',
            remote_debugging_port: int = 9222,
            log_level: int = 3,
            chrome_options: list = None,
            use_exist: bool = False,
    ):
        self.use_exist = use_exist
        self.window_size = window_size
        chrome_options = chrome_options or []
        self.options = [chrome_path, '--disable-popup-blocking', f'--window-size={self.window_size[0]},{self.window_size[1]}']
        if incognito:
            self.options.append('--incognito')
        if headless:
            self.options.append('--headless')
        if remote_debugging_port:
            self.options.append(f'--remote-debugging-port={remote_debugging_port}')
            self.options.append(f'--remote-allow-origins=http://{remote_debugging_host}:{remote_debugging_port}')
        if user_data_dir:
            self.options.append(f'--user-data-dir={user_data_dir}')
        if profile_directory:
            self.options.append(f'--profile-directory={profile_directory}')
        if log_level:
            self.options.append(f'--log-level={log_level}')
        for option in chrome_options:
            self.options.append(option)
        if not self.use_exist:
            self.process = subprocess.Popen(args=self.options, start_new_session=True)
            self._logger.debug(f'Chrome process: {" ".join(self.process.args)}')
        if remote_debugging_port:
            self.cdp = CDP(remote_debugging_host=remote_debugging_host, remote_debugging_port=remote_debugging_port)
            self.cdp.send('Network.enable')
        atexit.register(self.stop)

    @property
    def page_source(self) -> str:
        return self.cdp.page_source

    def get(self, url: str, blocking: bool = False, timeout: float = 10, **params):
        self.cdp.get(url, blocking=blocking, timeout=timeout, **params)

    def scroll(self, x: int, y: int, x_distance: int = 0, y_distance: int = 0, speed: int = 800, count: int = 1, repeat_delay: float = 0.25, *, blocking: bool = True, **params):
        self.cdp.scroll(x, y, x_distance, y_distance, speed, count, repeat_delay, blocking=blocking, **params)

    def clear_cache(self):
        self.cdp.clear_cache()

    def clear_cookies(self):
        self.cdp.clear_cookies()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def stop(self):
        if hasattr(self, 'cdp'):
            self.cdp.stop()
        if not self.use_exist:
            if self.process.poll() is None:
                self.process.terminate()


# https://chromedevtools.github.io/devtools-protocol
class CDP:
    _logger = logging.getLogger('CDP')

    def __init__(self, remote_debugging_host: str = '127.0.0.1', remote_debugging_port: str = 9222, timeout: float = 10):
        self.remote_debugging_host = remote_debugging_host
        self.remote_debugging_port = remote_debugging_port
        end_time = time.time() + timeout
        with requests.Session() as session:
            while time.time() < end_time:
                try:
                    self.websocket_url = session.get(f'http://{self.remote_debugging_host}:{self.remote_debugging_port}/json').json()[0]['webSocketDebuggerUrl']
                    time.sleep(0.5)
                    break
                except:
                    continue
            else:
                raise TimeoutError(f'Failed to connect http://{self.remote_debugging_host}:{self.remote_debugging_port}/json')
        self._logger.debug(f'CDP websocket url: {self.websocket_url}')
        self.websocket = websocket.create_connection(self.websocket_url)
        self.received = list()
        self._used_id = set()
        self._listeners: dict = dict()
        self._running = True
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()
        atexit.register(self.stop)

    @property
    def page_source(self) -> str:
        try:
            nodeId = self.get_received_by_id(self.send('DOM.getDocument'))['result']['root']['nodeId']
            return self.get_received_by_id(self.send('DOM.getOuterHTML', nodeId=nodeId))['result']['outerHTML']
        except:
            return ""

    def get(self, url: str, blocking: bool = False, timeout: float = 10, **params):
        if blocking:

            def callback(r):
                self.check_loading_finished(r['params']['requestId'], blocking=True, timeout=timeout)
                finished_event.set()

            listener = self.add_listener(callback, f'<CDP.get: {url}>', cdp_method='Network.requestWillBeSent', url_exact=url)
            finished_event = threading.Event()
            self.send('Page.navigate', url=url, **params)
            finished_event.wait(timeout=timeout)
            self.remove_listener(listener)
        else:
            self.send('Page.navigate', url=url, **params)

    def scroll(self, x: int, y: int, x_distance: int = 0, y_distance: int = 0, speed: int = 800, count: int = 1, repeat_delay: float = 0.25, *, blocking: bool = True, **params):
        self.send('Input.synthesizeScrollGesture', x=x, y=y, xDistance=x_distance, yDistance=y_distance, speed=speed, repeatCount=count - 1, repeatDelayMs=int(repeat_delay * 1000), **params)
        if blocking: time.sleep(max(abs(x_distance), abs(y_distance)) / speed * count + repeat_delay * (count - 1))

    def send(self, method: str, **params):
        id = self._generate_cdp_request_id()
        payload = json.dumps({'id': id, 'method': method, 'params': params})
        self.websocket.send(payload)
        return id

    def clear_cache(self):
        self.send('Network.clearBrowserCache')

    def clear_cookies(self):
        self.send('Network.clearBrowserCookies')

    def get_received_by_id(self, id: int, timeout=10):
        start_idx = 0
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                r = self.received[start_idx]
                if r['id'] == id: return r
                else: start_idx += 1
            except:
                time.sleep(0.1)
                continue
        else:
            raise ValueError(f'{id = } not found')

    def check_loading_finished(self, request_id: str, blocking: bool = False, timeout: float = 10):
        if blocking:
            start_idx = 0
            end_time = time.time() + timeout
            while time.time() < end_time:
                try:
                    r = self.received[start_idx]
                    if r['method'] == 'Network.loadingFinished':
                        if r['params']['requestId'] == request_id:
                            return True
                    start_idx += 1
                except:
                    time.sleep(0.1)
                    continue
            else:
                return False
        else:
            for r in self.received:
                if r['method'] == 'Network.loadingFinished':
                    if r['params']['requestId'] == request_id:
                        return True
            else:
                return False

    def add_listener(self, callback, name: str = None, cdp_method: str = None, request_id: str = None, resource_type: str = None, url_exact: str = None, url_contain: str = None, url_regex: str = None, status_code: int = None):

        def listener(response: dict):
            check = []
            try:
                if cdp_method: check.append(response['method'] == cdp_method)
                if request_id: check.append(response['params']['requestId'] == request_id)
                if resource_type: check.append(response['params']['type'] == resource_type)
                if status_code: check.append(response['params']['response']['status'] == status_code)
                url = response['params']['response']['url'] if 'response' in response['params'] else response['params']['request']['url'] if 'request' in response['params'] else ''
                if url_exact: check.append(url == url_exact)
                if url_contain: check.append(url_contain in url)
                if url_regex: check.append(re.search(url_regex, url))
            except:
                return
            if all(check):
                self._logger.debug(f'Run listener callback: {name}')
                job = threading.Thread(target=callback, args=(response, ))
                self._listeners[name]['jobs'].append(job)
                job.start()

        name = name or str(uuid.uuid4())
        self._listeners[name] = {'fn': listener, 'jobs': []}
        self._logger.debug(f'Add network listener: {name}')
        return name

    def remove_listener(self, name: str, timeout: float = 10):
        if name in self._listeners:
            for job in self._listeners[name]['jobs']:
                job.join(timeout)
            self._listeners.pop(name)
            self._logger.debug(f'Remove network listener: {name}')
        else:
            self._logger.warning(f'Network listener not found: {name}')

    def _recv(self):
        while self._running:
            try:
                data = defaultdict(lambda: None)
                data.update(json.loads(self.websocket.recv()))
                self.received.append(data)
                for listener in self._listeners.values():
                    listener['fn'](data)
            except:
                if self._running:
                    continue
                else:
                    return

    def _generate_cdp_request_id(self):
        i = random.randint(0, 2**16 - 1)
        if i in self._used_id:
            return self._generate_cdp_request_id()
        else:
            self._used_id.add(i)
            return i

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def stop(self):
        self._running = False
        self.websocket.close()
        while len(self._listeners) > 0:
            for listener in tuple(self._listeners.keys()):
                self.remove_listener(listener)
