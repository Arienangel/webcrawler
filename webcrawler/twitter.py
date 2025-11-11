import datetime
import json
import logging
import random
import threading
import time

import dateutil
import pytz

from .webdriver import ChromeProcess


class User:

    def __init__(self, id: int = None, alias: str = '', name: str = ''):
        self.id: int = id
        self.alias: str = alias
        self.name: str = name
        self.tweets: list[Tweet] = []
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Twitter user: {self.alias if self.alias else self.id}>'

    @property
    def url(self):
        return f'https://x.com/{self.alias}'

    def get(self, browser: ChromeProcess, min_count: int = 5, time_until: datetime.datetime | str = None, timeout: float = 30, stop_event: threading.Event = None, do_navigate: bool = True):

        def load_page():
            browser.get(self.url)
            self._logger.info(f'Connect: {self.url}')
            time.sleep(5)
            while not stop_event.is_set():
                browser.scroll(
                    x=browser.window_size[0] // 2 + int(10 * (random.random() - 0.5)),
                    y=browser.window_size[1] // 2 + int(10 * (random.random() - 0.5)),
                    x_distance=0,
                    y_distance=-800 + int(50 * (random.random() - 0.5)),
                    speed=1200 + int(200 * (random.random() - 0.5)),
                    count=1,
                    repeat_delay=1 + random.random(),
                )

        def on_listener1(r: dict):
            time.sleep(1)
            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
            if response['data']['user']['result']['timeline']['timeline']['instructions'][0]['type'] == 'TimelineClearCache': del response['data']['user']['result']['timeline']['timeline']['instructions'][0]
            self._parse_userdata(response['data']['user']['result']['timeline']['timeline']['instructions'][0]['entry']['content']['itemContent']['tweet_results']['result']['core']['user_results']['result'])
            tweets = response['data']['user']['result']['timeline']['timeline']['instructions'][1]['entries']
            extract_tweets(tweets)

        def extract_tweets(tweets: list):
            for p in tweets:
                try:
                    if ('profile-conversation' in p['entryId']) or ('tweet' in p['entryId']):
                        if 'items' in p['content']: p = p['content']['items'][0]['item']['itemContent']['tweet_results']['result']
                        elif 'itemContent' in p['content']: p = p['content']['itemContent']['tweet_results']['result']
                    else:
                        continue
                    tweet = Tweet(self)
                    tweet._parse(p)
                    if 'retweeted_status_result' in p['legacy']:
                        if int(p['legacy']['retweeted_status_result']['result']['core']['user_results']['result']['rest_id']) == tweet.user.id:
                            source_user = tweet.user
                        else:
                            source_user = User()
                            source_user._parse_userdata(p['legacy']['retweeted_status_result']['result']['core']['user_results']['result'])
                        source_tweet = Tweet(source_user)
                        source_tweet._parse(p['legacy']['retweeted_status_result']['result'])
                        tweet.retweeted_from = source_tweet
                    else:
                        tweet.retweeted_from = None
                    if 'quoted_status_result' in p:
                        if int(p['quoted_status_result']['result']['core']['user_results']['result']['rest_id']) == tweet.user.id:
                            quoted_user = tweet.user
                        else:
                            quoted_user = User()
                            quoted_user._parse_userdata(p['quoted_status_result']['result']['core']['user_results']['result'])
                        quoted_tweet = Tweet(quoted_user)
                        quoted_tweet._parse(p['quoted_status_result']['result'])
                        tweet.quoted_from = quoted_tweet
                    else:
                        tweet.quoted_from = None
                    self.tweets.append(tweet)
                    self._logger.debug(f'Extract post: {tweet.__repr__()}')
                except Exception as E:
                    self._logger.warning(f'Extract post failed: {type(E)}:{E.args}: {p}')
                    continue

        if stop_event is None: stop_event = threading.Event()
        listener1 = browser.cdp.add_listener(on_listener1, name=f'Listener 1: {self.__repr__()}', cdp_method='Network.responseReceived', resource_type='XHR', url_regex=r'graphql/.+/UserTweets')
        end_time = time.time() + timeout
        if isinstance(time_until, str): time_until = dateutil.parser.parse(time_until)
        if do_navigate: threading.Thread(target=load_page).start()
        try:
            while time.time() < end_time:
                time.sleep(0.01)
                if all([
                        True if min_count is None else True if len(self.tweets) >= min_count else False,
                        True if time_until is None else False if len(self.tweets) == 0 else True if self.tweets[-1].created_time.timestamp() <= time_until.timestamp() else False,
                ]):
                    return
                if stop_event.is_set():
                    return
        finally:
            stop_event.set()
            browser.cdp.remove_listener(listener1)
            self._logger.debug(f'#Posts: {len(self.tweets)}')

    def _parse_userdata(self, user_data: dict):
        self.id = int(user_data['rest_id'])
        self.name = user_data['core']['name']
        self.alias = user_data['core']['screen_name']
        self.description = user_data['legacy']['description']
        self.created_time = dateutil.parser.parse(user_data['core']['created_at'])
        self.image_url = user_data['avatar']['image_url']
        self.profile_banner_url = user_data['legacy']['profile_banner_url'] if 'profile_banner_url' in user_data['legacy'] else ''
        self.favourites_count = user_data['legacy']['favourites_count']
        self.followers_count = user_data['legacy']['followers_count']
        self.friends_count = user_data['legacy']['friends_count']
        self.listed_count = user_data['legacy']['listed_count']
        self.media_count = user_data['legacy']['media_count']
        self.statuses_count = user_data['legacy']['statuses_count']
        self.link = user_data['legacy']['url'] if 'url' in user_data['legacy'] else ''
        self.location = user_data['location']['location'] if 'location' in user_data else ''
        self.professional = user_data['professional']['category'][0]['name'] if 'professional' in user_data else ''
        self.is_blue_verified = user_data['is_blue_verified']
        self.verified = user_data['verification']['verified']
        self.can_dm = user_data['dm_permissions']['can_dm'] if 'can_dm' in user_data['dm_permissions'] else None
        self.can_dm_on_xchat = user_data['dm_permissions']['can_dm_on_xchat'] if 'can_dm_on_xchat' in user_data['dm_permissions'] else None
        self.has_graduated_access = user_data['has_graduated_access'] if 'has_graduated_access' in user_data else None
        self.has_custom_timelines = user_data['legacy']['has_custom_timelines']
        self.possibly_sensitive = user_data['legacy']['possibly_sensitive']
        self.is_translator = user_data['legacy']['is_translator']
        self.translator_type = user_data['legacy']['translator_type']
        self.profile_interstitial_type = user_data['legacy']['profile_interstitial_type']
        self.want_retweets = user_data['legacy']['want_retweets'] if 'want_retweets' in user_data['legacy'] else None
        self.can_media_tag = user_data['media_permissions']['can_media_tag'] if 'can_media_tag' in user_data['media_permissions'] else None
        self.parody_commentary_fan_label = user_data['parody_commentary_fan_label']
        self.protected = user_data['privacy']['protected']
        self.following = user_data['relationship_perspectives']['following'] if 'relationship_perspectives' in user_data['relationship_perspectives'] else None


class Tweet:

    def __init__(self, user: User = None, id: int = None):
        self.user: User = user or User()
        self.id: int = id
        self.created_time: datetime.datetime = datetime.datetime.fromtimestamp(0, tz=pytz.UTC)
        self.content: str = ''
        self.comments: list[Tweet] = []
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Twitter tweet: {self.user.alias if self.user.alias else self.id}:{self.id}>'

    @property
    def url(self):
        return f'https://x.com/{self.user.alias}/status/{self.id}'

    def get(self, browser: ChromeProcess, min_count: int = 10, timeout: float = 10, stop_event: threading.Event = None, do_navigate: bool = True):

        def load_page():
            browser.get(self.url)
            self._logger.info(f'Connect: {self.url}')
            time.sleep(5)
            while not stop_event.is_set():
                browser.scroll(
                    x=browser.window_size[0] // 2 + int(10 * (random.random() - 0.5)),
                    y=browser.window_size[1] // 2 + int(10 * (random.random() - 0.5)),
                    x_distance=0,
                    y_distance=-800 + int(50 * (random.random() - 0.5)),
                    speed=1200 + int(200 * (random.random() - 0.5)),
                    count=1,
                    repeat_delay=0.5 + random.random(),
                )

        def on_listener1(r: dict):
            time.sleep(1)
            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
            self._parse(response['data']['tweetResult']['result'])

        def on_listener2(r: dict):
            time.sleep(1)
            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
            if response['data']['threaded_conversation_with_injections_v2']['instructions'][0]['type'] == 'TimelineClearCache': del response['data']['threaded_conversation_with_injections_v2']['instructions'][0]
            comments = response['data']['threaded_conversation_with_injections_v2']['instructions'][0]['entries']
            for c in comments:
                try:
                    if 'tweet' in c['entryId']:
                        if 'items' in c['content']: c = c['content']['items'][0]['item']['itemContent']['tweet_results']['result']
                        elif 'itemContent' in c['content']: c = c['content']['itemContent']['tweet_results']['result']
                        self._parse(c)
                    elif 'conversationthread' in c['entryId']:
                        if 'items' in c['content']: c = c['content']['items'][0]['item']['itemContent']['tweet_results']['result']
                        elif 'itemContent' in c['content']: c = c['content']['itemContent']['tweet_results']['result']
                        if int(c['core']['user_results']['result']['rest_id']) == self.user.id:
                            user = self.user
                        else:
                            user = User()
                            user._parse_userdata(c['core']['user_results']['result'])
                        comment = Tweet(user)
                        comment._parse(c)
                        self.comments.append(comment)
                        self._logger.debug(f'Extract comment: {comment.__repr__()}')
                    else:
                        continue
                except Exception as E:
                    self._logger.warning(f'Extract comment failed: {type(E)}:{E.args}: {c}')
                    continue

        if stop_event is None: stop_event = threading.Event()
        listener1 = browser.cdp.add_listener(on_listener1, name=f'Listener 1: {self.__repr__()}', cdp_method='Network.responseReceived', resource_type='XHR', url_regex=r'graphql/.+/TweetResultByRestId')
        listener2 = browser.cdp.add_listener(on_listener2, name=f'Listener 2: {self.__repr__()}', cdp_method='Network.responseReceived', resource_type='XHR', url_regex=r'graphql/.+/TweetDetail')
        end_time = time.time() + timeout
        if do_navigate: threading.Thread(target=load_page).start()
        try:
            while time.time() < end_time:
                time.sleep(0.01)
                if all([
                        True if min_count is None else True if len(self.comments) >= min_count else False,
                ]):
                    return
                if stop_event.is_set():
                    return
        finally:
            stop_event.set()
            browser.cdp.remove_listener(listener1)
            browser.cdp.remove_listener(listener2)
            self._logger.debug(f'#Comments: {len(self.comments)}')

    def _parse(self, tweet_data: dict):
        self.id = int(tweet_data['rest_id'])
        self.created_time = dateutil.parser.parse(tweet_data['legacy']['created_at'])
        self.content = tweet_data['legacy']['full_text']
        self.view_count = tweet_data['views']['count'] if 'count' in tweet_data['views'] else None
        self.bookmark_count = tweet_data['legacy']['bookmark_count']
        self.favorite_count = tweet_data['legacy']['favorite_count']
        self.quote_count = tweet_data['legacy']['quote_count']
        self.reply_count = tweet_data['legacy']['reply_count']
        self.retweet_count = tweet_data['legacy']['retweet_count']
        self.bookmarked = tweet_data['legacy']['bookmarked']
        self.favorited = tweet_data['legacy']['favorited']
        self.unmention_data = tweet_data['unmention_data']
        self.is_quote_status = tweet_data['legacy']['is_quote_status']
        self.is_translatable = tweet_data['is_translatable']
        self.grok_analysis_button = tweet_data['grok_analysis_button']
        self.quick_promote_eligibility = tweet_data['quick_promote_eligibility'] if 'quick_promote_eligibility' in tweet_data else None
        self.lang = tweet_data['legacy']['lang']
