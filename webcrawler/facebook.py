import datetime
import json
import logging
import random
import re
import threading
import time

from bs4 import BeautifulSoup

from .webdriver import ChromeProcess


class Page:

    def __init__(self, id: int = None, alias: str = None):
        self.id: int = id if id else None
        self.alias: str = alias if alias else None
        self.name = None
        self.posts: list[Post] = []
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Facebook page: {self.alias if self.alias else self.id}>'

    @property
    def url(self):
        return f'https://www.facebook.com/{self.id if self.id else self.alias}'

    def get(self, browser: ChromeProcess, min_count: int = 5, time_until: datetime.datetime = None, timeout: float = 30):

        def load_page():
            browser.get(self.url)
            self._logger.info(f'Get: {self.url}')
            time.sleep(5)
            while not stop:
                browser.cdp.send('Runtime.evaluate', expression='''document.querySelector('div[role="dialog"] i.x1b0d499.x1d69dk1').click()''')
                browser.scroll(
                    x=browser.window_size[0] // 2 + int(10 * (random.random() - 0.5)),
                    y=browser.window_size[1] // 2 + int(10 * (random.random() - 0.5)),
                    x_distance=0,
                    y_distance=-800 + int(50 * (random.random() - 0.5)),
                    speed=1200 + int(200 * (random.random() - 0.5)),
                    count=1,
                    repeat_delay=1 + random.random(),
                )

        def read_received():
            while not stop:
                posts = []
                if len(listener1.queue):
                    r = listener1.get()
                    time.sleep(2)
                    response = browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body']
                    posts = [json.loads(BeautifulSoup(response, features='html.parser').find('script', type='application/json', string=re.compile(r'"post_id"')).text)['require'][0][3][0]['__bbox']['require'][-7][3][1]['__bbox']['result']['data']['user']['timeline_list_feed_units']['edges'][0]['node']]
                    self.id = int(posts[0]['comet_sections']['content']['story']['actors'][0]['id'])
                    self.alias = posts[0]['comet_sections']['content']['story']['actors'][0]['url'].split('/')[-1]
                    self.name = posts[0]['comet_sections']['content']['story']['actors'][0]['name']
                elif len(listener2.queue):
                    r = listener2.get()
                    time.sleep(1)
                    response = browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body']
                    L = response.split('\n')
                    posts = []
                    posts.append(json.loads(L[0])['data']['node']['timeline_list_feed_units']['edges'][0]['node'])
                    posts.extend([json.loads(i)['data']['node'] for i in L[1:-1]])
                for p in posts:
                    try:
                        post = Post(self, id=int(p['comet_sections']['content']['story']['post_id']), pfbid=p['comet_sections']['content']['story']['wwwURL'].split('/posts/')[1])
                        post.content = p['comet_sections']['content']['story']['message']['text'] if p['comet_sections']['content']['story']['message'] else None
                        post.title = p['comet_sections']['context_layout']['story']['comet_sections']['title']['story']['title']['text'] if p['comet_sections']['context_layout']['story']['comet_sections']['title']['story']['title'] else None
                        post.created_time = datetime.datetime.fromtimestamp(p['comet_sections']['timestamp']['story']['creation_time'])
                        post.reaction_count = p['comet_sections']['feedback']['story']['story_ufi_container']['story']['feedback_context']['feedback_target_with_context']['comet_ufi_summary_and_actions_renderer']['feedback']['reaction_count']['count']
                        if 'attachments' in p['comet_sections']['content']['story']:
                            if 'all_subattachments' in p['comet_sections']['content']['story']['attachments'][0]['styles']['attachment']:
                                post.attachments = [media['url'] for media in p['comet_sections']['content']['story']['attachments'][0]['styles']['attachment']['all_subattachments']['nodes']]
                            else:
                                post.attachments = [p['comet_sections']['content']['story']['attachments'][0]['styles']['attachment']['media']['url']]
                        else:
                            post.attachments = []
                        self.posts.append(post)
                        self._logger.debug(f'Extract post: {post.__repr__()}')
                    except Exception as E:
                        self._logger.warning(f'Extract post failed: {type(E)}:{E.args()}: {p}')
                        continue

        stop = False
        listener1 = browser.cdp.add_listener(f'Listener 1: {self.__repr__()}', 'Network.responseReceived', url_contain=self.url)
        listener2 = browser.cdp.add_listener(f'Listener 2: {self.__repr__()}', 'Network.responseReceived', url_contain='https://www.facebook.com/api/graphql/')
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.posts) >= min_count else False,
                        True if time_until is None else False if len(self.posts) == 0 else True if self.posts[-1].created_time <= time_until else False,
                ]):
                    return
        finally:
            stop = True
            listener1.shutdown()
            listener2.shutdown()


class Post:

    def __init__(self, page: Page, id: int = None, pfbid: str = None):
        self.page: Page = page
        self.id: int = id if id else None
        self.pfbid: str = pfbid if pfbid else None
        self.comments: list[Comment] = []
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Facebook post: {self.page.alias if self.page.alias else self.page.id}:{self.id if self.id else self.pfbid}>'

    @property
    def url(self):
        return f'https://www.facebook.com/{self.page.id if self.page.id else self.page.alias}/posts/{self.id if self.id else self.pfbid}'

    def get(self, browser: ChromeProcess, min_count: int = 10, timeout: float = 10):

        def load_page():
            nonlocal stop
            browser.get(self.url)
            self._logger.info(f'Get: {self.url}')
            time.sleep(5)
            while not stop:
                browser.scroll(
                    x=browser.window_size[0] // 2 + int(10 * (random.random() - 0.5)),
                    y=browser.window_size[1] // 2 + int(10 * (random.random() - 0.5)),
                    x_distance=0,
                    y_distance=-800 + int(50 * (random.random() - 0.5)),
                    speed=1200 + int(200 * (random.random() - 0.5)),
                    count=1,
                    repeat_delay=0.5 + random.random(),
                )

        def read_received():
            while not stop:
                if len(listener1.queue):
                    r = listener1.get()
                    time.sleep(3)
                    response = browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body']
                    post = json.loads(BeautifulSoup(response, features='html.parser').find('script', type='application/json', string=re.compile(r'"post_id"')).text)['require'][0][3][0]['__bbox']['require'][-7][3][1]['__bbox']['result']['data']['node']
                    if self.page.id != int(post['comet_sections']['content']['story']['actors'][0]['id']):
                        self.page = Page(id=int(post['comet_sections']['content']['story']['actors'][0]['id']), alias=post['comet_sections']['content']['story']['actors'][0]['url'].split('/')[-1])
                        self.page.name = post['comet_sections']['content']['story']['actors'][0]['name']
                    self.id = int(post['comet_sections']['content']['story']['post_id'])
                    self.pfbid = post['comet_sections']['content']['story']['wwwURL'].split('/posts/')[1]
                    self.content = post['comet_sections']['content']['story']['message']['text'] if post['comet_sections']['content']['story']['message'] else None
                    self.title = post['comet_sections']['context_layout']['story']['comet_sections']['title']['story']['title']['text'] if post['comet_sections']['context_layout']['story']['comet_sections']['title']['story']['title'] else None
                    self.created_time = datetime.datetime.fromtimestamp(post['comet_sections']['timestamp']['story']['creation_time'])
                    self.reaction_count = post['comet_sections']['feedback']['story']['story_ufi_container']['story']['feedback_context']['feedback_target_with_context']['comet_ufi_summary_and_actions_renderer']['feedback']['reaction_count']['count']
                    if 'attachments' in post['comet_sections']['content']['story']:
                        if 'all_subattachments' in post['comet_sections']['content']['story']['attachments'][0]['styles']['attachment']:
                            self.attachments = [media['url'] for media in post['comet_sections']['content']['story']['attachments'][0]['styles']['attachment']['all_subattachments']['nodes']]
                        else:
                            self.attachments = [post['comet_sections']['content']['story']['attachments'][0]['styles']['attachment']['media']['url']]
                    else:
                        self.attachments = []
                    self.encrypted_tracking = post['comet_sections']['content']['story']['encrypted_tracking']
                    self.sponsored_data = post['comet_sections']['content']['story']['sponsored_data']
                    self.text_format_metadata = post['comet_sections']['content']['story']['text_format_metadata']
                    self.ghl_mocked_encrypted_link = post['comet_sections']['content']['story']['ghl_mocked_encrypted_link']
                    self.ghl_label_mocked_cta_button = post['comet_sections']['content']['story']['ghl_label_mocked_cta_button']
                    self.target_group = post['comet_sections']['content']['story']['target_group']
                    self.attached_story = post['comet_sections']['content']['story']['attached_story']
                    self.copyright_violation_header = post['comet_sections']['copyright_violation_header']
                    self.header = post['comet_sections']['header']
                    self.aymt_footer = post['comet_sections']['aymt_footer']
                    self.outer_footer = post['comet_sections']['outer_footer']
                    self.footer = post['comet_sections']['footer']
                    self.is_prod_eligible = post['comet_sections']['timestamp']['is_prod_eligible']
                    self.override_url = post['comet_sections']['timestamp']['override_url']
                    self.video_override_url = post['comet_sections']['timestamp']['video_override_url']
                    self.unpublished_content_type = post['comet_sections']['timestamp']['story']['unpublished_content_type']
                    self.ghl_label = post['comet_sections']['timestamp']['story']['ghl_label']
                    for c in post['comet_sections']['feedback']['story']['story_ufi_container']['story']['feedback_context']['feedback_target_with_context']['comment_list_renderer']['feedback']['comment_rendering_instance_for_feed_location']['comments']['edges']:
                        try:
                            comment = Comment(self.page, self, c['node']['legacy_fbid'])
                            comment.content = c['node']['body']['text']
                            comment.created_time = datetime.datetime.fromtimestamp(c['node']['created_time'])
                            comment.reaction_count = c['node']['feedback']['reactors']['count_reduced']
                            if self.page.id == c['node']['author']['id']:
                                comment.author = self.page
                            else:
                                if 'pfbid' in c['node']['author']['id']:
                                    comment.author = Page(None)
                                    comment.author.pfbid = c['node']['author']['id']
                                else:
                                    comment.author = Page(int(c['node']['author']['id']))
                                if c['node']['author']['name']:
                                    comment.author.name = c['node']['author']['name']
                                if c['node']['author']['url']:
                                    comment.author.alias = c['node']['author']['url'].split('/')[-1]
                            comment.should_show_reply_count = c['node']['feedback']['expansion_info']['should_show_reply_count']
                            comment.viewer_actor = c['node']['feedback']['viewer_actor']
                            comment.if_viewer_can_comment_anonymously = c['node']['feedback']['if_viewer_can_comment_anonymously']
                            comment.comment_composer_placeholder = c['node']['feedback']['comment_composer_placeholder']
                            comment.constituent_badge_banner_renderer = c['node']['feedback']['constituent_badge_banner_renderer']
                            comment.associated_group = c['node']['feedback']['associated_group']
                            comment.have_comments_been_disabled = c['node']['feedback']['have_comments_been_disabled']
                            comment.are_live_video_comments_disabled = c['node']['feedback']['are_live_video_comments_disabled']
                            comment.is_viewer_muted = c['node']['feedback']['is_viewer_muted']
                            comment.comment_rendering_instance = c['node']['feedback']['comment_rendering_instance']
                            comment.viewer_feedback_reaction_info = c['node']['feedback']['viewer_feedback_reaction_info']
                            comment.is_markdown_enabled = c['node']['is_markdown_enabled']
                            comment.community_comment_signal_renderer = c['node']['community_comment_signal_renderer']
                            comment.comment_menu_tooltip = c['node']['comment_menu_tooltip']
                            comment.should_show_comment_menu = c['node']['should_show_comment_menu']
                            comment.is_author_weak_reference = c['node']['is_author_weak_reference']
                            comment.comment_parent = c['node']['comment_parent']
                            comment.is_declined_by_group_admin_assistant = c['node']['is_declined_by_group_admin_assistant']
                            comment.is_gaming_video_comment = c['node']['is_gaming_video_comment']
                            comment.translatability_for_viewer = c['node']['translatability_for_viewer']['source_dialect']
                            comment.written_while_video_was_live = c['node']['written_while_video_was_live']
                            comment.group_comment_info = c['node']['group_comment_info']
                            comment.bizweb_comment_info = c['node']['bizweb_comment_info']
                            comment.has_constituent_badge = c['node']['has_constituent_badge']
                            comment.can_viewer_see_subsribe_button = c['node']['can_viewer_see_subsribe_button']
                            comment.can_see_constituent_badge_upsell = c['node']['can_see_constituent_badge_upsell']
                            comment.question_and_answer_type = c['node']['question_and_answer_type']
                            comment.author_user_signals_renderer = c['node']['author_user_signals_renderer']
                            comment.author_badge_renderers = c['node']['author_badge_renderers']
                            comment.can_show_multiple_identity_badges = c['node']['can_show_multiple_identity_badges']
                            comment.is_viewer_comment_poster = c['node']['is_viewer_comment_poster']
                            comment.gen_ai_content_transparency_label_renderer = c['node']['gen_ai_content_transparency_label_renderer']
                            comment.work_ama_answer_status = c['node']['work_ama_answer_status']
                            comment.work_knowledge_inline_annotation_comment_badge_renderer = c['node']['work_knowledge_inline_annotation_comment_badge_renderer']
                            comment.business_comment_attributes = c['node']['business_comment_attributes']
                            comment.is_live_video_comment = c['node']['is_live_video_comment']
                            comment.translation_available_for_viewer = c['node']['translation_available_for_viewer']
                            comment.inline_survey_config = c['node']['inline_survey_config']
                            comment.spam_display_mode = c['node']['spam_display_mode']
                            comment.attached_story = c['node']['attached_story']
                            comment.comment_direct_parent = c['node']['comment_direct_parent']
                            comment.is_disabled = c['node']['is_disabled']
                            comment.work_answered_event_comment_renderer = c['node']['work_answered_event_comment_renderer']
                            comment.comment_upper_badge_renderer = c['node']['comment_upper_badge_renderer']
                            comment.elevated_comment_data = c['node']['elevated_comment_data']
                            self.comments.append(comment)
                            self._logger.debug(f'Extract comment: {comment.__repr__()}')
                        except Exception as E:
                            self._logger.warning(f'Extract comment failed: {type(E)}:{E.args()}: {c}')
                            continue

        stop = False
        listener1 = browser.cdp.add_listener(f'Listener 1: {self.__repr__()}', 'Network.responseReceived', url_contain=self.url)
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.comments) >= min_count else False,
                ]):
                    return
        finally:
            stop = True
            listener1.shutdown()


class Comment:

    def __init__(self, page: Page, post: Post, id: int = None):
        self.page: Page = page
        self.post: Post = post
        self.id: int = id
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Facebook post: {self.page.alias if self.page.alias else self.page.id}:{self.id if self.id else self.pfbid}>'

    @property
    def url(self):
        return f'https://www.facebook.com/{self.page.id if self.page.id else self.page.alias}/posts/{self.post.id if self.post.id else self.post.pfbid}?comment_id={id}'
