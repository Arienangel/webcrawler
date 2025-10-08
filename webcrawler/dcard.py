import datetime
import json
import logging
import random
import threading
import time

import dateutil
from bs4 import BeautifulSoup

from .webdriver import ChromeProcess


class Forum:

    def __init__(self, alias: str = None):
        self.alias: str = alias
        self.posts: list[Post] = []
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Dcard forum: {self.alias}>'

    @property
    def url(self):
        return f'https://www.dcard.tw/f/{self.alias}?tab=latest'

    def get(self, browser: ChromeProcess, min_count: int = 10, time_until: datetime.datetime = None, timeout: float = 30):

        def load_page():
            browser.get(self.url)
            self._logger.info(f'Connect: {self.url}')
            time.sleep(8)
            while not stop:
                if 'https://challenges.cloudflare.com/turnstile' in browser.page_source:
                    continue
                browser.scroll(
                    x=browser.window_size[0] // 2 + int(10 * (random.random() - 0.5)),
                    y=browser.window_size[1] // 2 + int(10 * (random.random() - 0.5)),
                    x_distance=0,
                    y_distance=-800 + int(50 * (random.random() - 0.5)),
                    speed=1200 + int(200 * (random.random() - 0.5)),
                    count=1,
                    repeat_delay=0.5 + random.random(),
                    blocking=True,
                )

        def read_received():
            while not stop:
                time.sleep(0.5)
                if len(listener1.queue):
                    r = listener1.get()
                    response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                    self.id = response['id']
                    self.name = response['name']
                    self.description = response['description']
                    self.created_time = dateutil.parser.parse(response['createdAt'])
                    self.modified_time = dateutil.parser.parse(response['updatedAt'])
                    self.subscriptionCount = response['subscriptionCount']
                    self.subscribed = response['subscribed']
                    self.read = response['read']
                    self.can_post = response['canPost']
                    self.ignore_post = response['ignorePost']
                    self.invisible = response['invisible']
                    self.is_school = response['isSchool']
                    self.fully_anonymous = response['fullyAnonymous']
                    self.can_use_nickname = response['canUseNickname']
                    self.should_categorized = response['shouldCategorized']
                    self.should_post_categorized = response['shouldPostCategorized']
                    self.has_post_categories = response['hasPostCategories']
                    self.post_title_placeholder = response['postTitlePlaceholder']
                    self.subcategories = response['subcategories']
                    self.topics = response['topics']
                    self.nsfw = response['nsfw']
                    self.media_threshold = response['mediaThreshold']
                    self.limit_countries = response['limitCountries']
                    self.limit_stage = response['limitStage']
                    self.available_layouts = response['availableLayouts']
                    self.hero_image = response['heroImage']['url']
                    self.logo = response['logo']['url']
                    self.post_count_last_30_days = response['postCount']['last30Days']
                    self.favorite = response['favorite']
                    self.is_persona_page = response['isPersonaPage']
                    self.is_featured_page = response['isFeaturedPage']
                    self.enable_private_message = response['enablePrivateMessage']
                    self.restrict_identity_on_comment = response['restrictIdentityOnComment']
                    self.enable_selected_posts = response['enableSelectedPosts']
                    self.supported_reactions = response['supportedReactions']
                    self.display_selected_posts_tab = response['displaySelectedPostsTab']
                    self.allow_mod_manage_selected_posts = response['allowModManageSelectedPosts']
                    self.display_group_chat_entry = response['displayGroupChatEntry']
                    self.enable_hinted_validations = response['enableHintedValidations']
                    self.enable_edited_history = response['enableEditedHistory']
                    self.enable_immersive_video = response['enableImmersiveVideo']
                    self.discussion_volume = response['discussionVolume']
                    self.latest_post_pinned_at = dateutil.parser.parse(response['latestPostPinnedAt']) if response['latestPostPinnedAt'] in response else None
                if len(listener2.queue):
                    r = listener2.get()
                    response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                    for widget in response['widgets']:
                        if 'forumList' in widget:
                            try:
                                p = widget['forumList']['items'][0]['post']
                                post = Post(self, p['id'])
                                post.title = p['title']
                                post.excerpt = p['excerpt']
                                post.content = None
                                post.created_time = dateutil.parser.parse(p['createdAt'])
                                post.modified_time = dateutil.parser.parse(p['updatedAt'])
                                post.anonymous_school = p['anonymousSchool']
                                post.anonymous_department = p['anonymousDepartment']
                                post.with_nickname = p['withNickname']
                                post.author.school = p['school'] if 'school' in p else None
                                post.author.department = p['department'] if 'department' in p else None
                                post.author.nickname = p['personaNickname'] if 'personaNickname' in p else None
                                post.author.id = p['personaUid'] if 'personaUid' in p else None
                                post.author.gender = p['gender'] if 'gender' in p else None
                                post.author.is_suspicious_account = p['isSuspiciousAccount']
                                post.author.is_moderator = p['isModerator']
                                post.author.verified_badge = p['verifiedBadge']
                                post.author.member_type = p['memberType']
                                post.author.creator_badge = p['creatorBadge'] if 'creatorBadge' in p else None
                                post.author.official_creatorBadge = p['officialCreatorBadge'] if 'officialCreatorBadge' in p else None
                                post.like_count = p['likeCount']
                                post.reactions = p['reactions']
                                post.comment_count = p['commentCount']
                                post.total_comment_count = p['totalCommentCount']
                                post.share_count = p['shareCount']
                                post.collection_count = p['collectionCount']
                                post.pinned = p['pinned']
                                post.reply_id = p['replyId']
                                post.tags = p['tags']
                                post.topics = p['topics']
                                post.report_reason = p['reportReason']
                                post.hidden_by_author = p['hiddenByAuthor']
                                post.pinned_in_profile_wall = p['pinnedInProfileWall']
                                post.hidden_in_profile_wall = p['hiddenInProfileWall']
                                post.nsfw = p['nsfw']
                                post.reply_title = p['replyTitle']
                                post.persona_subscriptable = p['personaSubscriptable']
                                post.identity_type = p['identityType']
                                post.quote_count = p['quoteCount']
                                post.hidden = p['hidden']
                                post.layout = p['layout']
                                post.spoiler_alert = p['spoilerAlert']
                                post.with_images = p['withImages']
                                post.with_videos = p['withVideos']
                                post.media = p['media']
                                post.report_reasonText = p['reportReasonText']
                                post.is_selected_post = p['isSelectedPost']
                                post.unsafe = p['unsafe']
                                post.enable_nested_comment = p['enableNestedComment']
                                post.media_meta = p['mediaMeta']
                                post.edited = p['edited']
                                post.links = p['links']
                                post.identity_idV3 = p['identityIdV3']
                                post.enable_list_link_preview = p['enableListLinkPreview']
                                post.post_avatar = p['postAvatar']
                                post.previews = p['previews']
                                post.is_blocker = p['isBlocker']
                                post.is_blocked = p['isBlocked']
                                post.excerpt_comments = p['excerptComments']
                                post.in_review = p['inReview']
                                post.activity_avatar = p['activityAvatar']
                                self.posts.append(post)
                                self._logger.debug(f'Extract post: {post.__repr__()}')
                            except Exception as E:
                                self._logger.warning(f'Extract post failed: {type(E)}:{E.args}: {p}')
                                continue

        stop = False
        listener1 = browser.cdp.add_listener(f'Listener 1: {self.__repr__()}', 'Network.responseReceived', url_contain='https://www.dcard.tw/service/api/v2/forums')
        listener2 = browser.cdp.add_listener(f'Listener 2: {self.__repr__()}', 'Network.responseReceived', url_contain='https://www.dcard.tw/service/api/v2/globalPaging/page')
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                time.sleep(0.5)
                if all([
                        True if min_count is None else True if len(self.posts) >= min_count else False,
                        True if time_until is None else False if len(self.posts) == 0 else True if self.posts[-1].created_at <= time_until else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True
            listener1.shutdown()
            listener2.shutdown()
            self._logger.debug(f'#Posts: {len(self.posts)}')


class Post:

    def __init__(self, forum: Forum = None, id: int = None):
        self.forum: Forum = forum
        self.id: int = id
        self.created_time: datetime.datetime = datetime.datetime.fromtimestamp(0)
        self.author: User = User()
        self.title: str = None
        self.content: str = None
        self.comments: list[Comment] = []
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Dcard post: {self.forum.alias}:{self.id}>'

    @property
    def url(self):
        return f'https://www.dcard.tw/f/{self.forum.alias}/p/{self.id}'

    def get(self, browser: ChromeProcess, min_count: int = 10, timeout: float = 10):

        def load_page():
            nonlocal stop
            browser.get(self.url)
            self._logger.info(f'Connect: {self.url}')
            time.sleep(8)
            while not stop:
                if 'https://challenges.cloudflare.com/turnstile' in browser.page_source:
                    continue
                browser.cdp.send('Runtime.evaluate', expression="document.querySelector('div#comment-list-section button:nth-last-child(2)').click()")
                browser.scroll(
                    x=browser.window_size[0] // 2 + int(10 * (random.random() - 0.5)),
                    y=browser.window_size[1] // 2 + int(10 * (random.random() - 0.5)),
                    x_distance=0,
                    y_distance=-800 + int(50 * (random.random() - 0.5)),
                    speed=1200 + int(200 * (random.random() - 0.5)),
                    count=1,
                    repeat_delay=0.5 + random.random(),
                    blocking=True,
                )

        def read_received():
            while not stop:
                time.sleep(0.5)
                if len(listener1.queue):
                    r = listener1.get()
                    time.sleep(0.5)
                    response = BeautifulSoup(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'], features="html.parser")
                    response = json.loads(response.find('script', type="application/ld+json").text)
                    self.title = response['headline']
                    self.content = response['text']
                    self.created_time = dateutil.parser.parse(response['datePublished'])
                    self.modified_time = dateutil.parser.parse(response['dateModified'])
                    self.author.school = response['author']['name'] if 'name' in response['author'] else None
                    self.author.department = response['author']['identifier'] if 'identifier' in response['author'] else None
                    self.author.nickname = response['author']['name'] if 'name' in response['author'] else None
                    self.author.id = response['author']['identifier'] if 'identifier' in response['author'] else None
                    self.author.gender = response['author']['gender'] if 'gender' in response['author'] else None
                    self.like_count = response['interactionStatistic'][0]['userInteractionCount']
                    self.comment_count = response['interactionStatistic'][1]['userInteractionCount']
                    self.share_count = response['interactionStatistic'][2]['userInteractionCount']
                if len(listener2.queue):
                    r = listener2.get()
                    response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                    self.title = response['title']
                    self.excerpt = response['excerpt']
                    self.content = response['content']
                    self.created_time = dateutil.parser.parse(response['createdAt'])
                    self.modified_time = dateutil.parser.parse(response['updatedAt'])
                    self.anonymous_school = response['anonymousSchool']
                    self.anonymous_department = response['anonymousDepartment']
                    self.with_nickname = response['withNickname']
                    self.author.school = response['school'] if 'school' in response else None
                    self.author.department = response['department'] if 'department' in response else None
                    self.author.nickname = response['personaNickname'] if 'personaNickname' in response else None
                    self.author.id = response['personaUid'] if 'personaUid' in response else None
                    self.author.gender = response['gender'] if 'gender' in c else None
                    self.author.is_suspicious_account = response['isSuspiciousAccount']
                    self.author.is_moderator = response['isModerator']
                    self.author.verified_badge = response['verifiedBadge']
                    self.author.member_type = response['memberType']
                    self.author.creator_badge = response['creatorBadge'] if 'creatorBadge' in response else None
                    self.author.official_creatorBadge = response['officialCreatorBadge'] if 'officialCreatorBadge' in response else None
                    self.like_count = response['likeCount']
                    self.reactions = response['reactions']
                    self.comment_count = response['commentCount']
                    self.total_comment_count = response['totalCommentCount']
                    self.share_count = response['shareCount']
                    self.collection_count = response['collectionCount']
                    self.pinned = response['pinned']
                    self.reply_id = response['replyId']
                    self.tags = response['tags']
                    self.topics = response['topics']
                    self.report_reason = response['reportReason']
                    self.hidden_by_author = response['hiddenByAuthor']
                    self.pinned_in_profile_wall = response['pinnedInProfileWall']
                    self.hidden_in_profile_wall = response['hiddenInProfileWall']
                    self.nsfw = response['nsfw']
                    self.reply_title = response['replyTitle']
                    self.persona_subscriptable = response['personaSubscriptable']
                    self.identity_type = response['identityType']
                    self.quote_count = response['quoteCount']
                    self.hidden = response['hidden']
                    self.layout = response['layout']
                    self.spoiler_alert = response['spoilerAlert']
                    self.with_images = response['withImages']
                    self.with_videos = response['withVideos']
                    self.media = response['media']
                    self.report_reasonText = response['reportReasonText']
                    self.is_selected_post = response['isSelectedPost']
                    self.unsafe = response['unsafe']
                    self.enable_nested_comment = response['enableNestedComment']
                    self.media_meta = response['mediaMeta']
                    self.edited = response['edited']
                    self.links = response['links']
                    self.identity_idV3 = response['identityIdV3']
                    self.enable_list_link_preview = response['enableListLinkPreview']
                    self.post_avatar = response['postAvatar']
                    self.previews = response['previews']
                    self.is_blocker = response['isBlocker']
                    self.is_blocked = response['isBlocked']
                    self.excerpt_comments = response['excerptComments']
                    self.in_review = response['inReview']
                    self.activity_avatar = response['activityAvatar']
                if len(listener3.queue):
                    r = listener3.get()
                    response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                    for c in response['items']:
                        try:
                            comment = Comment(self.forum, self, c['floor'])
                            comment.id = c['id']
                            comment.content = c['content'] if 'content' in c else None
                            comment.created_time = dateutil.parser.parse(c['createdAt'])
                            comment.modified_time = dateutil.parser.parse(c['updatedAt'])
                            comment.anonymous = c['anonymous'] if 'anonymous' in c else None
                            comment.with_nickname = c['withNickname']
                            comment.author.school = c['school'] if 'school' in c else None
                            comment.author.department = c['department'] if 'department' in c else None
                            comment.author.nickname = c['personaNickname'] if 'personaNickname' in c else None
                            comment.author.id = c['personaUid'] if 'personaUid' in c else None
                            comment.author.gender = c['gender'] if 'gender' in c else None
                            comment.author.is_suspicious_account = c['isSuspiciousAccount']
                            comment.author.is_moderator = c['isModerator']
                            comment.author.verified_badge = c['verifiedBadge']
                            comment.author.member_type = c['memberType']
                            comment.author.creator_badge = c['creatorBadge'] if 'creatorBadge' in c else None
                            comment.author.official_creator_badge = c['officialCreatorBadge'] if 'officialCreatorBadge' in c else None
                            comment.like_count = c['likeCount'] if 'likeCount' in c else None
                            comment.subcomment_count = c['subCommentCount'] if 'subCommentCount' in c else None
                            comment.hidden_by_author = c['hiddenByAuthor']
                            comment.pinned = c['pinned']
                            comment.host = c['host']
                            comment.report_reason = c['reportReason']
                            comment.is_blocked = c['isBlocked']
                            comment.is_blocker = c['isBlocker']
                            comment.media_meta = c['mediaMeta']
                            comment.current_member = c['currentMember']
                            comment.hidden = c['hidden']
                            comment.in_review = c['inReview']
                            comment.links = c['links']
                            comment.report_reason_text = c['reportReasonText']
                            comment.doorplate = c['doorplate']
                            comment.with_badge = c['withBadge']
                            comment.is_throttled = c['isThrottled']
                            comment.identity_idV3 = c['identityIdV3'] if 'identityIdV3' in c else None
                            comment.edited = c['edited']
                            comment.post_avatar = c['postAvatar']
                            comment.activity_avatar = c['activityAvatar']
                            self.comments.append(comment)
                            self._logger.debug(f'Extract comment: {comment.__repr__()}')
                        except Exception as E:
                            self._logger.warning(f'Extract comment failed: {type(E)}:{E.args}: {c}')
                            continue

        stop = False
        listener1 = browser.cdp.add_listener(f'Listener 1: {self.__repr__()}', 'Network.responseReceived', url_contain=self.url)
        listener2 = browser.cdp.add_listener(f'Listener 2: {self.__repr__()}', 'Network.responseReceived', url_contain=f'https://www.dcard.tw/service/api/v2/posts/{self.id}?withPreview=true')
        listener3 = browser.cdp.add_listener(f'Listener 3: {self.__repr__()}', 'Network.responseReceived', url_contain=f'https://www.dcard.tw/service/api/v3/posts/{self.id}/comments?sort=oldest')
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                time.sleep(0.5)
                if all([
                        True if min_count is None else True if len(self.comments) >= min_count else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True
            listener1.shutdown()
            listener2.shutdown()
            listener3.shutdown()
            self._logger.debug(f'#Comments: {len(self.comments)}')


class Comment:

    def __init__(self, forum: Forum = None, post: Post = None, floor: int = None):
        self.forum: Forum = forum
        self.post: Post = post
        self.floor: int = floor
        self.created_time: datetime.datetime = datetime.datetime.fromtimestamp(0)
        self.author: User = User()
        self.content: str = None
        self._logger = logging.getLogger(self.__repr__())

    def __repr__(self):
        return f'<Dcard comment: {self.forum.alias}:{self.post.id}:b{self.floor}>'


class User:

    def __init__(self, school: str = None, department: str = None, nickname: str = None, id: str = None, gender: str = None):
        self.school: str = school
        self.department: str = department
        self.nickname: str = nickname
        self.id: str = id
        self.gender: str = gender

    def __repr__(self):
        return f'<Dcard user: {self.school} {self.department}>'.strip()
