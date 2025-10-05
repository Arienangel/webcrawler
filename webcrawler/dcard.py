import datetime
import json
import random
import threading
import time

from bs4 import BeautifulSoup
import dateutil

from .webdriver import ChromeProcess


class Forum:

    def __init__(self, alias: str):
        self.alias: str = alias
        self.posts: list[Post] = []

    def __repr__(self):
        return f'<Dcard forum: {self.alias}>'

    @property
    def url(self):
        return f'https://www.dcard.tw/f/{self.alias}?tab=latest'

    def get_posts(self, browser: ChromeProcess, min_count: int = 10, time_until: datetime.datetime = None, timeout: float = 30):

        def load_page():
            browser.get(self.url, referrer='https://www.google.com/')
            time.sleep(3)
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
            nonlocal start_idx
            while not stop:
                for r in browser.cdp.received[start_idx:]:
                    start_idx += 1
                    if r['method'] == 'Network.responseReceived':
                        if 'https://www.dcard.tw/service/api/v2/forums' in r['params']['response']['url']:
                            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                            self.id = response['id']
                            self.name = response['name']
                            self.description = response['description']
                            self.subscriptionCount = response['subscriptionCount']
                            self.subscribedl = response['subscribed']
                            self.read = response['read']
                            self.created_at = dateutil.parser.parse(response['createdAt'])
                            self.updated_at = dateutil.parser.parse(response['updatedAt'])
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
                            self.latest_post_pinned_at = dateutil.parser.parse(response['latestPostPinnedAt'])
                        elif 'https://www.dcard.tw/service/api/v2/globalPaging/page' in r['params']['response']['url']:
                            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                            for widget in response['widgets']:
                                if 'forumList' in widget:
                                    post = Post(self, widget['forumList']['items'][0]['post']['id'])
                                    self.posts.append(post)

        stop = False
        start_idx = len(browser.cdp.received)
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.posts) >= min_count else False,
                        True if time_until is None else False if len(self.posts) == 0 else True if self.posts[-1].created_at <= time_until else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True

    def export(self, attributes: list[str], post_attributes: list[str]):
        data = {a: getattr(self, a, None) for a in attributes}
        if 'posts' in attributes:
            data.update({'posts': {post.id: {a: getattr(post, a, None) for a in post_attributes} for post in self.posts}})
        return json.dumps(data, ensure_ascii=False)


class Post:

    def __init__(self, forum: Forum, id: int):
        self.forum: Forum = forum
        self.id: int = id
        self.comments: list[Comment] = []

    def __repr__(self):
        return f'<Dcard post: {self.forum.alias}:{self.id}>'

    @property
    def url(self):
        return f'https://www.dcard.tw/f/{self.forum.alias}/p/{self.id}'

    def get_post(self, browser: ChromeProcess, min_count: int = 30, timeout: float = 30):

        def load_page():
            nonlocal stop
            browser.get(self.url, referrer='https://www.google.com/')
            time.sleep(3)
            while not stop:
                browser.cdp.send('Runtime.evaluate', expression="document.querySelector('div#comment-list-section button:nth-last-child(2)').click()")
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
            nonlocal start_idx
            while not stop:
                for r in browser.cdp.received[start_idx:]:
                    start_idx += 1
                    if r['method'] == 'Network.responseReceived':
                        if self.url in r['params']['response']['url']:
                            time.sleep(0.5)
                            response = BeautifulSoup(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'],
                                                     features="html.parser")
                            response = json.loads(response.find('script', type="application/ld+json").text)
                            self.title = response['headline']
                            self.content = response['text']
                            self.created_at = dateutil.parser.parse(response['datePublished'])
                            self.updated_at = dateutil.parser.parse(response['dateModified'])
                            self.persona_nickname = response['author']['name']
                            self.persona_uid = response['author']['identifier']
                            self.gender = response['author']['gender']
                            self.like_count = response['interactionStatistic'][0]['userInteractionCount']
                            self.comment_count = response['interactionStatistic'][1]['userInteractionCount']
                            self.share_count = response['interactionStatistic'][2]['userInteractionCount']
                        elif f'https://www.dcard.tw/service/api/v2/posts/{self.id}?withPreview=true' in r['params']['response']['url']:
                            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                            self.title = response['title']
                            self.excerpt = response['excerpt']
                            self.content = response['content']
                            self.created_at = dateutil.parser.parse(response['createdAt'])
                            self.updated_at = dateutil.parser.parse(response['updatedAt'])
                            self.anonymous_school = response['anonymousSchool']
                            self.anonymous_department = response['anonymousDepartment']
                            if 'school' in response: self.school = response['school']
                            if 'department' in response: self.department = response['department']
                            self.with_nickname = response['withNickname']
                            self.persona_nickname = response['personaNickname']
                            self.persona_uid = response['personaUid']
                            self.gender = response['gender']
                            self.is_suspicious_account = response['isSuspiciousAccount']
                            self.is_moderator = response['isModerator']
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
                            self.verified_badge = response['verifiedBadge']
                            self.member_type = response['memberType']
                            if 'creatorBadge' in response: self.creator_badge = response['creatorBadge']
                            if 'officialCreatorBadge' in response: self.official_creatorBadge = response['officialCreatorBadge']
                        elif f'https://www.dcard.tw/service/api/v3/posts/{self.id}/comments?sort=oldest' in r['params']['response']['url']:
                            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                            for c in response['items']:
                                comment = Comment(self.forum, self, c['floor'])
                                comment.id = c['id']
                                if 'content' in c: comment.content = c['content']
                                comment.created_at = dateutil.parser.parse(c['createdAt'])
                                comment.updated_at = dateutil.parser.parse(c['updatedAt'])
                                if 'anonymous' in c: comment.anonymous = c['anonymous']
                                if 'school' in c: comment.school = c['school']
                                if 'department' in c: comment.department = c['department']
                                comment.with_nickname = c['withNickname']
                                if 'persona_nickname' in c: comment.persona_nickname = c['personaNickname']
                                if 'persona_uid' in c: comment.persona_uid = c['personaUid']
                                if 'gender' in c: comment.gender = c['gender']
                                comment.is_suspicious_account = c['isSuspiciousAccount']
                                comment.is_moderator = c['isModerator']
                                if 'likeCount' in c: comment.like_count = c['likeCount']
                                if 'subCommentCount' in c: comment.subcomment_count = c['subCommentCount']
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
                                if 'identityIdV3' in c: comment.identity_idV3 = c['identityIdV3']
                                comment.edited = c['edited']
                                comment.post_avatar = c['postAvatar']
                                comment.activity_avatar = c['activityAvatar']
                                comment.verified_badge = c['verifiedBadge']
                                comment.member_type = c['memberType']
                                if 'creatorBadge' in c: comment.creator_badge = c['creatorBadge']
                                if 'officialCreatorBadge' in c: comment.official_creator_badge = c['officialCreatorBadge']
                                self.comments.append(comment)

        stop = False
        start_idx = len(browser.cdp.received)
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.comments) >= min_count else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True

        end_time = time.time() + timeout
        length = len(browser.cdp.received)
        while time.time() < end_time:
            if length == len(browser.cdp.received): break
            else: length = len(browser.cdp.received)

    def export(self, attributes: list[str], comment_attributes: list[str]):
        data = {a: getattr(self, a, None) for a in attributes}
        if 'comments' in attributes:
            data.update({'comments': {comment.id: {a: getattr(comment, a, None) for a in comment_attributes} for comment in self.comments}})
        return json.dumps(data, ensure_ascii=False)


class Comment:

    def __init__(self, forum: Forum, post: Post, floor: int):
        self.forum: Forum = forum
        self.post: Post = post
        self.floor: int = floor

    def __repr__(self):
        return f'<Dcard comment: {self.forum.alias}:{self.post.id}:b{self.floor}>'
