import datetime
import json
import random
import threading
import time

import dateutil

from .webdriver import ChromeProcess


class Forum:

    def __init__(self, alias: str):
        self.alias: str = alias
        self.url: str = f'https://www.dcard.tw/f/{alias}?tab=latest'
        self.posts: list[Post] = []

    def __repr__(self):
        return f'<Dcard forum: {self.alias}>'

    def get_posts(self, browser: ChromeProcess, min_count: int = 10, time_until: datetime.datetime = None, timeout: int = 30):

        def load_page():
            browser.get(self.url)
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
                        True if min_count is None else True if len(self.posts) > min_count else False,
                        True if time_until is None else True if self.posts[-1].created_at < time_until else False,
                ]):
                    return
                if stop:
                    return
        finally:
            stop = True


class Post:

    def __init__(self, forum: Forum, id: int):
        self.forum: Forum = forum
        self.id: int = id
        self.url: str = f'https://www.dcard.tw/f/{self.forum.alias}/p/{self.id}'
        self.comments: list[Comment] = []

    def __repr__(self):
        return f'<Dcard post: {self.forum.alias}:{self.id}>'

    def get_post(self, browser: ChromeProcess, min_count: int = 30, timeout: int = 30):

        def load_page():
            nonlocal stop
            browser.get(self.url)
            time.sleep(1)
            scrollY = 0
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
                new_scrollY = int(browser.cdp.get_received_by_id(browser.cdp.send('Runtime.evaluate', expression='window.scrollY;'))['result']['result']['value'])
                if scrollY == new_scrollY:
                    stop = True
                    return
                else:
                    scrollY = new_scrollY

        def read_received():
            nonlocal start_idx
            while not stop:
                for r in browser.cdp.received[start_idx:]:
                    start_idx += 1
                    if r['method'] == 'Network.responseReceived':
                        if f'https://www.dcard.tw/service/api/v2/posts/{self.id}?withPreview=true' in r['params']['response']['url']:
                            response = json.loads(browser.cdp.get_received_by_id(browser.cdp.send('Network.getResponseBody', requestId=r['params']['requestId']))['result']['body'])
                            self.title = response['title']
                            self.excerpt = response['excerpt']
                            self.anonymous_school = response['anonymousSchool']
                            self.anonymous_department = response['anonymousDepartment']
                            self.pinned = response['pinned']
                            self.reply_id = response['replyId']
                            self.created_at = dateutil.parser.parse(response['createdAt'])
                            self.updated_at = dateutil.parser.parse(response['updatedAt'])
                            self.comment_count = response['commentCount']
                            self.like_count = response['likeCount']
                            self.collection_count = response['collectionCount']
                            self.share_countt = response['shareCount']
                            self.tags = response['tags']
                            self.topics = response['topics']
                            self.with_nickname = response['withNickname']
                            self.report_reason = response['reportReason']
                            self.hidden_by_author = response['hiddenByAuthor']
                            self.pinned_in_profile_wall = response['pinnedInProfileWall']
                            self.hidden_in_profile_wall = response['hiddenInProfileWall']
                            self.nsfw = response['nsfw']
                            if 'school' in response: self.school = response['school']
                            if 'department' in response: self.department = response['department']
                            self.reply_title = response['replyTitle']
                            self.persona_subscriptable = response['personaSubscriptable']
                            self.gender = response['gender']
                            self.identity_type = response['identityType']
                            self.persona_nickname = response['personaNickname']
                            self.persona_uid = response['personaUid']
                            self.quote_count = response['quoteCount']
                            self.content = response['content']
                            self.reactions = response['reactions']
                            self.hidden = response['hidden']
                            self.is_suspicious_account = response['isSuspiciousAccount']
                            self.is_moderator = response['isModerator']
                            self.layout = response['layout']
                            self.spoiler_alert = response['spoilerAlert']
                            self.with_images = response['withImages']
                            self.with_videos = response['withVideos']
                            self.media = response['media']
                            self.report_reasonText = response['reportReasonText']
                            self.is_selected_post = response['isSelectedPost']
                            self.unsafe = response['unsafe']
                            self.enable_nested_comment = response['enableNestedComment']
                            self.total_comment_count = response['totalCommentCount']
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
                            for item in response['items']:
                                comment = Comment(self.forum, self, item['floor'])
                                comment.id = item['id']
                                comment.anonymous = item['anonymous']
                                comment.created_at = dateutil.parser.parse(item['createdAt'])
                                comment.updated_at = dateutil.parser.parse(item['updatedAt'])
                                comment.content = item['content']
                                comment.like_count = item['likeCount']
                                comment.with_nickname = item['withNickname']
                                comment.hidden_by_author = item['hiddenByAuthor']
                                comment.pinned = item['pinned']
                                comment.host = item['host']
                                comment.gender = item['gender']
                                if 'department' in item: comment.school = item['school']
                                if 'department' in item: comment.department = item['department']
                                comment.persona_nickname = item['personaNickname']
                                comment.persona_uid = item['personaUid']
                                comment.report_reason = item['reportReason']
                                comment.is_blocked = item['isBlocked']
                                comment.is_blocker = item['isBlocker']
                                comment.media_meta = item['mediaMeta']
                                comment.current_member = item['currentMember']
                                comment.hidden = item['hidden']
                                comment.in_review = item['inReview']
                                comment.links = item['links']
                                comment.report_reason_text = item['reportReasonText']
                                comment.is_suspicious_account = item['isSuspiciousAccount']
                                comment.is_moderator = item['isModerator']
                                comment.doorplate = item['doorplate']
                                comment.subcomment_count = item['subCommentCount']
                                comment.with_badge = item['withBadge']
                                comment.is_throttled = item['isThrottled']
                                comment.identity_idV3 = item['identityIdV3']
                                comment.edited = item['edited']
                                comment.post_avatar = item['postAvatar']
                                comment.activity_avatar = item['activityAvatar']
                                comment.verified_badge = item['verifiedBadge']
                                comment.member_type = item['memberType']
                                if 'creatorBadge' in item: comment.creator_badge = item['creatorBadge']
                                if 'officialCreatorBadge' in item: comment.official_creator_badge = item['officialCreatorBadge']
                                self.comments.append(comment)

        stop = False
        start_idx = len(browser.cdp.received)
        end_time = time.time() + timeout
        threading.Thread(target=load_page).start()
        threading.Thread(target=read_received).start()
        try:
            while time.time() < end_time:
                if all([
                        True if min_count is None else True if len(self.comments) > min_count else False,
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


class Comment:

    def __init__(self, forum: Forum, post: Post, floor: int):
        self.forum: Forum = forum
        self.post: Post = post
        self.floor: int = floor

    def __repr__(self):
        return f'<Dcard comment: {self.forum.alias}:{self.post.id}:b{self.floor}>'
