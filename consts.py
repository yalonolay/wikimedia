LOG_FORMAT: str = '\n%(asctime)s %(levelname)-8s %(name)s:  %(message)-40s   ### %(funcName)s | %(module)s  '

USER_NAME_COL = 'user_name'
USER_ID_COL = 'user_id'
IS_HIDDEN_USER_COL = 'is_hidden_user'
EDITS_SIZE_COL = 'edits_size_bytes'
TAGS_COL = 'tags'
USER_TALK_COL = 'user_talk'
PAGE_NAME_COL = 'page_name'
PAGE_URL_COL = 'page_url'
LAST_FETCH_USERS_TS_COL = 'last_fetch_users_ts'
LAST_FETCH_PAGE_NAME_TS_COL = 'last_fetch_page_name_ts'

USER_COLUMN_LOOKUP = {'user': USER_NAME_COL,
                      'userid': USER_ID_COL,
                      'userhidden': IS_HIDDEN_USER_COL,
                      'size': EDITS_SIZE_COL}
