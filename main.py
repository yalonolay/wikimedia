import os
import logging
import asyncio
import aiofiles
import argparse
from os import path
from sys import stdout
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

import dataconf
import pywikibot
import pandas as pd
from tqdm import tqdm
from dataclasses import dataclass
from aiocsv import AsyncWriter
from pandas import Timestamp
from pywikibot.exceptions import InvalidTitleError, NoPageError, IsRedirectPageError

from consts import LOG_FORMAT, USER_COLUMN_LOOKUP, USER_NAME_COL, USER_ID_COL, IS_HIDDEN_USER_COL, EDITS_SIZE_COL, \
    TAGS_COL, USER_TALK_COL, PAGE_NAME_COL, LAST_FETCH_USERS_TS_COL, PAGE_URL_COL, LAST_FETCH_PAGE_NAME_TS_COL

logger = logging.getLogger(__name__)


@dataclass
class WikiMediaConfig:
    local_path: str
    pages_file_name: str
    users_file_suffix: str
    users_file_dir: str
    total_pages: int
    max_users_per_file: int


def setup_logs(app_name: str = "", level: int = logging.INFO) -> None:
    log_format = LOG_FORMAT
    if app_name:
        log_format = log_format.replace("%(name)s", app_name)
    logging.basicConfig(level=level, format=log_format)


def tqdm_props(desc: str) -> Dict[str, Any]:
    props = {"desc": desc, "disable": False, "mininterval": 3, "file": stdout}
    return props


def get_wiki_site(language: str = 'he') -> pywikibot.Site:
    wiki_site = pywikibot.Site(language, 'wikipedia')
    return wiki_site


def get_user_contribution(user_name, wiki_site: pywikibot.Site, total=100) -> List[Tuple[pywikibot.Page, int, Timestamp, str]]:
    # E.G.:
    # (Page('יהודה שפר'), 37004282, Timestamp(2023, 8, 31, 8, 45, 39), '/* לאחר סיום תפקידו בפרקליטות  */ נכון לעת עתה')
    wiki_user = pywikibot.User(wiki_site, user_name)
    contribution = wiki_site.usercontribs(userprefix=wiki_user, total=total)
    contribution_list = list(contribution)
    return contribution_list


def get_page(page_name: str, wiki_site: pywikibot.Site = None) -> pywikibot.Page:
    if wiki_site is None:
        wiki_site = get_wiki_site()
    wiki_page = pywikibot.Page(wiki_site, page_name)
    return wiki_page


def agg_tags(tags_lists: List[str]) -> Optional[List[str]]:
    clean_tags_set = set()
    for tags in tags_lists:
        if tags:
            for tag in tags:
                clean_tags_set.add(tag)
    clean_tags_list = list(clean_tags_set)
    return clean_tags_list


def normalize_user_df(users_df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Start normalizing users df")
    users_df = users_df.filter(['user', 'userid', 'userhidden', 'tags', 'size'])
    users_df = users_df.rename(columns=USER_COLUMN_LOOKUP)
    users_groups = users_df.groupby([USER_NAME_COL, USER_ID_COL])
    users_df = users_groups.agg(
            edits_size_bytes=pd.NamedAgg(column=EDITS_SIZE_COL, aggfunc='sum'),
            tags=pd.NamedAgg(column=TAGS_COL, aggfunc=agg_tags),
            is_hidden_user=pd.NamedAgg(column=IS_HIDDEN_USER_COL, aggfunc=lambda hidden_list: list(set(hidden_list))))
    users_df = users_df.reset_index(drop=False)
    logger.debug("Finished normalizing users df")
    return users_df


def add_user_talk(page: pywikibot.Page, users_df: pd.DataFrame, inplace: bool = True) -> Optional[pd.DataFrame]:
    logger.debug(f"Start fetching user talk")
    if not inplace:
        users_df = users_df.copy()
    try:
        talks_text = page.toggleTalkPage().get()
    except (InvalidTitleError, NoPageError, IsRedirectPageError):
        users_df[USER_TALK_COL] = None
        return
    talks_list = talks_text.split('(IST)')
    filter_talk = lambda user: list(filter(lambda talk_line: user in talk_line, talks_list))
    users_df[USER_TALK_COL] = users_df[USER_NAME_COL].apply(filter_talk)
    empty_list_mask = ~users_df[USER_TALK_COL].apply(bool)
    users_df.loc[empty_list_mask, USER_TALK_COL] = None
    logger.debug("Finished fetching user talk")
    if not inplace:
        return users_df


async def append_csv_line(df: pd.DataFrame, file_path: str, file_name: str) -> None:
    logger.info(f"saving {len(df)} rows to csv")
    saving_path = path.join(file_path, file_name) + '.csv'
    if not path.exists(saving_path):
        df.to_csv(saving_path, index=False)
        return
    async with aiofiles.open(saving_path, mode="a", encoding="utf-8-sig", newline="\n") as afp:
        writer = AsyncWriter(afp, dialect="unix")
        await writer.writerow(df.squeeze())


def save_parquet(df: pd.DataFrame, file_path: str, file_name: str) -> None:
    if df.empty:
        return
    logger.info(f"saving {len(df)} rows to parquet")
    if not path.exists(file_path):
        logger.info(f"Creating dir {file_path}")
        os.makedirs('datafolder', exist_ok=True)
    saving_path = path.join(file_path, file_name) + '.parquet'
    df.to_parquet(saving_path, index=False)


async def fetch_all_pages(config: WikiMediaConfig) -> None:
    logger.info("\nStart fetching all pages")
    wiki_site = get_wiki_site()
    all_pages_generator = wiki_site.allpages()
    with tqdm(total=config.total_pages, **tqdm_props("Fetch all pages")) as progress_bar:
        for page in all_pages_generator:
            page_name = page.title()
            page_url = page.full_url()
            update_dict = {PAGE_NAME_COL: [page_name], PAGE_URL_COL: [page_url], LAST_FETCH_PAGE_NAME_TS_COL: str(datetime.today())}
            curr_page_df = pd.DataFrame(update_dict)
            await append_csv_line(curr_page_df, config.local_path, config.pages_file_name)
            progress_bar.update(1)
    logger.info("Finished fetching all pages")


async def fetch_users_edits_by_page(config: WikiMediaConfig, reproduce: bool = False, normalize_user: bool = True) -> None:
    all_pages_path = path.join(config.local_path, config.pages_file_name) + '.csv'
    all_pages_df = pd.read_csv(all_pages_path, low_memory=False, dtype={LAST_FETCH_USERS_TS_COL: str})
    job_rows_mask = all_pages_df.apply(lambda _: True, axis=1)
    last_fetched_users = all_pages_df.get(LAST_FETCH_USERS_TS_COL)
    if last_fetched_users.any() and (not reproduce):
        job_rows_mask = last_fetched_users.isna()
    wiki_site = get_wiki_site()
    logger.info(f"start fetching users edits for {len(all_pages_df[job_rows_mask])} pages")
    all_pages_df.loc[job_rows_mask, 'group'] = all_pages_df[job_rows_mask].index // config.max_users_per_file
    groups = all_pages_df[job_rows_mask].groupby('group')
    with tqdm(total=len(all_pages_df[job_rows_mask]), **tqdm_props("fetch users edits")) as progress_bar:
        users_df = pd.DataFrame()
        for group, pages_group_df in groups:
            for page_obj in pages_group_df.itertuples():
                progress_bar.update(1)
                all_pages_df.loc[page_obj.Index, LAST_FETCH_USERS_TS_COL] = str(datetime.today())
                curr_page_name = page_obj.page_name
                if pd.isna(curr_page_name):
                    continue
                page = get_page(curr_page_name, wiki_site)
                try:
                    curr_users_df = pd.DataFrame(page.revisions())
                except (InvalidTitleError, NoPageError):
                    continue
                if normalize_user:
                    curr_users_df = normalize_user_df(curr_users_df)
                curr_users_df[PAGE_NAME_COL] = curr_page_name
                add_user_talk(page, curr_users_df)
                users_df = pd.concat([users_df, curr_users_df], ignore_index=True)
            file_name = f"{int(group)}_{config.users_file_suffix}"
            file_path = f"{config.local_path}/{config.users_file_dir}"
            save_parquet(users_df, file_path, file_name)
            all_pages_df.to_csv(all_pages_path, index=False)
            users_df = pd.DataFrame()
    logger.info("Finished fetching users edits")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', default="config.conf", help='Config file for the layer fetcher')
    parser.add_argument('-j', help='job type', choices=['pages', 'users'], required=True)
    parser.add_argument('-r', help='reproduce all pages', type=bool, default=False)
    parser.add_argument('-n', help='drop duplicated users in every page and remove details', type=bool, default=True)
    args = parser.parse_args()
    config: WikiMediaConfig = dataconf.load(args.c, WikiMediaConfig)
    setup_logs(app_name='wikimedia', level=logging.INFO)
    if args.j == 'pages':
        await fetch_all_pages(config)
    elif args.j == 'users':
        await fetch_users_edits_by_page(config, args.r, args.n)

if __name__ == '__main__':
    asyncio.run(main())
