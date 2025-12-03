import json
import os
import gspread
from google.oauth2.service_account import Credentials
from loguru import logger
from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init
from xhs_utils.data_util import handle_note_info


class Data_Spider():
    def __init__(self):
        self.xhs_apis = XHS_Apis()
        self._printed_sample = False

    def spider_note(self, note_url: str, cookies_str: str, proxies=None):
        note_info = None
        try:
            success, msg, note_info = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
            if success:
                note = note_info['data']['items'][0]

                # 自定义字段
                note['url'] = note_url
                note = handle_note_info(note)

                if not self._printed_sample:
                    logger.info("Sample note: " + json.dumps(note, ensure_ascii=False)[:2000])
                    self._printed_sample = True

        except Exception as e:
            success = False
            msg = e

        logger.info(f'爬取笔记 {note_url}: {success}, msg={msg}')
        return success, msg, note

    def spider_some_search_note(self, query: str, require_num: int, cookies_str: str, proxies=None):
        note_urls = []
        note_info_list = []
        success = False
        msg = None

        try:
            success, msg, notes = self.xhs_apis.search_some_note(
                query, require_num, cookies_str,
                0, 0, 0, 0, 0, None, proxies
            )

            if success:
                notes = list(filter(lambda x: x['model_type'] == "note", notes))
                logger.info(f'关键词 {query} 搜到 {len(notes)} 条')
                for note in notes:
                    url = f"https://www.xiaohongshu.com/explore/{note['id']}?xsec_token={note['xsec_token']}"
                    note_urls.append(url)

                for url in note_urls:
                    s, m, note_info = self.spider_note(url, cookies_str)
                    if s and note_info:
                        note_info_list.append(note_info)

        except Exception as e:
            success = False
            msg = e

        return note_info_list, success, msg


# ===== Google Sheet 配置 =====
SPREADSHEET_ID = "1uge_TtzAauHqKv7pNViQ6lJ1n6RkKwMWGHJAd92y2fE"
SERVICE_ACCOUNT_FILE = "service_account.json"

def write_to_google_sheet(note_list, sheet_name: str):
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(sheet_name)

    header = [
        "title", "desc",
        "like_count", "collect_count", "comment_count", "share_count",
        "author", "author_id",
        "ip", "url"
    ]

    if not note_list:
        logger.info(f"{sheet_name}: 本次 note_list 为空，不覆盖上一次数据")
        return

    rows = []
    for n in note_list:
        rows.append([
            n.get("title", ""),
            n.get("desc", ""),
            n.get("liked_count", 0),
            n.get("collected_count", 0),
            n.get("comment_count", 0),
            n.get("share_count", 0),
            n.get("nickname", ""),
            n.get("user_id", ""),
            n.get("ip_location", ""),
            n.get("note_url1") or n.get("note_url") or n.get("url", ""),
        ])

    ws.clear()
    ws.append_rows([header] + rows)
    logger.info(f"{sheet_name}: 已写入 {len(rows)} 条数据")


if __name__ == '__main__':
    cookies_str, base_path = init()
    data_spider = Data_Spider()

    BRANDS = [
        ("dtcpay",   "dtcpay_v2"),
        ("Revolut",  "Revolut_v2"),
        ("Wise",     "Wise_v2"),
        ("YouTrip",  "YouTrip_v2"),
        ("Redotpay", "Redotpay_v2"),
        ("FOMOpay",  "FOMOpay_v2"),
    ]

    require_num_each = 100

    import time, random

    for keyword, sheet_name in BRANDS:
        logger.info(f"开始爬取品牌：{keyword}")
        note_list, success, msg = data_spider.spider_some_search_note(
            query=keyword,
            require_num=require_num_each,
            cookies_str=cookies_str,
        )

        logger.info(
            f"[{sheet_name}] 搜索 {keyword} 完成，success={success}, msg={msg}, "
            f"爬到 {len(note_list)} 条笔记"
        )

        write_to_google_sheet(note_list, sheet_name)

        sleep_sec = random.uniform(10, 20)
        logger.info(f"品牌 {keyword} 完成，休眠 {sleep_sec:.1f} 秒再继续下一家")
        time.sleep(sleep_sec)
