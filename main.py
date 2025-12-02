import json
import os
import gspread
from google.oauth2.service_account import Credentials
from loguru import logger
from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init
from xhs_utils.data_util import handle_note_info, download_note, save_to_xlsx


class Data_Spider():
    def __init__(self):
        self.xhs_apis = XHS_Apis()
        self._printed_sample = False  # 只打印一条样本

    def spider_note(self, note_url: str, cookies_str: str, proxies=None):
        """
        爬取一个笔记的信息
        """
        note_info = None
        try:
            success, msg, note_info = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
            if success:
                note_info = note_info['data']['items'][0]
                note_info['url'] = note_url
                note_info = handle_note_info(note_info)

                # 调试用：只打印第一条 note 的完整结构
                if os.getenv("PRINT_ONE_NOTE", "0") == "1" and not self._printed_sample:
                    logger.info("Sample note_info: " + json.dumps(note_info, ensure_ascii=False)[:2000])
                    self._printed_sample = True

        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取笔记信息 {note_url}: {success}, msg: {msg}')
        return success, msg, note_info

    def spider_some_note(self, notes: list, cookies_str: str, base_path: dict,
                         save_choice: str, excel_name: str = '', proxies=None):
        """
        爬取一些笔记的信息，返回整理好的 note_info 列表
        """
        if (save_choice == 'all' or save_choice == 'excel') and excel_name == '':
            raise ValueError('excel_name 不能为空')

        note_info_list = []

        for note_url in notes:
            success, msg, note_info = self.spider_note(note_url, cookies_str, proxies)
            if note_info is not None and success:
                note_info_list.append(note_info)

        # 下载媒体（可选）
        for note_info in note_info_list:
            if save_choice == 'all' or 'media' in save_choice:
                download_note(note_info, base_path['media'], save_choice)

        # 保存到本地 excel（可选）
        if save_choice == 'all' or save_choice == 'excel':
            file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}.xlsx'))
            save_to_xlsx(note_info_list, file_path)

        return note_info_list

    def spider_user_all_note(self, user_url: str, cookies_str: str, base_path: dict,
                             save_choice: str, excel_name: str = '', proxies=None):
        """
        爬取一个用户的所有笔记，返回 note_info 列表
        """
        note_urls = []
        try:
            success, msg, all_note_info = self.xhs_apis.get_user_all_notes(user_url, cookies_str, proxies)
            if success:
                logger.info(f'用户 {user_url} 作品数量: {len(all_note_info)}')
                for simple_note_info in all_note_info:
                    note_url = f"https://www.xiaohongshu.com/explore/{simple_note_info['note_id']}?xsec_token={simple_note_info['xsec_token']}"
                    note_urls.append(note_url)

            if save_choice == 'all' or save_choice == 'excel':
                excel_name = user_url.split('/')[-1].split('?')[0]

            note_info_list = self.spider_some_note(note_urls, cookies_str, base_path,
                                                   save_choice, excel_name, proxies)
        except Exception as e:
            success = False
            msg = e
            note_info_list = []

        logger.info(f'爬取用户所有视频 {user_url}: {success}, msg: {msg}')
        return note_info_list, success, msg

    def spider_some_search_note(self, query: str, require_num: int, cookies_str: str,
                                base_path: dict, save_choice: str,
                                sort_type_choice=0, note_type=0, note_time=0,
                                note_range=0, pos_distance=0, geo: dict = None,
                                excel_name: str = '', proxies=None):
        """
        指定数量搜索笔记，返回 note_info 列表
        """
        note_urls = []
        note_info_list = []
        success = False
        msg = None

        try:
            success, msg, notes = self.xhs_apis.search_some_note(
                query, require_num, cookies_str,
                sort_type_choice, note_type, note_time,
                note_range, pos_distance, geo, proxies
            )

            if success:
                notes = list(filter(lambda x: x['model_type'] == "note", notes))
                logger.info(f'搜索关键词 {query} 笔记数量: {len(notes)}')
                for note in notes:
                    note_url = f"https://www.xiaohongshu.com/explore/{note['id']}?xsec_token={note['xsec_token']}"
                    note_urls.append(note_url)

            if save_choice == 'all' or save_choice == 'excel':
                excel_name = excel_name or query

            note_info_list = self.spider_some_note(note_urls, cookies_str, base_path,
                                                   save_choice, excel_name, proxies)

        except Exception as e:
            success = False
            msg = e
            note_info_list = []

        logger.info(f'搜索关键词 {query} 笔记: {success}, msg: {msg}')
        return note_info_list, success, msg


# ===== Google Sheet 配置 =====
SPREADSHEET_ID = "1uge_TtzAauHqKv7pNViQ6lJ1n6RkKwMWGHJAd92y2fE"
SERVICE_ACCOUNT_FILE = "service_account.json"

def write_to_google_sheet(note_list, sheet_name: str):
    """
    把 note_list 写入指定 sheet（tab）
    """
    if not note_list:
        logger.info(f"{sheet_name}: note_list 为空，跳过写入")
        return

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=scopes
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(sheet_name)

    header = [
        "title", "desc",
        "like_count", "collect_count", "comment_count", "share_count",
        "author", "author_id",
        "publish_time", "url"
    ]

    rows = []
    for n in note_list:
        rows.append([
            n.get("title", ""),
            (n.get("desc", "")[:100] + "...") if n.get("desc") else "",
            n.get("liked_count", ""),
            n.get("collected_count", ""),
            n.get("comment_count", ""),
            n.get("share_count", ""),
            n.get("nickname", ""),
            n.get("user_id", ""),
            n.get("time", n.get("created_time", "")),
            n.get("url", "")
        ])

    ws.clear()
    ws.append_rows([header] + rows, value_input_option="RAW")
    logger.info(f"{sheet_name}: 已写入 {len(rows)} 条笔记")


if __name__ == '__main__':
    cookies_str, base_path = init()
    data_spider = Data_Spider()

    # 品牌 & 对应 sheet 名
    BRANDS = [
        ("dtcpay", "dtcpay"),
        ("Revolut", "Revolut"),
        ("Wise", "Wise"),
        ("Redotpay", "Redotpay"),
        ("YouTrip", "YouTrip"),
        ("FOMOpay", "FOMOpay"),
    ]

    # 通用搜索参数
    query_num = 100
    save_choice = "excel" 
    sort_type_choice = 0
    note_type = 0
    note_time = 0
    note_range = 0
    pos_distance = 0

    for keyword, sheet_name in BRANDS:
        excel_name = f"{keyword}_xhs"

        note_list, success, msg = data_spider.spider_some_search_note(
            query=keyword,
            require_num=query_num,
            cookies_str=cookies_str,
            base_path=base_path,
            save_choice=save_choice,
            sort_type_choice=sort_type_choice,
            note_type=note_type,
            note_time=note_time,
            note_range=note_range,
            pos_distance=pos_distance,
            geo=None,
            excel_name=excel_name,
        )

        logger.info(
            f"[{sheet_name}] 搜索 {keyword} 完成，success={success}, msg={msg}, 爬到 {len(note_list)} 条笔记"
        )
        write_to_google_sheet(note_list, sheet_name)
