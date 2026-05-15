import os
import sys
import time
from typing import Set

import scrapy

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from config.moa_settings import MOA_HEADERS, MOA_KEYWORDS, MOA_SEARCH_URL

LOG_DIR = os.path.join(project_root, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


class MoaScrapySpider(scrapy.Spider):
    name = "moa_scrapy"
    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "CONCURRENT_REQUESTS": 16,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",
        "LOG_FILE": os.path.join(LOG_DIR, f"moa_scrapy_{time.strftime('%Y%m%d_%H%M%S')}.log"),
        "LOG_FORMAT": "%(asctime)s %(levelname)s %(message)s",
    }

    def __init__(self, pages=7, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pages = int(pages)
        self.search_url = MOA_SEARCH_URL
        self.headers = MOA_HEADERS
        self.keywords = MOA_KEYWORDS
        self._seen_urls: Set[str] = set()
        self._detail_count = 0

        self.logger.info("=" * 50)
        self.logger.info(" Scrapy 模式日志已启动")
        self.logger.info(f" 关键词: {self.keywords}")
        self.logger.info(f" 每关键词抓取页数: {self.pages}")
        self.logger.info("=" * 50)

    def start_requests(self):
        for k in range(1, self.pages + 1):
            for keyword in self.keywords:
                self.logger.info(f"正在获取第 {k} 页（每页 20 条，关键词：{keyword}）")
                form_data = {
                    "siteCode": "zcggs_moa",
                    "tab": "all",
                    "qt": keyword,
                    "keyPlace": "0",
                    "sort": "relevance",
                    "fileType": "",
                    "timeOption": "0",
                    "page": str(k),
                    "pageSize": "20",
                    "ie": "ffb7761c-70f6-4221-8b62-4bffa82d3b07",
                }
                yield scrapy.FormRequest(
                    url=self.search_url,
                    headers=self.headers,
                    formdata=form_data,
                    callback=self.parse_list,
                    cb_kwargs={"keyword": keyword, "page_no": k},
                    dont_filter=True,
                )

    def parse_list(self, response, keyword: str, page_no: int):
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"列表解析失败: keyword={keyword} page={page_no} err={e}")
            return
        docs = data.get("resultDocs", [])
        self.logger.info(f" 第 {page_no} 页解析完成，获取到 {len(docs)} 条记录")
        for item in docs:
            url = item.get("data", {}).get("url")
            if not url or url in self._seen_urls:
                continue
            self._seen_urls.add(url)
            self._detail_count += 1
            self.logger.info(f"  [{self._detail_count}] 发现详情页: {url}")
            yield scrapy.Request(
                url=url,
                headers=self.headers,
                callback=self.parse_detail,
                cb_kwargs={"url": url, "idx": self._detail_count},
                dont_filter=True,
            )
        self.logger.info(f" 当前累计发现 {len(self._seen_urls)} 个不重复 URL")

    def parse_detail(self, response, url: str, idx: int):
        self.logger.info(f"  [{idx}] 抓取完成: {url}")
        yield {
            "URL": url,
            "内容": response.text,
            "来源": "农业农村部",
        }
