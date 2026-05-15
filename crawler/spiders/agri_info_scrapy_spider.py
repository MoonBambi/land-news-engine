import os
import sys
import time
from typing import Set
from urllib.parse import urlencode

import scrapy

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from config.agri_settings import AGRI_BASE_URL, AGRI_COOKIES, AGRI_HEADERS, AGRI_KEYWORDS

LOG_DIR = os.path.join(project_root, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


class AgriInfoScrapySpider(scrapy.Spider):
    name = "agri_info_scrapy"
    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "CONCURRENT_REQUESTS": 16,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",
        "LOG_FILE": os.path.join(LOG_DIR, f"agri_info_scrapy_{time.strftime('%Y%m%d_%H%M%S')}.log"),
        "LOG_FORMAT": "%(asctime)s %(levelname)s %(message)s",
    }

    def __init__(self, pages=3, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pages = int(pages)
        self.base_url = AGRI_BASE_URL
        self.cookies = AGRI_COOKIES
        self.headers = AGRI_HEADERS
        self.keywords = AGRI_KEYWORDS
        self._seen_urls: Set[str] = set()
        self._detail_count = 0

        self.logger.info("=" * 50)
        self.logger.info(" Scrapy 模式日志已启动")
        self.logger.info(f" 关键词: {self.keywords}")
        self.logger.info(f" 每关键词抓取页数: {self.pages}")
        self.logger.info("=" * 50)

    def start_requests(self):
        for i in range(self.pages):
            for keyword in self.keywords:
                self.logger.info(f"正在获取第 {i+1} 页（每页 10 条，关键词：{keyword}）")
                params = {
                    "channelid": "211475",
                    "keyword": keyword,
                    "perpage": "10",
                    "page": str(i),
                    "orderby": "-docreltime",
                }
                url = f"{self.base_url}?{urlencode(params)}"
                yield scrapy.Request(
                    url=url,
                    cookies=self.cookies,
                    headers=self.headers,
                    callback=self.parse_list,
                    cb_kwargs={"keyword": keyword, "page_no": i + 1},
                    dont_filter=True,
                )

    def parse_list(self, response, keyword: str, page_no: int):
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"列表解析失败: keyword={keyword} page={page_no} err={e}")
            return
        items = data.get("items", [])
        self.logger.info(f" 第 {page_no} 页解析完成，获取到 {len(items)} 条记录")
        for item in items:
            url = item.get("docpuburl")
            if not url or url in self._seen_urls:
                continue
            self._seen_urls.add(url)
            self._detail_count += 1
            self.logger.info(f"  [{self._detail_count}] 发现详情页: {url}")
            yield scrapy.Request(
                url=url,
                cookies=self.cookies,
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
            "来源": "农业农村信息网",
        }
