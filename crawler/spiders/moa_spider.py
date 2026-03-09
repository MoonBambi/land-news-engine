import time
import requests
import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path for standalone execution
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from config.moa_settings import MOA_HEADERS, MOA_SEARCH_URL, MOA_KEYWORDS

class MoaSpider:
    def __init__(self):
        self.headers = MOA_HEADERS
        self.search_url = MOA_SEARCH_URL
        self.keywords = MOA_KEYWORDS
        self.data_url = []
        self.records = []
        self.output_dir = os.path.join(project_root, 'data')
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def fetch_list(self, pages=7):
        """
        收集所有 URL
        """
        print('正在获取列表信息，请稍等。。。。。。')
        for k in range(1, pages + 1):
            for keyword in self.keywords:
                print(f"正在获取 {keyword} 第 {k} 页")
                # 适当减少列表页的休眠时间，因为请求量相对较少
                time.sleep(1)

                data = {
                    'siteCode': 'zcggs_moa',
                    'tab': 'all',
                    'qt': keyword,
                    'keyPlace': '0',
                    'sort': 'relevance',
                    'fileType': '',
                    'timeOption': '0',
                    'page': str(k),
                    'pageSize': '20',
                    'ie': 'ffb7761c-70f6-4221-8b62-4bffa82d3b07',
                }
                try:
                    response = requests.post(self.search_url, headers=self.headers, data=data, timeout=10)
                    result = response.json()
                    docs = result.get('resultDocs', [])
                    for item in docs:
                        url = item.get('data', {}).get('url')
                        if url:
                            self.data_url.append(url)
                except Exception as e:
                    print(f"第 {k} 页处理失败: {e}")

        # 去重 URL（保留首次出现的顺序）
        self.data_url = list(dict.fromkeys(self.data_url))
        print(f"总共收集到 {len(self.data_url)} 个不重复的 URL")

    def fetch_single_detail(self, url, idx):
        """
        抓取单个 URL 内容的任务函数
        """
        print(f"正在抓取第 {idx} 个: {url}")
        content_text = ""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            content_type = resp.headers.get('Content-Type', '').lower()

            if 'text/html' in content_type or 'text/plain' in content_type:
                resp.encoding = resp.apparent_encoding or 'utf-8'
                content_text = resp.text
            else:
                content_text = f"[非文本文件，类型：{content_type}]"
        except Exception as e:
            content_text = f"抓取失败: {e}"
            print(f"抓取失败: {url}, 错误: {e}")
        
        return {
            '序号': idx,
            'URL': url,
            '内容': content_text
        }

    def fetch_details(self):
        """
        使用多线程并发抓取内容
        """
        print(f"开始并发抓取 {len(self.data_url)} 个详情页...")
        
        # 使用 ThreadPoolExecutor 进行并发抓取
        # max_workers=10 表示最多同时有 10 个线程在抓取，可以显著提高速度
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {
                executor.submit(self.fetch_single_detail, url, idx): idx 
                for idx, url in enumerate(self.data_url, start=1)
            }
            
            for future in as_completed(future_to_url):
                try:
                    result = future.result()
                    self.records.append(result)
                except Exception as e:
                    print(f"线程执行异常: {e}")

        # 按序号重新排序，保证结果顺序一致
        self.records.sort(key=lambda x: x['序号'])

    def process_and_save(self, filename='农业农村部_完整版.json'):
        """
        保存为 JSON 文件
        """
        if not self.records:
            print("没有数据可保存")
            return

        try:
            output_path = os.path.join(self.output_dir, filename)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.records, f, ensure_ascii=False, indent=4)
            print(f"已保存到 {output_path}")
        except Exception as e:
            print(f"保存 JSON 失败: {e}")

    def run(self, pages=7):
        self.fetch_list(pages=pages)
        self.fetch_details()
        self.process_and_save()

if __name__ == "__main__":
    spider = MoaSpider()
    # 测试运行，仅抓取 1 页
    spider.run(pages=1)
