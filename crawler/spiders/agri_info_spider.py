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

from config.agri_settings import AGRI_COOKIES, AGRI_HEADERS, AGRI_BASE_URL

class AgriInfoSpider:
    def __init__(self):
        self.cookies = AGRI_COOKIES
        self.headers = AGRI_HEADERS
        self.base_url = AGRI_BASE_URL
        self.data_url = []
        self.records = []
        self.output_dir = os.path.join(project_root, 'data')
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def fetch_list(self, pages=3):
        """
        获取文章列表 URL
        :param pages: 抓取页数
        """
        print('正在获取列表信息，请稍等。。。。。。')
        for i in range(0, pages):
            print(f"正在获取第 {(i+1)*10} 条信息")
            time.sleep(1)
            
            params = {
                'channelid': '211475',
                'keyword': '土地流转',
                'perpage': '10',
                'page': str(i),
                'orderby': '-docreltime',
            }
            
            try:
                response = requests.get(self.base_url, params=params, cookies=self.cookies, headers=self.headers, timeout=10)
                data = response.json()
                content = data.get('items', [])
                for item in content:
                    url = item.get('docpuburl')
                    if url:
                        self.data_url.append(url)
            except Exception as e:
                print(f"第 {i} 页处理失败: {e}")
        
        # 去重 URL
        self.data_url = list(dict.fromkeys(self.data_url))
        print(f"总共收集到 {len(self.data_url)} 个不重复的 URL")

    def fetch_single_detail(self, url, idx):
        """
        抓取单个 URL 内容的任务函数
        """
        print(f"正在抓取第 {idx} 个: {url}")
        content_text = ""
        try:
            resp = requests.get(url, cookies=self.cookies, headers=self.headers, timeout=10)
            resp.encoding = 'utf-8'
            content_text = resp.text
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

        # 按序号重新排序
        self.records.sort(key=lambda x: x['序号'])

    def process_and_save(self, filename='农业农村信息网文章内容.json'):
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

    def run(self, pages=201):
        self.fetch_list(pages=pages)
        self.fetch_details()
        self.process_and_save()

if __name__ == "__main__":
    # 手动运行测试
    spider = AgriInfoSpider()
    # 为了测试方便，默认只抓取少量页面，实际使用可调整 run 参数
    spider.run(pages=3)
