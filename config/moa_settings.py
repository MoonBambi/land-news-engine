# 农业农村部爬虫配置

MOA_HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'zh-CN,zh;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://zcggs.moa.gov.cn',
    'referer': 'https://zcggs.moa.gov.cn/',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

MOA_SEARCH_URL = 'https://api.so-gov.cn/query/s'

MOA_KEYWORDS = ['宅基地改革', '土地确权', '经营权流转', '承包权', '土地流转']

# 每个单元格最多保存字符数
MAX_SEG_LEN = 30000
