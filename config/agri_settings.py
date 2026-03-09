# 农业农村信息网爬虫配置

AGRI_COOKIES = {
    'JSESSIONID': '2C085EE2E80ABA3794F9DE77378EE868',
    'https_waf_cookie': '8b78dd1d-615f-4af3d1660cfbb5fc9e1525ea97b1ee024e68',
    'wdcid': '3e15bb37aeca6bbc',
    'wdlast': '1772883373',
    'wdses': '3f9256ca34308269',
}

AGRI_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://www.agri.cn/search/?searchWord=%E5%9C%9F%E5%9C%B0%E6%B5%81%E8%BD%AC&pageNo=2&pageSize=10&orderby=-docreltime',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

AGRI_BASE_URL = 'https://www.agri.cn/was5/web/search'

AGRI_KEYWORDS = ['宅基地改革', '土地确权', '经营权流转', '承包权', '土地流转']
