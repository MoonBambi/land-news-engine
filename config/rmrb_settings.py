# 人民日报/中国日报爬虫配置

RMRB_COOKIES = {
    'wdcid': '70e20dd9946244d5',
    'sajssdk_2015_cross_new_user': '1',
    'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219cc7ee2c7d1fc1-000717f313bc4b924-26061c51-1327104-19cc7ee2c7e172a%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24metas%22%3A%7B%22articaltype%22%3A%22COMPO%22%2C%22filetype%22%3A%221%22%2C%22publishedtype%22%3A%221%22%2C%22pagetype%22%3A%221%22%2C%22catalogs%22%3A%22673fe8aaa310b59111da4dda%22%2C%22contentid%22%3A%22WS69804dd7a310942cc499dd11%22%2C%22publishdate%22%3A%222026-02-02%22%2C%22editor%22%3A%22%E9%BB%84%E5%87%8C%E7%9D%BF%22%2C%22author%22%3A%22%E9%BB%84%E5%87%8C%E7%9D%BF%22%2C%22source%22%3A%22%E5%8D%8A%E5%B2%9B%E7%BD%91%22%7D%2C%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTljYzdlZTJjN2QxZmMxLTAwMDcxN2YzMTNiYzRiOTI0LTI2MDYxYzUxLTEzMjcxMDQtMTljYzdlZTJjN2UxNzJhIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219cc7ee2c7d1fc1-000717f313bc4b924-26061c51-1327104-19cc7ee2c7e172a%22%7D',
    'sensorsdata2015jssdksession': '%7B%22session_id%22%3A%2219cc7ee2c88e2e01aa7938f58906626061c51132710419cc7ee2c8920e0%22%2C%22first_session_time%22%3A1772880800904%2C%22latest_session_time%22%3A1772881640865%7D',
}

RMRB_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://newssearch.chinadaily.com.cn/cn/search?query=%E5%9C%9F%E5%9C%B0%E6%B5%81%E8%BD%AC',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

RMRB_SEARCH_URL = 'https://newssearch.chinadaily.com.cn/rest/cn/search'

RMRB_KEYWORDS = ['宅基地改革', '土地确权', '经营权流转', '承包权', '土地流转']
