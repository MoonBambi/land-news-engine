# 关键词、地区、流转面积等关键信息提取
import jieba

class InfoExtractor:
    def extract_keywords(self, text):
        # TODO: Extract keywords using Jieba or other methods
        return jieba.analyse.extract_tags(text, topK=10)

    def extract_location(self, text):
        # TODO: Extract location information
        pass

    def extract_area(self, text):
        # TODO: Extract land area
        pass
