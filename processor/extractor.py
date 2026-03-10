# 关键词、地区、流转面积等关键信息提取
import argparse
import json
import os
import re
from collections import Counter
from typing import Dict, Iterable, List, Optional

import jieba
import jieba.analyse

try:
    import ijson
except Exception:
    ijson = None


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


_DEFAULT_STOPWORDS = {
    "的",
    "了",
    "在",
    "是",
    "和",
    "与",
    "及",
    "或",
    "对",
    "等",
    "为",
    "将",
    "把",
    "被",
    "也",
    "就",
    "都",
    "而",
    "并",
    "及其",
    "以及",
    "通过",
    "进行",
    "开展",
    "推进",
    "促进",
    "加强",
    "进一步",
    "持续",
    "不断",
    "有关",
    "相关",
    "工作",
    "方面",
    "问题",
    "这个",
    "这些",
    "一个",
    "我们",
    "你们",
    "他们",
    "她们",
    "它们",
    "其中",
    "同时",
    "目前",
    "今年",
    "近日",
    "此次",
    "表示",
    "记者",
    "报道",
    "来源",
    "内容",
    "原标题",
    "责任编辑",
}


def _iter_clean_files(input_path: str) -> List[str]:
    if os.path.isfile(input_path):
        return [input_path]
    paths: List[str] = []
    for root, _, filenames in os.walk(input_path):
        for name in filenames:
            low = name.lower()
            if low.endswith("_clean.json") and not low.endswith("_llm.json"):
                paths.append(os.path.join(root, name))
    return sorted(paths)


def _iter_json_array_items(path: str) -> Iterable[Dict]:
    if ijson:
        with open(path, "rb") as f:
            for item in ijson.items(f, "item"):
                if isinstance(item, dict):
                    yield item
        return
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item


def _load_stopwords(path: Optional[str]) -> set:
    if not path:
        return set(_DEFAULT_STOPWORDS)
    stopwords = set(_DEFAULT_STOPWORDS)
    if not os.path.exists(path):
        return stopwords
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            word = line.strip()
            if word:
                stopwords.add(word)
    return stopwords


_TOKEN_RE = re.compile(r"[\u4e00-\u9fff]+|[a-zA-Z0-9]+")


def _tokenize(text: str) -> Iterable[str]:
    for token in jieba.cut(text, cut_all=False):
        token = token.strip()
        if not token:
            continue
        if _TOKEN_RE.fullmatch(token) is None:
            continue
        yield token


def build_word_frequency(
    input_path: str,
    stopwords_path: Optional[str] = None,
    min_len: int = 2,
    limit: Optional[int] = None,
) -> Counter:
    stopwords = _load_stopwords(stopwords_path)
    counter: Counter = Counter()

    processed = 0
    for fp in _iter_clean_files(input_path):
        for item in _iter_json_array_items(fp):
            if limit is not None and processed >= limit:
                return counter
            content = item.get("正文") or item.get("content") or ""
            if not isinstance(content, str):
                content = str(content)
            for tok in _tokenize(content):
                if len(tok) < min_len:
                    continue
                if tok in stopwords:
                    continue
                counter[tok] += 1
            processed += 1
    return counter


def _save_word_frequency(counter: Counter, output_dir: str, top: int) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "word_frequency_top.json")
    items = [{"word": w, "count": int(c)} for w, c in counter.most_common(top)]
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    return output_path


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    default_input = os.path.join(project_root, "data", "clean")
    default_output = os.path.join(project_root, "data", "stats")

    parser = argparse.ArgumentParser(description="从 clean.json 正文提取全量词频（jieba）")
    parser.add_argument("--input", default=default_input, help="输入目录或文件（*_clean.json）")
    parser.add_argument("--output", default=default_output, help="输出目录（新文件夹）")
    parser.add_argument("--stopwords", default=None, help="停用词文件（每行一个词）")
    parser.add_argument("--min-len", type=int, default=2, help="最短词长度")
    parser.add_argument("--top", type=int, default=300, help="输出 TopN 词频")
    parser.add_argument("--limit", type=int, default=None, help="限制处理条数（测试用）")
    args = parser.parse_args()

    counter = build_word_frequency(
        input_path=args.input,
        stopwords_path=args.stopwords,
        min_len=max(1, args.min_len),
        limit=args.limit,
    )
    output_path = _save_word_frequency(counter, args.output, max(1, args.top))
    print(f"词频统计完成，输出: {output_path}")


if __name__ == "__main__":
    main()
