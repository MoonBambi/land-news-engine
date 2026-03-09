import argparse
import json
import os
import re
from lxml import html


class DataCleaner:
    def __init__(self, input_dir, output_dir, limit=None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.limit = limit

    def clean_text(self, text):
        if not text:
            return ""
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def extract_first(self, tree, xpaths):
        for xp in xpaths:
            value = tree.xpath(xp)
            if isinstance(value, list):
                value = value[0] if value else ""
            if value:
                return self.clean_text(str(value))
        return ""

    def extract_text(self, tree, xpaths):
        for xp in xpaths:
            texts = tree.xpath(xp)
            if texts:
                joined = self.clean_text(" ".join(texts))
                if joined:
                    return joined
        return ""

    def extract_date_from_url(self, url):
        if not url:
            return ""
        match = re.search(r"/(\d{4})(\d{2})/(\d{2})/", url)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        match = re.search(r"t(\d{8})_", url)
        if match:
            raw = match.group(1)
            return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
        return ""

    def parse_html(self, html_text, url):
        if not html_text:
            return {"标题": "", "日期": "", "来源": "", "正文": ""}
        tree = html.fromstring(html_text)
        for node in tree.xpath("//script|//style"):
            node.drop_tree()

        title = self.extract_first(
            tree,
            [
                '//meta[@name="ArticleTitle"]/@content',
                '//meta[@property="og:title"]/@content',
                "//title/text()",
            ],
        )
        date = self.extract_first(
            tree,
            [
                '//meta[@name="publishdate"]/@content',
                '//meta[@name="PubDate"]/@content',
                '//meta[@property="article:published_time"]/@content',
            ],
        )
        if not date:
            date = self.extract_date_from_url(url)
        source = self.extract_first(
            tree,
            [
                '//meta[@name="source"]/@content',
                '//meta[@name="ContentSource"]/@content',
            ],
        )
        content = self.extract_text(
            tree,
            [
                '//div[@class="TRS_Editor"]//text()',
                '//div[@id="Content"]//text()',
                '//div[@class="content"]//text()',
                "//article//text()",
            ],
        )

        return {"标题": title, "日期": date, "来源": source, "正文": content}

    def clean_item(self, item):
        url = item.get("URL", "")
        html_text = item.get("内容", "")
        extracted = self.parse_html(html_text, url)
        cleaned = {
            "序号": item.get("序号", ""),
            "URL": url,
            "标题": extracted["标题"],
            "日期": extracted["日期"],
            "来源": extracted["来源"],
            "正文": extracted["正文"],
        }
        return cleaned

    def iter_input_files(self):
        files = []
        for root, _, filenames in os.walk(self.input_dir):
            for name in filenames:
                if name.lower().endswith(".json"):
                    files.append(os.path.join(root, name))
        return sorted(files)

    def load_json(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_json(self, records, output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

    def run(self):
        if not os.path.exists(self.input_dir):
            return []
        input_files = self.iter_input_files()
        results = []
        for path in input_files:
            data = self.load_json(path)
            cleaned = []
            for idx, item in enumerate(data):
                if self.limit is not None and idx >= self.limit:
                    break
                cleaned.append(self.clean_item(item))
            base = os.path.splitext(os.path.basename(path))[0]
            output_path = os.path.join(self.output_dir, f"{base}_clean.json")
            self.save_json(cleaned, output_path)
            results.append(output_path)
        return results


def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_input = os.path.join(project_root, "data", "raw")
    default_output = os.path.join(project_root, "data", "clean")

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=default_input)
    parser.add_argument("--output", default=default_output)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    cleaner = DataCleaner(args.input, args.output, args.limit)
    cleaner.run()


if __name__ == "__main__":
    main()
