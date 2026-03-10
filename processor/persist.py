import argparse
import json
import os
import sys
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import ijson
except Exception:
    ijson = None

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from storage.db_client import MySQLClient


class JsonArrayWriter:
    def __init__(self, output_path: str):
        self.output_path = output_path
        self._fp = None
        self._first = True

    def __enter__(self):
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self._fp = open(self.output_path, "w", encoding="utf-8")
        self._fp.write("[\n")
        self._first = True
        return self

    def write_one(self, item: Dict[str, Any]) -> None:
        if not self._fp:
            raise RuntimeError("Writer not opened")
        if not self._first:
            self._fp.write(",\n")
        self._fp.write(json.dumps(item, ensure_ascii=False, indent=2))
        self._first = False

    def write_many(self, items: Iterable[Dict[str, Any]]) -> None:
        for item in items:
            self.write_one(item)

    def __exit__(self, exc_type, exc, tb):
        if self._fp:
            self._fp.write("\n]\n")
            self._fp.close()
            self._fp = None


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _as_float(value: Any, default: float = 0.5) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:
            return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return float(text)
        except Exception:
            return default
    return default


def _as_keywords(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for v in value:
            s = _as_str(v)
            if s:
                out.append(s)
        return out
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                data = json.loads(text)
                return _as_keywords(data)
            except Exception:
                pass
        parts = [p.strip() for p in text.replace("，", ",").split(",")]
        return [p for p in parts if p]
    return []


def _pick(record: Dict[str, Any], keys: Sequence[str]) -> Any:
    for k in keys:
        if k in record and record.get(k) not in (None, ""):
            return record.get(k)
    return None


def normalize_llm_record(record: Dict[str, Any]) -> Dict[str, Any]:
    url = _as_str(_pick(record, ["URL", "url"]))
    title = _as_str(_pick(record, ["标题", "title"]))
    date = _as_str(_pick(record, ["日期", "publish_date", "date"]))
    source = _as_str(_pick(record, ["来源", "source"]))
    content = _as_str(_pick(record, ["正文", "content", "content_summary", "summary"]))
    score = _as_float(_pick(record, ["sentiment_score", "score"]))
    keywords = _as_keywords(_pick(record, ["keywords", "关键词"]))

    out: Dict[str, Any] = dict(record)
    out["URL"] = url or out.get("URL", "")
    out["标题"] = title or out.get("标题", "")
    out["日期"] = date or out.get("日期", "")
    out["来源"] = source or out.get("来源", "")
    if content:
        out["正文"] = content
    out["sentiment_score"] = score
    out["keywords"] = keywords
    return out


def _dedupe_key(record: Dict[str, Any]) -> str:
    url = _as_str(_pick(record, ["URL", "url"]))
    if url:
        return f"url::{url}"
    title = _as_str(_pick(record, ["标题", "title"]))
    if title:
        return f"title::{title}"
    return ""


def merge_records(primary: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(primary)

    for k in ("URL", "标题", "日期", "来源", "正文"):
        if not _as_str(out.get(k)) and _as_str(incoming.get(k)):
            out[k] = incoming.get(k)

    if len(_as_str(out.get("正文"))) < len(_as_str(incoming.get("正文"))):
        if _as_str(incoming.get("正文")):
            out["正文"] = incoming.get("正文")

    out_score = _as_float(out.get("sentiment_score"), default=0.5)
    in_score = _as_float(incoming.get("sentiment_score"), default=0.5)
    if out.get("sentiment_score") in (None, ""):
        out["sentiment_score"] = in_score
    else:
        out["sentiment_score"] = max(out_score, in_score)

    out_kw = _as_keywords(out.get("keywords"))
    in_kw = _as_keywords(incoming.get("keywords"))
    if not out_kw and in_kw:
        out["keywords"] = in_kw
    else:
        out["keywords"] = out_kw

    return out


def iter_llm_files(input_path: str) -> List[str]:
    if os.path.isfile(input_path):
        return [input_path]
    paths: List[str] = []
    for root, _, filenames in os.walk(input_path):
        for name in filenames:
            low = name.lower()
            if low.endswith("_llm.json"):
                paths.append(os.path.join(root, name))
    return sorted(paths)


def iter_json_array(path: str) -> Iterable[Dict[str, Any]]:
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


def merge_dedupe_llm_files(input_path: str, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    files = iter_llm_files(input_path)
    seen: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []

    stats = {"files": len(files), "read": 0, "kept": 0, "deduped": 0, "skipped": 0}

    for fp in files:
        for item in iter_json_array(fp):
            if limit is not None and stats["read"] >= limit:
                break
            stats["read"] += 1
            normalized = normalize_llm_record(item)
            key = _dedupe_key(normalized)
            if not key:
                stats["skipped"] += 1
                continue
            if key in seen:
                seen[key] = merge_records(seen[key], normalized)
                stats["deduped"] += 1
            else:
                seen[key] = normalized
                order.append(key)
                stats["kept"] += 1
        if limit is not None and stats["read"] >= limit:
            break

    records = [seen[k] for k in order]
    return records, stats


def _parse_date(value: str) -> Optional[str]:
    text = _as_str(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text[:10], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    if len(text) >= 10 and text[4] in ("-", "/", ".") and text[7] in ("-", "/", "."):
        normalized = text[:10].replace("/", "-").replace(".", "-")
        try:
            datetime.strptime(normalized, "%Y-%m-%d")
            return normalized
        except Exception:
            return None
    return None


def _content_summary(record: Dict[str, Any], max_len: int = 800) -> str:
    summary = _as_str(_pick(record, ["content_summary", "summary"]))
    if summary:
        return summary[:max_len]
    content = _as_str(_pick(record, ["正文", "content"]))
    return content[:max_len] if content else ""


def to_mysql_rows(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rec in records:
        url = _as_str(_pick(rec, ["URL", "url"]))
        title = _as_str(_pick(rec, ["标题", "title"]))
        publish_date = _parse_date(_as_str(_pick(rec, ["日期", "publish_date", "date"])))  # yyyy-mm-dd
        source = _as_str(_pick(rec, ["来源", "source"]))
        sentiment = _as_float(_pick(rec, ["sentiment_score", "score"]), default=0.5)
        keywords = _as_keywords(_pick(rec, ["keywords", "关键词"]))
        out.append(
            {
                "url": url,
                "title": title,
                "publish_date": publish_date,
                "source": source,
                "content_summary": _content_summary(rec),
                "sentiment_score": sentiment,
                "keywords": json.dumps(keywords, ensure_ascii=False),
            }
        )
    return out


def create_table_if_needed(client: MySQLClient, table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(500),
        title VARCHAR(500),
        publish_date DATE,
        source VARCHAR(100),
        content_summary TEXT,
        sentiment_score DECIMAL(4,3),
        keywords JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4
    """
    client.execute(sql)


def create_word_frequency_table_if_needed(client: MySQLClient, table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        word VARCHAR(200) NOT NULL,
        count INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4
    """
    client.execute(sql)


def load_word_frequency(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        word = _as_str(item.get("word"))
        if not word:
            continue
        try:
            cnt = int(item.get("count", 0))
        except Exception:
            cnt = 0
        out.append({"word": word, "count": cnt})
    return out


def chunked(seq: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def run() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    default_input = os.path.join(project_root, "data", "clean")
    default_output = os.path.join(project_root, "data", "clean", "merged_llm_dedup.json")
    default_wordfreq_path = os.path.join(project_root, "data", "stats", "word_frequency_top.json")

    parser = argparse.ArgumentParser(description="LLM 结果去重合并并批量写入 MySQL")
    parser.add_argument("--input", default=default_input, help="输入目录或文件（*_llm.json）")
    parser.add_argument("--output", default=default_output, help="去重合并后的输出 JSON 文件")
    parser.add_argument("--limit", type=int, default=None, help="限制处理条数（测试用）")
    parser.add_argument("--only-dedupe", action="store_true", help="只做去重合并，不入库")
    parser.add_argument("--import-wordfreq", action="store_true", help="导入词频 JSON 到 MySQL")
    parser.add_argument("--wordfreq-path", default=default_wordfreq_path, help="词频 JSON 文件路径")
    parser.add_argument("--wordfreq-table", default=os.getenv("MYSQL_WORDFREQ_TABLE", "word_frequency_stats"), help="词频表名")

    parser.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST"), help="MySQL Host")
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.getenv("MYSQL_USER"), help="MySQL User")
    parser.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD"), help="MySQL Password")
    parser.add_argument("--mysql-db", default=os.getenv("MYSQL_DB"), help="MySQL Database")
    parser.add_argument("--mysql-table", default=os.getenv("MYSQL_TABLE", "land_news_analysis"), help="MySQL Table")
    parser.add_argument("--create-table", action="store_true", help="不存在则建表")
    parser.add_argument("--batch-size", type=int, default=1000, help="executemany 批大小")
    parser.add_argument("--dry-run", action="store_true", help="只打印统计信息，不写入数据库")

    args = parser.parse_args()

    if args.import_wordfreq:
        if not os.path.exists(args.wordfreq_path):
            raise RuntimeError(f"词频文件不存在: {args.wordfreq_path}")
        rows = load_word_frequency(args.wordfreq_path)
        if args.dry_run:
            print(f"待入库行数: {len(rows)}")
            if rows:
                print("示例数据:")
                print(json.dumps(rows[0], ensure_ascii=False, indent=2))
            return
        if not (args.mysql_host and args.mysql_user and args.mysql_password and args.mysql_db):
            raise RuntimeError("缺少 MySQL 连接参数：--mysql-host/--mysql-user/--mysql-password/--mysql-db")
        client = MySQLClient(
            host=args.mysql_host,
            user=args.mysql_user,
            password=args.mysql_password,
            db=args.mysql_db,
            port=args.mysql_port,
        )
        try:
            if args.create_table:
                create_word_frequency_table_if_needed(client, args.wordfreq_table)
            columns = ["word", "count"]
            inserted = 0
            for part in chunked(rows, max(1, args.batch_size)):
                inserted += client.insert_many(args.wordfreq_table, part, columns)
            print(f"MySQL 入库完成: table={args.wordfreq_table} inserted={inserted}")
        finally:
            client.close()
        return

    records, stats = merge_dedupe_llm_files(args.input, limit=args.limit)
    with JsonArrayWriter(args.output) as writer:
        writer.write_many(records)

    print(
        f"合并完成: files={stats['files']} read={stats['read']} kept={stats['kept']} "
        f"deduped={stats['deduped']} skipped={stats['skipped']}"
    )
    print(f"输出文件: {args.output}")

    if args.only_dedupe:
        return

    rows = to_mysql_rows(records)
    if args.dry_run:
        sample = rows[0] if rows else None
        print(f"待入库行数: {len(rows)}")
        if sample:
            print("示例数据:")
            print(json.dumps(sample, ensure_ascii=False, indent=2))
        return

    if not (args.mysql_host and args.mysql_user and args.mysql_password and args.mysql_db):
        print("未提供 MySQL 连接参数，已跳过入库。")
        print("如只想合并去重：运行 python .\\processor\\persist.py --only-dedupe")
        print("如要写入 MySQL：请提供 --mysql-host/--mysql-user/--mysql-password/--mysql-db，或设置环境变量 MYSQL_HOST 等。")
        return

    client = MySQLClient(
        host=args.mysql_host,
        user=args.mysql_user,
        password=args.mysql_password,
        db=args.mysql_db,
        port=args.mysql_port,
    )
    try:
        if args.create_table:
            create_table_if_needed(client, args.mysql_table)
        columns = ["url", "title", "publish_date", "source", "content_summary", "sentiment_score", "keywords"]
        inserted = 0
        for part in chunked(rows, max(1, args.batch_size)):
            inserted += client.insert_many(args.mysql_table, part, columns)
        print(f"MySQL 入库完成: inserted={inserted}")
    finally:
        client.close()


if __name__ == "__main__":
    run()
