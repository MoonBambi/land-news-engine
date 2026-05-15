# AGENTS.md

## Project Identity

This is a graduation thesis project (江苏理工学院) — **data production end** of a rural land-transfer news analysis system. The companion **data application end** is at `E:\Terminal_helper` (Electron + Vue desktop app). Both share a MySQL database.

## Key Scripts & Execution Order

**Data pipeline** (must run in this order):
```
python .\processor\cleaner.py          # raw HTML → structured JSON
python .\processor\analyzer.py --model qwen-flash --async --stream --concurrency 100 --batch-size 50
python .\processor\extractor.py        # Jieba word freq
python .\processor\persist.py --create-table --mysql-host ... --mysql-user ... --mysql-password ... --mysql-db land_news
```

**Thesis writing pipeline**:
```
# 1. Edit the source-of-truth markdown
edit 序言.md

# 2. Sync markdown content into Word document
python .\insert_content_to_docx.py --input 初稿0.1.docx --output 初稿0.x.docx
```

`_replace_chapters.py` is the older version (heading-only replacement), deprecated in favor of `insert_content_to_docx.py` which also inserts body text.

## Architecture

- **Scrapy**: Not a full project (no `scrapy.cfg`). Each spider runs via `scrapy runspider`. Dual strategy exists — `*_scrapy_spider.py` (Scrapy) + standalone `*_spider.py` (requests + ThreadPoolExecutor), but thesis only describes Scrapy.
- **main.py**: Stub only, no pipeline orchestration.
- **Data flow**: `data/raw/` → `data/clean/` → `data/stats/` → MySQL (`land_news_analysis`, `word_frequency_stats`)
- **Dependencies**: `SnowNLP` and `pymongo` are in requirements.txt but **not used** by any code. Do not reference them.

## Environment

- `DASHSCOPE_API_KEY` env var required before running analyzer
- MySQL defaults in `config/settings.py` (localhost / root / password / land_news) — change for real use
- Windows PowerShell is the expected shell; use `` ` `` as escape character, not `\`

## Writing Rules for This Codebase

1. **序言.md is the canonical thesis source**. All chapter content lives there. Never write directly into docx.
2. Thesis uses **Chinese academic prose style**. Keep technical descriptions precise but avoid verbose English terms — prefer `Qwen 大模型` over `阿里云 DashScope 平台提供的通义千问（Qwen）大语言模型 API`.
3. Core project chapters (4 & 5) should be kept detailed; theory chapters (1-3) and wrap-up (6-7) should be concise.
4. Placeholders for manual work use the format `【待填写】` (data) or `【待插入：描述】` (diagrams).
5. Reference citations use `[1]`, `[2]` inline format; the reference list is at the end of 序言.md.

## Files to Ignore

- `data/`, `logs/`, `venv/` are gitignored runtime artifacts
- `diagnose_docx.py`, `trim_text*.py` are one-off utility scripts, not part of the project
- `_replace_chapters.py` is legacy, use `insert_content_to_docx.py` instead
