# Land News Engine

## 简介
Land News Engine 是一个面向农村土地流转与农业政策新闻的数据采集与分析工程。项目包含爬虫采集、数据清洗、LLM 分析、词频统计与 MySQL 入库的完整流程，支持异步并发与大文件流式处理，适合批量处理多来源新闻数据。

## 项目结构
```
land-news-engine/
├─ crawler/                爬虫模块（多来源采集）
├─ config/                 配置与日志
├─ processor/              数据处理与分析
├─ storage/                数据库/存储相关
├─ data/
│  ├─ raw/                 原始采集数据
│  ├─ clean/               清洗后数据 + LLM 结果
│  └─ stats/               统计结果（如词频）
├─ requirements.txt        依赖列表
├─ main.py                 入口（预留）
└─ README.md               项目说明
```

## 核心功能
- 多来源新闻采集与关键词抓取
- 数据清洗与结构化输出
- LLM 情感分析与关键词抽取
- 异步并发与流式读取的大规模处理
- 词频统计与停用词过滤
- MySQL 批量入库（executemany）

## 模块划分
- crawler/：各站点爬虫脚本与中间件
- config/：关键词、站点参数与日志配置
- processor/
  - cleaner.py：清洗 raw 数据生成 clean 数据
  - analyzer.py：调用 LLM 进行情感分析与关键词抽取
  - extractor.py：对 clean 数据正文做全量分词词频统计
  - persist.py：去重合并、MySQL 入库、词频入库
- storage/：MySQL 客户端封装与数据模型

## 安装指南
1. 创建虚拟环境并安装依赖
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r .\requirements.txt
```

2. 数据清洗（raw → clean）
```powershell
python .\processor\cleaner.py
```

3. LLM 分析（clean → clean_llm）
```powershell
python .\processor\analyzer.py --async --stream --concurrency 100 --batch-size 50
```

4. 词频统计（输出到 data/stats）
```powershell
python .\processor\extractor.py
```

5. 去重合并并写入 MySQL
```powershell
python .\processor\persist.py --create-table ^
  --mysql-host 127.0.0.1 --mysql-port 3306 ^
  --mysql-user root --mysql-password your_password ^
  --mysql-db land_news
```

6. 词频结果写入 MySQL
```powershell
python .\processor\persist.py --import-wordfreq --create-table ^
  --mysql-host 127.0.0.1 --mysql-port 3306 ^
  --mysql-user root --mysql-password your_password ^
  --mysql-db land_news
```
