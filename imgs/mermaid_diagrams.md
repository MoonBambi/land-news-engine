# Mermaid 图代码

说明：
- 本文件用于集中存放可在 `https://mermaid.live/` 渲染的论文插图代码。
- 各图已按图号分节标注。
- `图 3-1` 已有成品图片，位于 `imgs/3-1.png`。
- `图 3-2` 已有成品图片，位于 `imgs/3-2.png`。
- 第 5 章界面截图类图片和 `图 4-7` 终端运行截图不适合 Mermaid，建议直接截图。

## 图 2-1 Scrapy 五大组件架构图

```mermaid
flowchart LR
    U[用户启动 Spider] --> E[引擎 Engine]
    E --> S[调度器 Scheduler]
    S --> D[下载器 Downloader]
    D --> M[下载器中间件]
    M --> W[目标网站 Web]
    W --> M
    M --> D
    D --> E
    E --> P[爬虫 Spider]
    P --> E
    E --> I[项目管道 Item Pipeline]
    I --> O[JSON / 数据存储]

    P -.生成 Request.-> E
    E -.分发 Request.-> S
    W -.返回 Response.-> D
    P -.提取 Item.-> I
```

## 图 2-2 并发采集与容错机制层次图

```mermaid
flowchart TD
    A[Twisted 异步事件循环] --> B[并发请求调度]
    B --> C[RetryMiddleware 自动重试]
    C --> D[Spider try/except 异常捕获]
    D --> E[DOWNLOAD_DELAY 请求限速]
    E --> F[稳定采集输出]

    C -.失败请求重试.-> B
    D -.解析异常记录日志.-> F
```

## 图 2-3 HTML 文本清洗流程图

```mermaid
flowchart TD
    A[raw HTML] --> B[lxml DOM 解析]
    B --> C[移除 script / style]
    C --> D[XPath 提取标题]
    D --> E[XPath 提取日期]
    E --> F[XPath 提取来源]
    F --> G[XPath 提取正文]
    G --> H[clean_text 文本规范化]
    H --> I[结构化 JSON 输出]
```

## 图 2-4 两阶段去重策略示意图

```mermaid
flowchart TD
    subgraph S1[采集阶段]
        A1[搜索结果 URL] --> A2[_seen_urls 集合]
        A2 --> A3[过滤重复 URL]
        A3 --> A4[详情页抓取]
    end

    subgraph S2[持久化阶段]
        B1[多个 _llm.json 文件] --> B2[merge_dedupe]
        B2 --> B3[按 URL / 标题建键]
        B3 --> B4[保留更长正文]
        B4 --> B5[情感分取最大值]
        B5 --> B6[关键词取并集]
    end

    A4 --> B1
    B6 --> C[去重后统一入库]
```

## 图 2-5 Jieba 分词与停用词过滤流程图

```mermaid
flowchart TD
    A[正文文本输入] --> B[jieba.cut 精确模式分词]
    B --> C[正则校验]
    C --> D[停用词过滤]
    D --> E[min_len >= 2]
    E --> F[Counter 词频统计]
    F --> G[高频词输出]
```

## 图 2-6 Qwen API 调用架构图

```mermaid
flowchart TD
    A[LlmAnalyzer] --> B[官方 SDK 调用]
    B -->|成功| C[返回 JSON 结果]
    B -->|失败| D[兼容 REST API 调用]
    D -->|成功| C
    D -->|解析失败| E[正则提取 JSON 兜底]
    E -->|成功| C
    E -->|失败| F[默认值 0.5 / 空关键词]

    A --> G[同步 BatchRunner]
    A --> H[异步 AsyncBatchRunner]
    H --> I[Semaphore 100 并发]
    H --> J[ijson 流式读取]
    H --> K[JsonArrayWriter 流式写入]
```

## 图 3-1 系统数据库 E-R 图

```mermaid
erDiagram
    land_news_analysis {
        INT id PK
        VARCHAR url
        VARCHAR title
        DATE publish_date
        VARCHAR source
        TEXT content_summary
        DECIMAL sentiment_score
        JSON keywords
        TIMESTAMP created_at
    }

    word_frequency_stats {
        INT id PK
        VARCHAR word
        INT count
        TIMESTAMP created_at
    }

    land_news_analysis ||--o{ word_frequency_stats : "keywords JSON\n多对多关联"
```

## 图 3-2 系统总体架构图

```mermaid
flowchart LR
    A[(agri.cn\nmoa.gov.cn\nchinadaily)] --> B[Scrapy\n3专用Spider\nEngine+Downloader]
    B --> C[处理层\nDataCleaner清洗\nQwen情感+Jieba词频]
    C --> D[(MySQL双表)]
    D --> E[Electron应用\nMain Process\nVue3+ECharts看板]
    E -.->|IPC查询| D
```

## 图 2-7 MySQL 数据库双表结构示意图

（已移至图 3-1）

## 图 3-3 系统技术选型总览图

```mermaid
flowchart LR
    A[System Message\n只返回 JSON 的助手] --> D[完整提示词]
    B[User Message\n标题 + 正文片段] --> D
    C[JSON 示例模板\nscore + keywords] --> D
    D --> E[Qwen 模型]
    E --> F[纯 JSON 输出]
    F --> G[json.loads 解析]
    G --> H[结构化结果]
    G -.失败.-> I[正则兜底提取]
    I --> H
```

## 图 2-10 Electron 双进程架构图

```mermaid
flowchart LR
    subgraph MP[Main Process]
        A[窗口管理]
        B[数据库访问]
        C[Qwen 调用]
        D[本地 Shell 管理]
    end

    subgraph PP[Preload Script]
        E[contextBridge API]
    end

    subgraph RP[Renderer Process]
        F[Vue 页面]
        G[ECharts 看板]
        H[智能问答]
        I[终端界面]
    end

    MP <--> PP
    PP <--> RP
```

## 图 2-11 IPC 进程间通信示意图

```mermaid
sequenceDiagram
    participant R as Renderer
    participant P as Preload
    participant M as Main

    R->>P: ipcRenderer.invoke("qa:ask", payload)
    P->>M: contextBridge 暴露接口转发
    M->>M: ipcMain.handle() 处理请求
    M-->>P: 返回结果
    P-->>R: 响应数据
```

## 图 3-3 系统技术选型总览图

```mermaid
flowchart LR
    A[采集端\nScrapy + 3 Spider] --> B[清洗端\nlxml + XPath]
    B --> C[分析端\nQwen + Jieba]
    C --> D[存储端\nMySQL + PyMySQL]
    D --> E[应用端\nElectron + Vue3 + ECharts]
```

## 图 4-2 Scrapy 异常处理机制层次图

```mermaid
flowchart TD
    A[请求发起] --> B[DOWNLOAD_TIMEOUT 超时控制]
    B --> C[RetryMiddleware 自动重试]
    C --> D[Spider try/except 捕获异常]
    D --> E[errback 失败兜底]
    E --> F[AUTOTHROTTLE 动态调速]
    F --> G[日志记录与稳定输出]
```

## 图 4-5 Jieba 词频统计流程图

```mermaid
flowchart TD
    A[clean.json 正文] --> B[jieba.cut 分词]
    B --> C[正则过滤]
    C --> D[停用词过滤]
    D --> E[Counter 计数]
    E --> F[most_common(300)]
    F --> G[word_frequency_top.json]
```

## 图 4-6 数据持久化入库流程图

```mermaid
flowchart TD
    A[_llm.json 输入] --> B[normalize_llm_record 标准化]
    B --> C[merge_dedupe 去重合并]
    C --> D[to_mysql_rows 字段映射]
    D --> E[executemany 批量写入]
    E --> F[land_news_analysis]

    G[word_frequency_top.json] --> H[load_word_frequency]
    H --> I[insert_many 批量导入]
    I --> J[word_frequency_stats]
```

## 图 6-1 系统测试场景覆盖概览图

```mermaid
flowchart LR
    subgraph A[采集]
        TC01[TC01 采集成功率]
    end

    subgraph B[清洗]
        TC02[TC02 字段提取准确率]
    end

    subgraph C[分析]
        TC03[TC03 JSON 合规率]
        TC04[TC04 Top10 合理性]
        TC09[TC09 流式内存优化]
    end

    subgraph D[入库]
        TC05[TC05 入库一致性]
        TC10[TC10 全链路执行]
    end

    subgraph E[可视化与问答]
        TC06[TC06 图表数据匹配]
        TC07[TC07 Text-to-SQL 语义正确性]
    end

    subgraph F[安全]
        TC08[TC08 非法 SQL 拦截]
    end
```
