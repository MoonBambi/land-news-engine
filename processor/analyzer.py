import argparse
import asyncio
import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional

import dashscope
from dashscope import Generation
try:
    from dashscope import AioGeneration
except Exception:
    AioGeneration = None
try:
    import ijson
except Exception:
    ijson = None
import requests
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


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


class LlmAnalyzer:
    def __init__(self, model: str = "qwen-flash", api_key: Optional[str] = None):
        self.model = model
        self.enable_thinking = False
        self.compatible_base_url = os.getenv("DASHSCOPE_COMPATIBLE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1").rstrip("/")
        # 优先使用传入的 api_key，否则从环境变量 DASHSCOPE_API_KEY 中读取
        if api_key:
            dashscope.api_key = api_key
        elif os.getenv("DASHSCOPE_API_KEY"):
            dashscope.api_key = os.getenv("DASHSCOPE_API_KEY")

    def _build_prompt(self, title: str, content: str) -> str:
        # 截取正文前 600 字，节省 Token 并聚焦核心政策内容
        trimmed = content[:600] if content else ""
        return (
            "你是一个农村土地政策研究专家。请分析以下新闻：\n"
            f"标题：{title}\n"
            f"正文：{trimmed}\n\n"
            "任务要求：\n"
            "1. 情感评分：0.0（极负面）到 1.0（极正面），中性为 0.5。\n"
            "2. 关键词：提取3个与土地流转相关的专业词汇。\n"
            "3. 必须只输出JSON格式，严禁包含任何解释文字。\n"
            "格式示例：{\"score\": 0.85, \"keywords\": [\"承包权\", \"经营权\", \"流转费\"]}"
        )

    def _extract_json(self, text: str) -> Dict[str, Any]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", text, re.S)
            if match:
                return json.loads(match.group(0))
            raise

    def _resolve_api_key(self) -> Optional[str]:
        api_key = getattr(dashscope, "api_key", None)
        if api_key:
            return api_key
        return os.getenv("DASHSCOPE_API_KEY")

    def _call_compatible_chat(self, messages: List[Dict[str, str]]) -> str:
        api_key = self._resolve_api_key()
        if not api_key:
            raise RuntimeError("Missing DASHSCOPE_API_KEY")

        url = f"{self.compatible_base_url}/chat/completions"
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "enable_thinking": self.enable_thinking,
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        try:
            data = resp.json()
        except Exception:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}") from None

        if resp.status_code != 200:
            message = ""
            if isinstance(data, dict):
                err = data.get("error")
                if isinstance(err, dict):
                    message = err.get("message") or ""
            raise RuntimeError(f"HTTP {resp.status_code}: {message or data}")

        choices = data.get("choices") if isinstance(data, dict) else None
        if not isinstance(choices, list) or not choices:
            raise RuntimeError(f"Invalid response: {data}")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        if not isinstance(message, dict):
            raise RuntimeError(f"Invalid response: {data}")
        content = message.get("content")
        if not isinstance(content, str):
            raise RuntimeError(f"Invalid response: {data}")
        return content

    def analyze(self, title: str, content: str) -> Optional[Dict[str, Any]]:
        messages = [
            {"role": "system", "content": "你是一个只返回JSON格式数据的助手。"},
            {"role": "user", "content": self._build_prompt(title, content)},
        ]
        try:
            try:
                response = Generation.call(
                    model=self.model,
                    messages=messages,
                    result_format="message",
                    extra_body={"enable_thinking": self.enable_thinking},
                )
            except TypeError:
                response = Generation.call(
                    model=self.model,
                    messages=messages,
                    result_format="message",
                )
            if response.status_code == 200:
                raw_content = response.output.choices[0].message.content
                return self._extract_json(raw_content)
            code = getattr(response, "code", "")
            message = getattr(response, "message", "")
            if isinstance(message, str) and "url error" in message.lower():
                raw_content = self._call_compatible_chat(messages)
                return self._extract_json(raw_content)
            print(f"\n[错误] API 调用失败: {code} - {message}")
            return None
        except Exception as e:
            try:
                raw_content = self._call_compatible_chat(messages)
                return self._extract_json(raw_content)
            except Exception:
                print(f"\n[异常] 分析过程中出错: {e}")
            return None

    async def async_analyze(self, title: str, content: str) -> Optional[Dict[str, Any]]:
        messages = [
            {"role": "system", "content": "你是一个只返回JSON格式数据的助手。"},
            {"role": "user", "content": self._build_prompt(title, content)},
        ]
        if not AioGeneration:
            return await asyncio.to_thread(self.analyze, title, content)
        try:
            try:
                response = await AioGeneration.call(
                    model=self.model,
                    messages=messages,
                    result_format="message",
                    extra_body={"enable_thinking": self.enable_thinking},
                )
            except TypeError:
                response = await AioGeneration.call(
                    model=self.model,
                    messages=messages,
                    result_format="message",
                )
            if response.status_code == 200:
                raw_content = response.output.choices[0].message.content
                return self._extract_json(raw_content)
            message = getattr(response, "message", "")
            if isinstance(message, str) and "url error" in message.lower():
                raw_content = await asyncio.to_thread(self._call_compatible_chat, messages)
                return self._extract_json(raw_content)
            return None
        except Exception:
            try:
                raw_content = await asyncio.to_thread(self._call_compatible_chat, messages)
                return self._extract_json(raw_content)
            except Exception:
                return None


class BatchRunner:
    def __init__(self, analyzer: LlmAnalyzer, input_dir: str, output_dir: str, limit: Optional[int]):
        self.analyzer = analyzer
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.limit = limit

    def _iter_input_files(self) -> List[str]:
        files: List[str] = []
        for root, _, filenames in os.walk(self.input_dir):
            for name in filenames:
                if name.lower().endswith(".json") and not name.lower().endswith("_llm.json"):
                    files.append(os.path.join(root, name))
        return sorted(files)

    def _load_json(self, path: str) -> List[Dict[str, Any]]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_json(self, records: List[Dict[str, Any]], output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

    def _analyze_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        title = item.get("标题") or item.get("title") or "无标题"
        content = item.get("正文") or item.get("content") or ""
        
        result = self.analyzer.analyze(title, content)
        if result:
            item["sentiment_score"] = result.get("score")
            item["keywords"] = result.get("keywords")
        else:
            item["sentiment_score"] = 0.5
            item["keywords"] = []
        return item

    def run(self) -> None:
        if not os.path.exists(self.input_dir):
            print(f"输入目录不存在: {self.input_dir}")
            return

        if os.path.isfile(self.input_dir):
            input_files = [self.input_dir]
        else:
            input_files = self._iter_input_files()
        if not input_files:
            print("未找到待处理的 JSON 文件。")
            return

        for path in input_files:
            print(f"\n正在加载大文件: {os.path.basename(path)} ...")
            data = self._load_json(path)
            
            # 如果设置了 limit，则只截取前 N 条
            items_to_process = data[: self.limit] if self.limit else data
            processed_data: List[Dict[str, Any]] = []

            # 加入进度条显示
            iterator = tqdm(items_to_process, desc="分析进度", unit="条") if tqdm else items_to_process
            
            for item in iterator:
                processed_data.append(self._analyze_item(item))

            base_name = os.path.splitext(os.path.basename(path))[0]
            output_path = os.path.join(self.output_dir, f"{base_name}_llm.json")
            self._save_json(processed_data, output_path)
            print(f"分析完成！结果已保存至: {output_path}")


class AsyncBatchRunner:
    def __init__(
        self,
        analyzer: LlmAnalyzer,
        input_path: str,
        output_dir: str,
        limit: Optional[int],
        concurrency: int,
        batch_size: int,
        stream: bool,
    ):
        self.analyzer = analyzer
        self.input_path = input_path
        self.output_dir = output_dir
        self.limit = limit
        self.concurrency = max(1, concurrency)
        self.batch_size = max(1, batch_size)
        self.stream = stream

    def _iter_input_files(self) -> List[str]:
        if os.path.isfile(self.input_path):
            return [self.input_path]
        files: List[str] = []
        for root, _, filenames in os.walk(self.input_path):
            for name in filenames:
                if name.lower().endswith(".json") and not name.lower().endswith("_llm.json"):
                    files.append(os.path.join(root, name))
        return sorted(files)

    async def _analyze_item(self, semaphore: asyncio.Semaphore, item: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            title = item.get("标题") or item.get("title") or "无标题"
            content = item.get("正文") or item.get("content") or ""
            result = await self.analyzer.async_analyze(title, content)
            if result:
                item["sentiment_score"] = result.get("score")
                item["keywords"] = result.get("keywords")
            else:
                item["sentiment_score"] = 0.5
                item["keywords"] = []
            return item

    async def _process_file_stream(self, input_path: str, output_path: str) -> None:
        if not ijson:
            raise RuntimeError("Missing ijson")
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks: List[asyncio.Task] = []
        processed = 0
        total = self.limit if self.limit is not None else None
        bar = tqdm(total=total, desc=os.path.basename(input_path), unit="条") if tqdm else None
        with open(input_path, "rb") as f, JsonArrayWriter(output_path) as writer:
            for item in ijson.items(f, "item"):
                if self.limit is not None and processed >= self.limit:
                    break
                tasks.append(asyncio.create_task(self._analyze_item(semaphore, item)))
                processed += 1
                if len(tasks) >= self.batch_size:
                    results = await asyncio.gather(*tasks)
                    writer.write_many(results)
                    if bar is not None:
                        bar.update(len(results))
                    tasks = []
            if tasks:
                results = await asyncio.gather(*tasks)
                writer.write_many(results)
                if bar is not None:
                    bar.update(len(results))
        if bar is not None:
            bar.close()

    async def _process_file_load(self, input_path: str, output_path: str) -> None:
        semaphore = asyncio.Semaphore(self.concurrency)
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data[: self.limit] if self.limit else data
        bar = tqdm(total=len(items), desc=os.path.basename(input_path), unit="条") if tqdm else None
        tasks: List[asyncio.Task] = []
        with JsonArrayWriter(output_path) as writer:
            for item in items:
                tasks.append(asyncio.create_task(self._analyze_item(semaphore, item)))
                if len(tasks) >= self.batch_size:
                    results = await asyncio.gather(*tasks)
                    writer.write_many(results)
                    if bar is not None:
                        bar.update(len(results))
                    tasks = []
            if tasks:
                results = await asyncio.gather(*tasks)
                writer.write_many(results)
                if bar is not None:
                    bar.update(len(results))
        if bar is not None:
            bar.close()

    async def run(self) -> None:
        if not os.path.exists(self.input_path):
            print(f"输入目录不存在: {self.input_path}")
            return
        input_files = self._iter_input_files()
        if not input_files:
            print("未找到待处理的 JSON 文件。")
            return
        for input_path in input_files:
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(self.output_dir, f"{base_name}_llm.json")
            if self.stream:
                await self._process_file_stream(input_path, output_path)
            else:
                await self._process_file_load(input_path, output_path)
            print(f"分析完成！结果已保存至: {output_path}")


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    
    default_input = os.path.join(project_root, "data", "clean")
    default_output = os.path.join(project_root, "data", "clean")

    parser = argparse.ArgumentParser(description="土地流转新闻情感分析工具")
    parser.add_argument("--input", default=default_input, help="输入 JSON 目录")
    parser.add_argument("--output", default=default_output, help="输出结果目录")
    parser.add_argument("--limit", type=int, default=None, help="限制处理条数（测试用）")
    parser.add_argument("--model", default="qwen-flash", help="模型名称")
    parser.add_argument("--api-key", default=None, help="DashScope API Key")
    parser.add_argument("--async", dest="use_async", action="store_true", help="启用异步并发分析")
    parser.add_argument("--concurrency", type=int, default=100, help="并发数")
    parser.add_argument("--batch-size", type=int, default=50, help="批次大小")
    parser.add_argument("--stream", action="store_true", help="使用 ijson 流式读取")
    
    args = parser.parse_args()

    analyzer = LlmAnalyzer(model=args.model, api_key=args.api_key)
    
    print("="*30)
    print("农村土地流转新闻 NLP 分析系统启动")
    print(f"使用模型: {args.model}")
    print(f"限制条数: {args.limit if args.limit else '全量处理'}")
    print("="*30)
    
    if args.use_async:
        runner = AsyncBatchRunner(
            analyzer,
            args.input,
            args.output,
            args.limit,
            args.concurrency,
            args.batch_size,
            args.stream,
        )
        asyncio.run(runner.run())
    else:
        runner = BatchRunner(analyzer, args.input, args.output, args.limit)
        runner.run()


if __name__ == "__main__":
    main()
