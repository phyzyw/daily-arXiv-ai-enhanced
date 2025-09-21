import os
import json
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
import dotenv
import argparse
from tqdm import tqdm
from time import sleep
import PyPDF2
from io import BytesIO

if os.path.exists('.env'):
    dotenv.load_dotenv()

# 嵌入提示模板
TEMPLATE = """
Generate a JSON summary in {language} for the following paper content:
{content}

Return the response in the following JSON format:
{
  "tldr": "A concise summary of the paper (1-2 sentences).",
  "motivation": "The motivation or problem addressed by the paper.",
  "method": "The methodology or approach used in the paper.",
  "result": "Key results or findings of the paper.",
  "conclusion": "The conclusions or implications of the paper."
}
"""

# 嵌入系统指令
SYSTEM = """
You are an AI assistant that summarizes academic papers. Always respond with a valid JSON object containing the fields: tldr, motivation, method, result, and conclusion. Do not include any text outside the JSON structure. Ensure the response is based on the full content provided, capturing key details accurately.
"""

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, required=True, help="jsonline data file")
    parser.add_argument("--max_workers", type=int, default=1, help="Maximum number of parallel workers")
    return parser.parse_args()

def download_pdf(url: str) -> str:
    """从 arXiv 下载 PDF 并提取文本"""
    try:
        # 将 abs 链接转换为 PDF 链接
        pdf_url = url.replace('/abs/', '/pdf/') + '.pdf'
        response = requests.get(pdf_url, timeout=10)
        response.raise_for_status()
        pdf_file = BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text = ""
        for page in pdf_reader.pages:
            page_text = page.extract_text() or ""
            text += page_text
        # 限制文本长度（Cloudflare API 输入限制）
        return text[:8000]  # 截断到 8000 字符
    except Exception as e:
        print(f"Failed to download or extract PDF from {url}: {e}", file=sys.stderr)
        return None

def call_cloudflare_api(account_id, api_token, model_name, prompt, max_tokens=1024):
    """调用 Cloudflare Workers AI REST API"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model_name}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }
    try:
        print(f"Sending prompt to Cloudflare API: {prompt[:100]}...", file=sys.stderr)
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        response_text = result.get('result', {}).get('response', '')
        print(f"Received response: {response_text[:100]}...", file=sys.stderr)
        return response_text
    except requests.exceptions.RequestException as e:
        print(f"Cloudflare API error: {e}", file=sys.stderr)
        return None

def process_single_item(item: Dict, language: str) -> Dict:
    """处理单个数据项，使用 Cloudflare Workers AI"""
    if not item or 'id' not in item or 'summary' not in item or 'abs' not in item or 'categories' not in item:
        print(f"Invalid item: {item}", file=sys.stderr)
        return None
    
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    model_name = os.environ.get("MODEL_NAME", "@cf/meta/llama-3-8b-instruct")
    
    if not account_id or not api_token:
        print(f"Missing Cloudflare credentials for {item['id']}", file=sys.stderr)
        return {
            **item,
            'AI': {
                "tldr": item['summary'][:100] + "..." if item.get('summary') else "No summary available",
                "motivation": "N/A",
                "method": "N/A",
                "result": "N/A",
                "conclusion": "N/A"
            }
        }
    
    # 下载并提取 PDF 全文
    full_text = download_pdf(item['abs'])
    content = full_text if full_text else item['summary']
    
    # 构建提示
    try:
        prompt = TEMPLATE.format(language=language, content=content[:8000])
    except KeyError as e:
        print(f"Template formatting error for {item['id']}: {e}", file=sys.stderr)
        return {
            **item,
            'AI': {
                "tldr": item['summary'][:100] + "..." if item.get('summary') else "No summary available",
                "motivation": "N/A",
                "method": "N/A",
                "result": "N/A",
                "conclusion": "N/A"
            }
        }
    
    for attempt in range(3):
        try:
            response_text = call_cloudflare_api(account_id, api_token, model_name, prompt)
            if response_text:
                try:
                    ai_data = json.loads(response_text)
                    if all(key in ai_data for key in ["tldr", "motivation", "method", "result", "conclusion"]):
                        return {**item, 'AI': ai_data}
                    else:
                        print(f"Incomplete JSON response for {item['id']}: {response_text}", file=sys.stderr)
                except json.JSONDecodeError:
                    print(f"Invalid JSON response for {item['id']}: {response_text}", file=sys.stderr)
                    return {
                        **item,
                        'AI': {
                            "tldr": response_text[:100] + "..." if response_text else "No response",
                            "motivation": "N/A",
                            "method": "N/A",
                            "result": "N/A",
                            "conclusion": "N/A"
                        }
                    }
            else:
                print(f"Empty response for {item['id']}", file=sys.stderr)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {item['id']}: {e}", file=sys.stderr)
            if attempt < 2:
                sleep(2)
        if attempt == 2:
            return {
                **item,
                'AI': {
                    "tldr": item['summary'][:100] + "..." if item.get('summary') else "No summary available",
                    "motivation": "N/A",
                    "method": "N/A",
                    "result": "N/A",
                    "conclusion": "N/A"
                }
            }
    return item

def process_all_items(data: List[Dict], language: str, max_workers: int) -> List[Dict]:
    """并行处理所有数据项"""
    print(f'Connected to Cloudflare Workers AI', file=sys.stderr)
    processed_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(process_single_item, item, language): item for item in data}
        for future in tqdm(as_completed(future_to_item), total=len(data), desc="Processing items"):
            item = future_to_item[future]
            try:
                result = future.result()
                if result is not None:
                    processed_data.append(result)
                else:
                    print(f"Skipping invalid item: {item.get('id', 'unknown')}", file=sys.stderr)
            except Exception as e:
                print(f"Item {item.get('id', 'unknown')} generated an exception: {e}", file=sys.stderr)
                processed_data.append({
                    **item,
                    'AI': {
                        "tldr": item['summary'][:100] + "..." if item.get('summary') else "No summary available",
                        "motivation": "N/A",
                        "method": "N/A",
                        "result": "N/A",
                        "conclusion": "N/A"
                    }
                })
    return processed_data

def main():
    args = parse_args()
    language = os.environ.get("LANGUAGE", 'English')
    
    if not os.path.exists(args.data):
        print(f"Error: Input file {args.data} does not exist", file=sys.stderr)
        sys.exit(1)
    
    base_name = os.path.splitext(args.data)[0]
    target_file = f"{base_name}_AI_enhanced_{language}.jsonl"
    
    print(f"Input file: {args.data}", file=sys.stderr)
    print(f"Target output file: {target_file}", file=sys.stderr)
    
    if os.path.exists(target_file):
        os.remove(target_file)
        print(f"Removed existing output file: {target_file}", file=sys.stderr)
    
    data = []
    print(f'Opening input file: {args.data}', file=sys.stderr)
    with open(args.data, "r", encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    item = json.loads(line)
                    if 'id' in item and 'summary' in item and 'abs' in item and 'categories' in item:
                        data.append(item)
                    else:
                        print(f"Skipping invalid JSON line: {line.strip()}", file=sys.stderr)
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON line: {e}", file=sys.stderr)
    
    seen_ids = set()
    unique_data = []
    for item in data:
        if item['id'] not in seen_ids:
            seen_ids.add(item['id'])
            unique_data.append(item)
    data = unique_data
    print(f'Loaded {len(data)} unique items from {args.data}', file=sys.stderr)
    
    # 限制处理数量以避免配额超限
    max_items = 10
    data = data[:max_items]
    print(f'Processing {len(data)} items from {args.data}', file=sys.stderr)
    
    processed_data = process_all_items(data, language, args.max_workers)
    
    print(f'Writing to output file: {target_file}', file=sys.stderr)
    with open(target_file, "w", encoding='utf-8') as f:
        for item in processed_data:
            if item is not None:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Successfully wrote {len(processed_data)} items to {target_file}", file=sys.stderr)

if __name__ == "__main__":
    main()
