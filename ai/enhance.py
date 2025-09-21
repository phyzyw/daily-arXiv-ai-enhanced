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
    parser.add_argument("--data", type=str, required=True, help="json or jsonline data file")
    parser.add_argument("--max_workers", type=int, default=1, help="Maximum number of parallel workers")
    return parser.parse_args()

def download_pdf(url: str) -> str:
    """从 arXiv 下载 PDF 并提取文本"""
    try:
        # 将 abs 链接转换为 PDF 链接
        pdf_url = url.replace('/abs/', '/pdf/') + '.pdf'
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()
        pdf_file = BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text = ""
        for page in pdf_reader.pages:
            page_text = page.extract_text() or ""
            text += page_text
        return text[:12000]  # 限制文本长度
    except Exception as e:
        print(f"Failed to download or extract PDF from {url}: {e}", file=sys.stderr)
        return None

def call_cloudflare_api(account_id, api_token, model_name, prompt, max_tokens=2048):
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
        "temperature": 0.1
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        result = response.json()
        return result.get('result', {}).get('response', '')
    except requests.exceptions.RequestException as e:
        print(f"Cloudflare API error: {e}", file=sys.stderr)
        return None

def extract_json_from_response(response_text: str) -> Dict:
    """从响应文本中提取JSON对象"""
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        try:
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}')
            if start_idx != -1 and end_idx != -1:
                json_str = response_text[start_idx:end_idx+1]
                return json.loads(json_str)
        except:
            pass
    return None

def process_single_item(item: Dict, language: str) -> Dict:
    """处理单个数据项"""
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    model_name = os.environ.get("MODEL_NAME", "@cf/meta/llama-3-8b-instruct")
    
    if not account_id or not api_token:
        print(f"Missing Cloudflare credentials", file=sys.stderr)
        return {
            **item,
            'AI': {
                "tldr": item.get('summary', '')[:200] + "..." if item.get('summary') else "No summary",
                "motivation": "N/A",
                "method": "N/A",
                "result": "N/A",
                "conclusion": "N/A"
            }
        }
    
    # 获取PDF URL - 优先使用pdf_url字段，如果没有则从abs字段构造
    pdf_url = item.get('pdf_url')
    if not pdf_url and 'abs' in item:
        pdf_url = item['abs'].replace('/abs/', '/pdf/') + '.pdf'
    
    # 下载PDF
    full_text = download_pdf(pdf_url) if pdf_url else None
    content = full_text if full_text else item.get('summary', '')
    
    # 构建提示
    try:
        prompt = TEMPLATE.format(language=language, content=content[:12000])
    except Exception as e:
        print(f"Template error: {e}", file=sys.stderr)
        return {
            **item,
            'AI': {
                "tldr": item.get('summary', '')[:200] + "..." if item.get('summary') else "No summary",
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
                ai_data = extract_json_from_response(response_text)
                
                if ai_data and all(key in ai_data for key in ["tldr", "motivation", "method", "result", "conclusion"]):
                    return {**item, 'AI': ai_data}
                else:
                    print(f"Invalid JSON response for {item.get('id', 'unknown')}: {response_text[:100]}...", file=sys.stderr)
                    # 创建回退响应
                    return {
                        **item,
                        'AI': {
                            "tldr": item.get('summary', '')[:200] + "..." if item.get('summary') else "No summary available",
                            "motivation": "Extracted from paper content" if full_text else "Based on abstract",
                            "method": "See original paper for methodology details",
                            "result": "Refer to the paper for complete results",
                            "conclusion": "Consult the original publication for conclusions"
                        }
                    }
            else:
                print(f"Empty response for {item.get('id', 'unknown')} on attempt {attempt + 1}", file=sys.stderr)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {item.get('id', 'unknown')}: {e}", file=sys.stderr)
        
        if attempt < 2:
            sleep(5)  # 增加重试间隔
    
    # 所有尝试都失败后的回退
    return {
        **item,
        'AI': {
            "tldr": item.get('summary', '')[:200] + "..." if item.get('summary') else "No summary available",
            "motivation": "N/A",
            "method": "N/A",
            "result": "N/A",
            "conclusion": "N/A"
        }
    }

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
                    # 添加一个基本的回退项
                    processed_data.append({
                        **item,
                        'AI': {
                            "tldr": item.get('summary', '')[:200] + "..." if item.get('summary') else "No summary",
                            "motivation": "N/A",
                            "method": "N/A",
                            "result": "N/A",
                            "conclusion": "N/A"
                        }
                    })
            except Exception as e:
                print(f"Item {item.get('id', 'unknown')} generated an exception: {e}", file=sys.stderr)
                processed_data.append({
                    **item,
                    'AI': {
                        "tldr": item.get('summary', '')[:200] + "..." if item.get('summary') else "No summary",
                        "motivation": "N/A",
                        "method": "N/A",
                        "result": "N/A",
                        "conclusion": "N/A"
                    }
                })
    
    return processed_data

def read_jsonl_file(file_path: str) -> List[Dict]:
    """读取JSONL文件，无论扩展名是什么"""
    data = []
    print(f'Opening input file: {file_path}', file=sys.stderr)
    
    with open(file_path, "r", encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                # 检查必需字段 - 根据您的实际数据结构调整
                if 'id' in item and 'title' in item and 'summary' in item:
                    # 确保所有必需字段都有默认值
                    item.setdefault('authors', [])
                    item.setdefault('categories', [])
                    item.setdefault('abs', '')
                    item.setdefault('pdf_url', '')
                    data.append(item)
                else:
                    print(f"Line {line_num}: Missing required fields - id: {'id' in item}, title: {'title' in item}, summary: {'summary' in item}", file=sys.stderr)
                    print(f"Line content: {line[:100]}...", file=sys.stderr)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON line {line_num}: {e}", file=sys.stderr)
                print(f"Line content: {line[:100]}...", file=sys.stderr)
    
    return data

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
    
    # 读取数据
    data = read_jsonl_file(args.data)
    
    if not data:
        print(f"No valid data found in {args.data}", file=sys.stderr)
        sys.exit(1)
    
    # 去重
    seen_ids = set()
    unique_data = []
    for item in data:
        item_id = item.get('id')
        if item_id and item_id not in seen_ids:
            seen_ids.add(item_id)
            unique_data.append(item)
    
    data = unique_data
    print(f'Loaded {len(data)} unique items from {args.data}', file=sys.stderr)
    
    # 处理所有项目
    processed_data = process_all_items(data, language, args.max_workers)
    
    # 保存结果
    print(f'Writing {len(processed_data)} items to output file: {target_file}', file=sys.stderr)
    with open(target_file, "w", encoding='utf-8') as f:
        for item in processed_data:
            if item is not None:
                # 确保输出格式符合转换脚本的要求
                output_item = {
                    'id': item.get('id'),
                    'title': item.get('title'),
                    'authors': item.get('authors', []),
                    'summary': item.get('summary', ''),
                    'abs': item.get('abs', ''),
                    'categories': item.get('categories', []),
                    'AI': item.get('AI', {})
                }
                f.write(json.dumps(output_item, ensure_ascii=False) + "\n")
    
    print(f"Successfully processed {len(processed_data)} items", file=sys.stderr)

if __name__ == "__main__":
    main()
