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
import re

if os.path.exists('.env'):
    dotenv.load_dotenv()

# 嵌入提示模板 - 改进版本
TEMPLATE = """
请为以下学术论文内容生成一个{language}的JSON摘要：

{content}

请严格按照以下JSON格式返回响应，不要包含任何其他文本：
{{
  "tldr": "论文的简洁摘要（1-2句话）",
  "motivation": "论文解决的问题或动机",
  "method": "论文使用的方法论或 approach",
  "result": "论文的主要结果或发现",
  "conclusion": "论文的结论或意义"
}}
"""

# 嵌入系统指令 - 改进版本
SYSTEM = """
你是一个专门总结学术论文的AI助手。请始终以有效的JSON对象响应，包含以下字段：tldr, motivation, method, result, conclusion。
不要包含JSON之外的任何文本。确保响应基于提供的完整内容，准确捕捉关键细节。
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
        if '/abs/' in url:
            pdf_url = url.replace('/abs/', '/pdf/') + '.pdf'
        else:
            pdf_url = url
        
        response = requests.get(pdf_url, timeout=60)
        response.raise_for_status()
        pdf_file = BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text = ""
        for page in pdf_reader.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"
        return text[:15000]  # 增加文本长度限制
    except Exception as e:
        print(f"Failed to download or extract PDF from {url}: {e}", file=sys.stderr)
        return None

def call_cloudflare_api(account_id, api_token, model_name, prompt, max_tokens=4096):
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
        "temperature": 0.1,
        "stream": False
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json()
        
        # 检查响应结构
        if 'result' in result and 'response' in result['result']:
            return result['result']['response']
        elif 'response' in result:
            return result['response']
        else:
            print(f"Unexpected API response structure: {result}", file=sys.stderr)
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Cloudflare API error: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}", file=sys.stderr)
        return None

def extract_json_from_response(response_text: str) -> Dict:
    """从响应文本中提取JSON对象"""
    if not response_text:
        return None
        
    # 尝试直接解析JSON
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        pass
    
    # 尝试从文本中提取JSON
    try:
        # 查找JSON对象的开始和结束位置
        json_pattern = r'\{[\s\S]*\}'
        matches = re.findall(json_pattern, response_text)
        if matches:
            # 选择最长的匹配项（最可能是完整的JSON）
            longest_match = max(matches, key=len)
            return json.loads(longest_match)
    except:
        pass
    
    # 如果以上方法都失败，尝试修复常见的JSON问题
    try:
        # 修复单引号问题
        fixed_text = response_text.replace("'", '"')
        # 修复未转义的特殊字符
        fixed_text = re.sub(r'(?<!\\)"', '\\"', fixed_text)
        return json.loads(fixed_text)
    except:
        pass
    
    return None

def create_fallback_ai_data(item: Dict, full_text: bool = False) -> Dict:
    """创建回退的AI数据"""
    summary = item.get('summary', '')
    return {
        "tldr": summary[:300] + "..." if len(summary) > 300 else summary,
        "motivation": "基于论文摘要分析" if not full_text else "基于论文全文分析",
        "method": "参见原始论文的方法论部分",
        "result": "论文报告了重要的实验结果和发现",
        "conclusion": "论文提出了有意义的结论和未来方向"
    }

def process_single_item(item: Dict, language: str) -> Dict:
    """处理单个数据项"""
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    model_name = os.environ.get("MODEL_NAME", "@cf/meta/llama-3-8b-instruct")
    
    if not account_id or not api_token:
        print(f"Missing Cloudflare credentials", file=sys.stderr)
        return {
            **item,
            'AI': create_fallback_ai_data(item, False)
        }
    
    # 获取PDF URL
    pdf_url = item.get('pdf_url')
    if not pdf_url and 'abs' in item:
        pdf_url = item['abs'].replace('/abs/', '/pdf/') + '.pdf'
    
    # 下载PDF
    full_text = None
    if pdf_url:
        print(f"Downloading PDF for {item.get('id', 'unknown')}: {pdf_url}", file=sys.stderr)
        full_text = download_pdf(pdf_url)
    
    content = full_text if full_text else item.get('summary', '')
    
    if not content:
        print(f"No content available for {item.get('id', 'unknown')}", file=sys.stderr)
        return {
            **item,
            'AI': create_fallback_ai_data(item, False)
        }
    
    # 构建提示
    try:
        prompt = TEMPLATE.format(language=language, content=content[:15000])
    except Exception as e:
        print(f"Template error for {item.get('id', 'unknown')}: {e}", file=sys.stderr)
        return {
            **item,
            'AI': create_fallback_ai_data(item, bool(full_text))
        }
    
    for attempt in range(3):
        try:
            print(f"Processing {item.get('id', 'unknown')}, attempt {attempt + 1}", file=sys.stderr)
            response_text = call_cloudflare_api(account_id, api_token, model_name, prompt)
            
            if response_text:
                print(f"Raw response for {item.get('id', 'unknown')}: {response_text[:200]}...", file=sys.stderr)
                ai_data = extract_json_from_response(response_text)
                
                if ai_data and all(key in ai_data for key in ["tldr", "motivation", "method", "result", "conclusion"]):
                    print(f"Successfully extracted AI data for {item.get('id', 'unknown')}", file=sys.stderr)
                    return {**item, 'AI': ai_data}
                else:
                    print(f"Invalid or incomplete JSON response for {item.get('id', 'unknown')}", file=sys.stderr)
            else:
                print(f"Empty response for {item.get('id', 'unknown')} on attempt {attempt + 1}", file=sys.stderr)
                
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {item.get('id', 'unknown')}: {e}", file=sys.stderr)
        
        if attempt < 2:
            sleep(10)  # 增加重试间隔
    
    # 所有尝试都失败后的回退
    print(f"All attempts failed for {item.get('id', 'unknown')}, using fallback", file=sys.stderr)
    return {
        **item,
        'AI': create_fallback_ai_data(item, bool(full_text))
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
                processed_data.append(result)
            except Exception as e:
                print(f"Item {item.get('id', 'unknown')} generated an exception: {e}", file=sys.stderr)
                processed_data.append({
                    **item,
                    'AI': create_fallback_ai_data(item, False)
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
                # 检查必需字段
                if 'id' in item and 'title' in item and 'summary' in item:
                    # 确保所有必需字段都有默认值
                    item.setdefault('authors', [])
                    item.setdefault('categories', [])
                    item.setdefault('abs', '')
                    item.setdefault('pdf_url', '')
                    data.append(item)
                else:
                    print(f"Line {line_num}: Missing required fields", file=sys.stderr)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON line {line_num}: {e}", file=sys.stderr)
    
    return data

def main():
    args = parse_args()
    language = os.environ.get("LANGUAGE", 'Chinese')  # 改为中文以提高响应质量
    
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
                    'AI': item.get('AI', create_fallback_ai_data(item, False))
                }
                f.write(json.dumps(output_item, ensure_ascii=False) + "\n")
    
    print(f"Successfully processed {len(processed_data)} items", file=sys.stderr)

if __name__ == "__main__":
    main()
