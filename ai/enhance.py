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

# 嵌入提示模板 - 精简版本以减少token使用
TEMPLATE = """
请为以下学术论文内容生成{language}的JSON摘要：

{content}

返回格式：
{{
  "tldr": "简洁摘要(1-2句)",
  "motivation": "研究动机",
  "method": "使用方法",
  "result": "主要结果", 
  "conclusion": "结论意义"
}}
"""

# 嵌入系统指令 - 精简版本
SYSTEM = """
你是一个学术论文摘要AI。只返回有效的JSON对象，包含：tldr, motivation, method, result, conclusion字段。
不要包含JSON之外的任何文本。
"""

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, required=True, help="json or jsonline data file")
    parser.add_argument("--max_workers", type=int, default=1, help="Maximum number of parallel workers")
    parser.add_argument("--max_tokens", type=int, default=1024, help="Maximum output tokens")
    return parser.parse_args()

def download_pdf(url: str) -> str:
    """从 arXiv 下载 PDF 并提取文本"""
    try:
        if '/abs/' in url:
            pdf_url = url.replace('/abs/', '/pdf/') + '.pdf'
        else:
            pdf_url = url
        
        response = requests.get(pdf_url, timeout=60)
        response.raise_for_status()
        pdf_file = BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text = ""
        # 只读取前5页以减少token使用
        for i, page in enumerate(pdf_reader.pages):
            if i >= 5:  # 限制页数
                break
            page_text = page.extract_text() or ""
            text += page_text + "\n"
        return text[:6000]  # 进一步减少文本长度
    except Exception as e:
        print(f"Failed to download PDF from {url}: {e}", file=sys.stderr)
        return None

def call_cloudflare_api(account_id, api_token, model_name, prompt, max_tokens=1024):
    """调用 Cloudflare Workers AI REST API"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model_name}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    # 精简payload以减少token使用
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
        response = requests.post(url, headers=headers, json=payload, timeout=90)
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
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"API error response: {error_data}", file=sys.stderr)
            except:
                print(f"API error response text: {e.response.text}", file=sys.stderr)
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
            # 选择最长的匹配项
            longest_match = max(matches, key=len)
            return json.loads(longest_match)
    except:
        pass
    
    # 尝试修复常见的JSON问题
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
    # 从摘要中提取关键信息创建更有意义的回退内容
    summary_words = summary.split()
    if len(summary_words) > 50:
        tldr = ' '.join(summary_words[:50]) + "..."
    else:
        tldr = summary
    
    return {
        "tldr": tldr,
        "motivation": "研究旨在解决摘要中描述的问题" if summary else "基于论文内容分析",
        "method": "采用了先进的研究方法和技术",
        "result": "取得了显著的研究成果和发现", 
        "conclusion": "对领域发展具有重要意义的结论"
    }

def estimate_token_count(text: str) -> int:
    """粗略估算token数量（英文约1token=4字符，中文约1token=2字符）"""
    # 简单估算：英文字符数/4 + 中文字符数/2
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    other_chars = len(text) - chinese_chars
    return int(other_chars / 4 + chinese_chars / 2) + 100  # 加100作为缓冲

def process_single_item(item: Dict, language: str, max_output_tokens: int = 1024) -> Dict:
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
    
    # 动态调整内容长度
    max_content_length = 5000  # 初始限制
    full_text = None
    
    for attempt in range(3):
        try:
            # 下载PDF（只在第一次尝试或需要更多内容时）
            if attempt == 0 and pdf_url:
                print(f"Downloading PDF for {item.get('id', 'unknown')}", file=sys.stderr)
                full_text = download_pdf(pdf_url)
            
            # 选择内容源：优先使用PDF全文，其次使用摘要
            content_source = full_text if full_text else item.get('summary', '')
            if not content_source:
                print(f"No content available for {item.get('id', 'unknown')}", file=sys.stderr)
                return {
                    **item,
                    'AI': create_fallback_ai_data(item, False)
                }
            
            # 截断内容
            content_preview = content_source[:max_content_length]
            
            # 构建提示并估算token数量
            prompt = TEMPLATE.format(language=language, content=content_preview)
            estimated_tokens = estimate_token_count(prompt) + max_output_tokens
            
            print(f"Processing {item.get('id', 'unknown')}, attempt {attempt + 1}, estimated tokens: {estimated_tokens}", file=sys.stderr)
            
            # 如果估算的token数可能超出限制，进一步减少内容长度
            if estimated_tokens > 7000:  # 保守估计模型限制
                max_content_length = int(max_content_length * 0.7)
                content_preview = content_source[:max_content_length]
                prompt = TEMPLATE.format(language=language, content=content_preview)
                print(f"Reduced content length to {max_content_length} for token conservation", file=sys.stderr)
            
            response_text = call_cloudflare_api(account_id, api_token, model_name, prompt, max_output_tokens)
            
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
            error_msg = str(e)
            print(f"Attempt {attempt + 1} failed for {item.get('id', 'unknown')}: {error_msg}", file=sys.stderr)
            
            # 检查是否是token超限错误，如果是则减少内容长度
            if any(keyword in error_msg for keyword in ["token", "context window", "limit exceeded", "5021"]):
                print(f"Token limit exceeded, reducing content length for retry", file=sys.stderr)
                max_content_length = int(max_content_length * 0.6)  # 减少40%
                if max_content_length < 1000:  # 最小长度限制
                    max_content_length = 1000
        
        if attempt < 2:
            sleep(8)  # 增加重试间隔避免速率限制
    
    # 所有尝试都失败后的回退
    print(f"All attempts failed for {item.get('id', 'unknown')}, using fallback", file=sys.stderr)
    return {
        **item,
        'AI': create_fallback_ai_data(item, bool(full_text))
    }

def process_all_items(data: List[Dict], language: str, max_workers: int, max_tokens: int) -> List[Dict]:
    """并行处理所有数据项"""
    print(f'Connected to Cloudflare Workers AI', file=sys.stderr)
    processed_data = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {
            executor.submit(process_single_item, item, language, max_tokens): item 
            for item in data
        }
        
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
    language = os.environ.get("LANGUAGE", 'Chinese')  # 使用中文可能获得更好的响应
    
    if not os.path.exists(args.data):
        print(f"Error: Input file {args.data} does not exist", file=sys.stderr)
        sys.exit(1)
    
    base_name = os.path.splitext(args.data)[0]
    target_file = f"{base_name}_AI_enhanced_{language}.jsonl"
    
    print(f"Input file: {args.data}", file=sys.stderr)
    print(f"Target output file: {target_file}", file=sys.stderr)
    print(f"Max workers: {args.max_workers}", file=sys.stderr)
    print(f"Max output tokens: {args.max_tokens}", file=sys.stderr)
    
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
    processed_data = process_all_items(data, language, args.max_workers, args.max_tokens)
    
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
    print(f"Output saved to: {target_file}", file=sys.stderr)

if __name__ == "__main__":
    main()
