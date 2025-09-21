import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
import dotenv
import argparse
from tqdm import tqdm
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint
from langchain.prompts import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)

if os.path.exists('.env'):
    dotenv.load_dotenv()

template = open("template.txt", "r").read()
system = open("system.txt", "r").read()

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, required=True, help="jsonline data file")
    parser.add_argument("--max_workers", type=int, default=1, help="Maximum number of parallel workers")
    return parser.parse_args()

def process_single_item(chain, item: Dict, language: str) -> Dict:
    """处理单个数据项"""
    try:
        response = chain.invoke({
            "language": language,
            "content": item['summary']
        })
        # HuggingFace may not directly support structured output, so parse if necessary
        if isinstance(response, dict):
            item['AI'] = response
        else:
            # Assume response is a string containing JSON (common for HF models)
            try:
                item['AI'] = json.loads(response)
            except json.JSONDecodeError:
                item['AI'] = {
                    "tldr": response,  # Fallback to raw response
                    "motivation": "Error",
                    "method": "Error",
                    "result": "Error",
                    "conclusion": "Error"
                }
    except Exception as e:
        print(f"Failed to process {item['id']}: {e}", file=sys.stderr)
        item['AI'] = {
            "tldr": "Error",
            "motivation": "Error",
            "method": "Error",
            "result": "Error",
            "conclusion": "Error"
        }
    return item

def process_all_items(data: List[Dict], model_name: str, language: str, max_workers: int) -> List[Dict]:
    """并行处理所有数据项"""
    # Initialize Hugging Face model
    try:
        llm_endpoint = HuggingFaceEndpoint(
            repo_id=model_name,
            huggingfacehub_api_token=os.environ.get("HUGGINGFACE_API_KEY"),
            task="text-generation",
            max_new_tokens=512,
            temperature=0.7,
        )
        llm = ChatHuggingFace(llm=llm_endpoint)
        print(f'Connected to Hugging Face model: {model_name}', file=sys.stderr)
    except Exception as e:
        print(f"Failed to initialize HuggingFace model {model_name}: {e}", file=sys.stderr)
        sys.exit(1)

    prompt_template = ChatPromptTemplate.from_messages([
        SystemMessagePromptTemplate.from_template(system),
        HumanMessagePromptTemplate.from_template(template=template)
    ])
    chain = prompt_template | llm
    # 使用线程池并行处理
    processed_data = [None] * len(data)  # 预分配结果列表
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(process_single_item, chain, item, language): idx
            for idx, item in enumerate(data)
        }
        for future in tqdm(
            as_completed(future_to_idx),
            total=len(data),
            desc="Processing items"
        ):
            idx = future_to_idx[future]
            try:
                result = future.result()
                processed_data[idx] = result
            except Exception as e:
                print(f"Item at index {idx} generated an exception: {e}", file=sys.stderr)
                processed_data[idx] = data[idx]
    return processed_data

def main():
    args = parse_args()
    model_name = os.environ.get("MODEL_NAME", 'meta-llama/Llama-3.1-8B-Instruct')
    language = os.environ.get("LANGUAGE", 'Chinese')
    
    # 确保输入文件存在
    if not os.path.exists(args.data):
        print(f"Error: Input file {args.data} does not exist", file=sys.stderr)
        sys.exit(1)
    
    # 计算输出文件名，确保基于输入文件名生成正确的输出路径
    base_name = os.path.splitext(args.data)[0]  # 去掉扩展名
    target_file = f"{base_name}_AI_enhanced_{language}.jsonl"
    
    print(f"Input file: {args.data}", file=sys.stderr)
    print(f"Target output file: {target_file}", file=sys.stderr)
    
    # 如果输出文件存在，删除它
    if os.path.exists(target_file):
        os.remove(target_file)
        print(f"Removed existing output file: {target_file}", file=sys.stderr)
    
    # 读取输入文件
    data = []
    print(f'Opening input file: {args.data}', file=sys.stderr)
    with open(args.data, "r", encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    
    # 去重
    seen_ids = set()
    unique_data = []
    for item in data:
        if item['id'] not in seen_ids:
            seen_ids.add(item['id'])
            unique_data.append(item)
    data = unique_data
    print(f'Loaded {len(data)} unique items from {args.data}', file=sys.stderr)
    
    # 处理数据
    processed_data = process_all_items(
        data,
        model_name,
        language,
        args.max_workers
    )
    
    # 写入输出文件
    print(f'Writing to output file: {target_file}', file=sys.stderr)
    with open(target_file, "w", encoding='utf-8') as f:
        for item in processed_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Successfully wrote {len(processed_data)} items to {target_file}", file=sys.stderr)

if __name__ == "__main__":
    main()
