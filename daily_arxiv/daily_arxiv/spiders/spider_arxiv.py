import os
import logging
import json
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import arxiv
from arxiv import SortCriterion, SortOrder

class ArxivAPISpider:
    def __init__(self, categories=None, days=3):
        """初始化 arXiv 爬虫，搜索最近几天的文章"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # 设置类别
        if categories is None:
            categories = os.environ.get("CATEGORIES", "")
            self.categories = [cat.strip() for cat in categories.split(",")] if categories else []
        else:
            self.categories = categories

        if not self.categories:
            raise ValueError("至少需要指定一个类别")

        # 设置搜索天数
        self.days = days
        self.end_date = datetime.now(ZoneInfo("UTC"))
        self.start_date = self.end_date - timedelta(days=days)
        
        self.logger.info(f"搜索时间范围: {self.start_date.strftime('%Y-%m-%d')} 到 {self.end_date.strftime('%Y-%m-%d')}")

        # 生成交叉学科组合
        cross_categories = ["cs.LG", "cs.AI"]
        self.target_category_pairs = [
            (cat, cross_cat) for cat in self.categories for cross_cat in cross_categories
        ]

        self.logger.info(f"目标类别对: {self.target_category_pairs}")

    def construct_query(self):
        """构造正确的查询字符串 - 修复逻辑错误"""
        # 正确的查询逻辑: (cs.CV AND (cs.LG OR cs.AI))
        base_queries = []
        for target_cat in self.categories:
            # 对于每个主类别，构造 (cs.LG OR cs.AI) 的子查询
            cross_query = " OR ".join([f"cat:{cross_cat}" for cross_cat in ["cs.LG", "cs.AI"]])
            base_queries.append(f"cat:{target_cat} AND ({cross_query})")
        
        return " OR ".join(base_queries)

    def search_articles_with_pagination(self, max_results=300):
        """带分页控制的搜索，避免API限制"""
        query = self.construct_query()
        self.logger.info(f"执行查询: {query}")

        all_results = []
        max_retries = 3
        batch_size = 100  # 每次获取100条
        
        try:
            client = arxiv.Client()
            
            # 分批获取结果，避免分页错误
            for start in range(0, max_results, batch_size):
                remaining = max_results - start
                current_batch_size = min(batch_size, remaining)
                
                if current_batch_size <= 0:
                    break
                
                self.logger.info(f"获取第 {start} 到 {start + current_batch_size - 1} 条结果")
                
                search = arxiv.Search(
                    query=query,
                    max_results=current_batch_size,
                    start=start,
                    sort_by=SortCriterion.SubmittedDate,
                    sort_order=SortOrder.Descending
                )
                
                # 带重试机制的获取
                for attempt in range(max_retries):
                    try:
                        batch_results = list(client.results(search))
                        if not batch_results:
                            self.logger.info(f"第 {start} 批没有更多结果，停止搜索")
                            return all_results
                        
                        for result in batch_results:
                            result_dict = {
                                'id': result.entry_id,
                                'title': result.title,
                                'authors': [{'name': author.name} for author in result.authors],
                                'summary': result.summary,
                                'published': result.published.isoformat(),
                                'categories': [str(cat) for cat in result.categories],
                                'pdf_url': result.pdf_url,
                                'primary_category': str(result.primary_category) if result.primary_category else ""
                            }
                            all_results.append(result_dict)
                        
                        self.logger.info(f"成功获取第 {start} 批的 {len(batch_results)} 条结果")
                        break
                        
                    except Exception as e:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            self.logger.warning(f"第 {start} 批获取失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                            self.logger.info(f"{wait_time}秒后重试...")
                            time.sleep(wait_time)
                        else:
                            self.logger.error(f"第 {start} 批所有重试均失败: {str(e)}")
                            return all_results
                
                # 检查是否已经获取到足够旧的结果
                if len(all_results) > 0:
                    oldest_result_date = datetime.fromisoformat(all_results[-1]['published'].replace('Z', '+00:00'))
                    if oldest_result_date < self.start_date:
                        self.logger.info(f"已获取到足够旧的结果 ({oldest_result_date.strftime('%Y-%m-%d')})，停止搜索")
                        break
                
                # 避免请求过于频繁
                time.sleep(1)
                
            return all_results

        except Exception as e:
            self.logger.error(f"搜索文章时出错: {str(e)}")
            return all_results

    def filter_articles_by_date_range(self, results):
        """按日期范围筛选结果"""
        filtered_results = []
        
        for result in results:
            # 提取发布时间
            published_str = result.get('published', '')
            try:
                published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                continue
                
            # 检查是否在目标日期范围内
            if self.start_date <= published_date <= self.end_date:
                # 验证类别
                categories = result.get('categories', [])
                found_pair = False
                for target_cat, cross_cat in self.target_category_pairs:
                    if target_cat in categories and cross_cat in categories:
                        found_pair = True
                        break
                
                if found_pair:
                    # 提取arXiv ID
                    paper_id = re.sub(r'v\d+$', '', result.get('id', '').split('/')[-1])
                    
                    filtered_results.append({
                        "id": paper_id,
                        "title": result.get('title', '').replace('\n', ''),
                        "authors": [author.get('name', '') for author in result.get('authors', [])],
                        "summary": result.get('summary', '').replace('\n', ' '),
                        "published": published_date.strftime("%Y-%m-%d"),
                        "categories": categories,
                        "pdf_url": result.get('pdf_url', ''),
                        "primary_category": categories[0] if categories else ""
                    })
        
        return filtered_results

    def group_results_by_date(self, results):
        """按日期分组结果"""
        grouped = {}
        for result in results:
            date = result['published']
            if date not in grouped:
                grouped[date] = []
            grouped[date].append(result)
        return grouped

    def run(self, output_file=None):
        """运行爬虫"""
        self.logger.info(f"开始搜索最近 {self.days} 天的文章...")

        try:
            results = self.search_articles_with_pagination(max_results=500)
            self.logger.info(f"总共获取到 {len(results)} 条原始结果")
            
            filtered_results = self.filter_articles_by_date_range(results)
            
            # 按日期分组
            grouped_results = self.group_results_by_date(filtered_results)
            
            self.logger.info(f"找到 {len(filtered_results)} 篇匹配的文章")
            
            # 按日期打印统计信息
            for date, articles in grouped_results.items():
                self.logger.info(f"日期 {date}: {len(articles)} 篇文章")
            
            for result in filtered_results:
                self.logger.info(f"找到文章: {result['id']}, 日期: {result['published']}, 标题: {result['title'][:50]}...")

            if output_file:
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    for result in filtered_results:
                        json.dump(result, f, ensure_ascii=False)
                        f.write('\n')
                self.logger.info(f"结果已保存到 {output_file}")

            return filtered_results

        except Exception as e:
            self.logger.error(f"搜索过程中发生错误: {str(e)}")
            return []

if __name__ == "__main__":
    # 从环境变量获取类别，或使用默认值
    categories = os.environ.get("CATEGORIES", "cs.CV,cs.CL")
    
    # 从环境变量获取天数，默认为3天
    days = int(os.environ.get("DAYS", "5"))
    
    # 生成输出文件名，包含日期范围
    end_date = datetime.now(ZoneInfo("UTC"))
    start_date = end_date - timedelta(days=days)
    date_range_str = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    output_file = os.environ.get("OUTPUT_FILE", f"data/last_{days}_days_{date_range_str}.jsonl")

    # 创建并运行爬虫
    spider = ArxivAPISpider(
        categories=categories.split(","),
        days=days
    )
    
    results = spider.run(output_file=output_file)

    # 打印结果摘要
    print(f"\n找到 {len(results)} 篇文章 (最近 {days} 天):")
    
    # 按日期分组显示
    grouped_results = {}
    for result in results:
        date = result['published']
        if date not in grouped_results:
            grouped_results[date] = []
        grouped_results[date].append(result)
    
    for date, articles in sorted(grouped_results.items(), reverse=True):
        print(f"\n📅 {date} ({len(articles)} 篇):")
        for result in articles:
            print(f"  - {result['id']}: {result['title'][:60]}...")
            print(f"    类别: {result['categories']}")
    
    if not results:
        print("未找到符合条件的文章。")
        print("建议:")
        print("1. 检查网络连接")
        print("2. 等待几分钟后重试（arXiv API可能有速率限制）")
        print("3. 手动验证查询: https://arxiv.org/search/?query=cat%3Acs.CL+AND+(cat%3Acs.LG+OR+cat%3Acs.AI)&searchtype=all&abstracts=show&order=-submitted_date&size=50")
