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
        """构造正确的查询字符串"""
        base_queries = []
        for target_cat in self.categories:
            # 对于每个主类别，构造 (cs.LG OR cs.AI) 的子查询
            cross_query = " OR ".join([f"cat:{cross_cat}" for cross_cat in ["cs.LG", "cs.AI"]])
            base_queries.append(f"cat:{target_cat} AND ({cross_query})")
       
        return " OR ".join(base_queries)

    def search_articles_simple(self, max_results=200):
        """简单搜索，不进行复杂的分页控制"""
        query = self.construct_query()
        self.logger.info(f"执行查询: {query}")
        try:
            client = arxiv.Client()
            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=SortCriterion.SubmittedDate,
                sort_order=SortOrder.Descending
            )
           
            results = []
            for result in client.results(search):
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
                results.append(result_dict)
               
            self.logger.info(f"查询返回 {len(results)} 条原始结果")
            return results
        except Exception as e:
            self.logger.error(f"搜索文章时出错: {str(e)}")
            return []

    def search_multiple_queries(self, max_results_per_query=100):
        """为每个类别对分别执行查询，避免复杂查询语法问题"""
        all_results = []
       
        for target_cat, cross_cat in self.target_category_pairs:
            query = f"cat:{target_cat} AND cat:{cross_cat}"
            self.logger.info(f"执行子查询: {query}")
           
            try:
                client = arxiv.Client()
                search = arxiv.Search(
                    query=query,
                    max_results=max_results_per_query,
                    sort_by=SortCriterion.SubmittedDate,
                    sort_order=SortOrder.Descending
                )
               
                batch_results = []
                for result in client.results(search):
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
                    batch_results.append(result_dict)
               
                self.logger.info(f"子查询 '{query}' 返回 {len(batch_results)} 条结果")
                all_results.extend(batch_results)
               
                # 避免请求过于频繁
                time.sleep(1)
               
            except Exception as e:
                self.logger.error(f"子查询 '{query}' 出错: {str(e)}")
                continue
       
        # 去重
        unique_results = []
        seen_ids = set()
        for result in all_results:
            paper_id = re.sub(r'v\d+$', '', result.get('id', '').split('/')[-1])
            if paper_id not in seen_ids:
                seen_ids.add(paper_id)
                unique_results.append(result)
       
        return unique_results

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

    def run(self, output_file=None, strategy="multiple"):
        """运行爬虫"""
        self.logger.info(f"开始搜索最近 {self.days} 天的文章...")
        try:
            if strategy == "multiple":
                results = self.search_multiple_queries(max_results_per_query=150)
            else:
                results = self.search_articles_simple(max_results=300)
           
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
            
            # 如果未提供 output_file，生成以当天日期命名的文件
            if not output_file:
                today = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d')
                output_file = f"data/{today}.json"
            
            # 确保输出目录存在并写入文件
            self.logger.info(f"Attempting to write to {output_file}")
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
   
    # 从环境变量获取天数，默认为5天
    days = int(os.environ.get("DAYS", "3"))
   
    # 生成输出文件名，使用当天日期
    today = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d')
    output_file = os.environ.get("OUTPUT_FILE", f"data/{today}.json")
   
    # 创建并运行爬虫
    spider = ArxivAPISpider(
        categories=categories.split(","),
        days=days
    )
   
    # 使用多查询策略，更稳定
    results = spider.run(output_file=output_file, strategy="multiple")
   
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
            print(f" - {result['id']}: {result['title'][:60]}...")
            print(f" 类别: {result['categories']}")
