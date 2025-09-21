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
    def __init__(self, categories=None, date=None):
        """初始化 arXiv 爬虫（适配新版arxiv库）"""
        # 设置日志
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
            raise ValueError("至少需要指定一个类别 / At least one category is required")

        # 设置目标日期（使用 UTC 时间）
        if date is None:
            self.target_date = (datetime.now(ZoneInfo("UTC")) - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            self.target_date = date

        # 生成交叉学科组合
        cross_categories = ["cs.LG", "cs.AI"]
        self.target_category_pairs = [
            (cat, cross_cat) for cat in self.categories for cross_cat in cross_categories
        ]

        self.logger.info(f"目标类别对: {self.target_category_pairs}, 目标日期: {self.target_date}")

    def construct_query(self):
        """构造新版arXiv API的查询字符串，并直接包含日期范围"""
        # 转换日期格式
        target_date_obj = datetime.strptime(self.target_date, "%Y-%m-%d")
        start_date_str = target_date_obj.strftime("%Y%m%d")
        end_date_str = (target_date_obj + timedelta(days=1)).strftime("%Y%m%d")
        
        base_queries = []
        for target_cat, cross_cat in self.target_category_pairs:
            # 为每个类别对构造查询
            category_query = f"cat:{target_cat} AND cat:{cross_cat}"
            # 将日期范围添加到查询中
            full_query = f"({category_query}) AND submittedDate:[{start_date_str} TO {end_date_str}]"
            base_queries.append(full_query)
        
        # 用 OR 连接不同类别对的查询
        return " OR ".join(base_queries)

    def search_articles(self, max_results=200):
        """使用新版arXiv API搜索文章"""
        query = self.construct_query()
        self.logger.info(f"执行查询: {query}")

        try:
            # 使用推荐的 Client 方式
            client = arxiv.Client()
            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=SortCriterion.SubmittedDate,
                sort_order=SortOrder.Descending
            )
            
            # 获取结果
            results = []
            for result in client.results(search):
                # 转换为与后续代码兼容的字典格式
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
                
            return results

        except Exception as e:
            self.logger.error(f"搜索文章时出错: {str(e)}")
            return []

    def search_articles_with_retry(self, max_results=200, retries=3):
        """带重试的搜索函数"""
        for attempt in range(retries):
            try:
                return self.search_articles(max_results)
            except Exception as e:
                self.logger.warning(f"搜索尝试 {attempt+1}/{retries} 失败: {str(e)}")
                if attempt < retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    self.logger.info(f"{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    self.logger.error("所有重试均失败。")
                    return []
        return []

    def filter_by_categories(self, results):
        """确保文章确实同时包含目标类别对（二次验证）"""
        filtered_results = []
        for result in results:
            categories = result.get('categories', [])
            
            # 验证是否匹配目标类别对
            for target_cat, cross_cat in self.target_category_pairs:
                if target_cat in categories and cross_cat in categories:
                    # 提取arXiv ID
                    paper_id = re.sub(r'v\d+$', '', result.get('id', '').split('/')[-1])
                    
                    filtered_results.append({
                        "id": paper_id,
                        "title": result.get('title', '').replace('\n', ''),
                        "authors": [author.get('name', '') for author in result.get('authors', [])],
                        "summary": result.get('summary', '').replace('\n', ' '),
                        "published": self.target_date,
                        "categories": categories,
                        "pdf_url": result.get('pdf_url', ''),
                        "primary_category": categories[0] if categories else ""
                    })
                    break
        return filtered_results

    def run(self, output_file=None):
        """运行爬虫并保存结果"""
        self.logger.info("开始使用 arXiv API搜索文章...")

        try:
            # 使用带重试的搜索
            results = self.search_articles_with_retry(max_results=200, retries=3)
            filtered_results = self.filter_by_categories(results)
            self.logger.info(f"找到 {len(filtered_results)} 篇匹配的文章")

            for result in filtered_results:
                self.logger.info(f"找到文章: {result['id']}, 标题: {result['title']}, 类别: {result['categories']}")

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
    # 从环境变量获取输出路径
    yesterday = (datetime.now(ZoneInfo("UTC")) - timedelta(days=1)).strftime("%Y-%m-%d")
    output_file = os.environ.get("OUTPUT_FILE", f"data/{yesterday}.jsonl")

    # 创建并运行爬虫
    spider = ArxivAPISpider(
        categories=categories.split(","),
        date=yesterday
    )
    results = spider.run(output_file=output_file)

    # 打印结果摘要
    try:
        import pkg_resources
        print('arxiv_version:', pkg_resources.get_distribution("arxiv").version)
    except:
        print("无法获取arxiv库版本信息")
    
    print(f"\n找到 {len(results)} 篇文章:")
    for result in results:
        print(f"- {result['id']}: {result['title']}")
        print(f"  类别: {result['categories']}")
        print(f"  日期: {result['published']}\n")
