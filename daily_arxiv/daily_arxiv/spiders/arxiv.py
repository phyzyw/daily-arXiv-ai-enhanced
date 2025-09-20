import os
import logging
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import arxiv  # 使用专门的arXiv库

class ArxivAPISpider:
    def __init__(self, categories=None, date=None):
        """初始化 arXiv 爬虫（使用官方库）"""
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
        """构造符合arXiv语法的查询字符串"""
        queries = []
        for target_cat, cross_cat in self.target_category_pairs:
            # arXiv查询语法：category:"cs.CV" AND category:"cs.LG"
            query = f'category:"{target_cat}" AND category:"{cross_cat}"'
            queries.append(query)
        return " OR ".join([f"({q})" for q in queries])

    def search_articles(self, max_results=1000):
        """使用arXiv官方库搜索文章"""
        query = self.construct_query()
        self.logger.info(f"执行查询: {query}")

        try:
            # 转换日期格式为datetime对象（arXiv库要求）
            target_date_obj = datetime.strptime(self.target_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC"))
            next_day = target_date_obj + timedelta(days=1)

            # 使用arXiv官方API搜索
            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending,
                submitted_after=target_date_obj,  # 大于等于目标日期
                submitted_before=next_day         # 小于次日（即只包含目标日期）
            )

            # 执行搜索并返回结果
            return list(search.results())

        except Exception as e:
            self.logger.error(f"搜索文章时出错: {str(e)}")
            return []

    def filter_by_date(self, results):
        """按日期和类别筛选结果（双重验证）"""
        filtered_results = []
        target_date_str = self.target_date
        
        for result in results:
            # 验证提交日期
            submitted_date = result.published.strftime("%Y-%m-%d")
            if submitted_date != target_date_str:
                continue
                
            # 提取类别（arXiv类别格式如['cs.CV', 'cs.LG']）
            categories = result.categories
                
            # 验证是否匹配目标类别对
            for target_cat, cross_cat in self.target_category_pairs:
                if target_cat in categories and cross_cat in categories:
                    # 提取arXiv ID（如从'2301.01234v1'中提取'2301.01234'）
                    paper_id = re.sub(r'v\d+$', '', result.get_short_id())
                    
                    filtered_results.append({
                        "id": paper_id,
                        "title": result.title,
                        "authors": [str(author) for author in result.authors],
                        "summary": result.summary,
                        "published": target_date_str,
                        "categories": categories,
                        "pdf_url": result.pdf_url,
                        "primary_category": categories[0] if categories else ""
                    })
                    break
        return filtered_results

    def run(self, output_file=None):
        """运行爬虫并保存结果"""
        self.logger.info("开始使用 arXiv 官方API搜索文章...")

        try:
            results = self.search_articles(max_results=1000)
            filtered_results = self.filter_by_date(results)
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
    print(f"\n找到 {len(results)} 篇文章:")
    for result in results:
        print(f"- {result['id']}: {result['title']}")
        print(f" 类别: {result['categories']}")
        print(f" 日期: {result['published']}\n")
    
