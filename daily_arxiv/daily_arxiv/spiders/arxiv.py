import os
import logging
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import arxiv  # 兼容0.x版本的arxiv库

class ArxivAPISpider:
    def __init__(self, categories=None, date=None):
        """初始化 arXiv 爬虫（兼容旧版本库）"""
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
        """构造符合旧版arXiv API的查询字符串"""
        queries = []
        for target_cat, cross_cat in self.target_category_pairs:
            # 旧版API使用的类别查询格式
            queries.append(f"cat:{target_cat}+AND+cat:{cross_cat}")
        return "+OR+".join(queries)

    def search_articles(self, max_results=1000):
        """使用旧版arXiv API搜索文章"""
        query = self.construct_query()
        self.logger.info(f"执行查询: {query}")

        try:
            # 转换日期格式（旧版API使用YYYYMMDD格式）
            target_date_obj = datetime.strptime(self.target_date, "%Y-%m-%d")
            start_date = target_date_obj.strftime("%Y%m%d")
            end_date = (target_date_obj + timedelta(days=1)).strftime("%Y%m%d")

            # 旧版API使用arxiv.query()方法
            # 参数说明：
            # - id_list: 为空表示不按ID筛选
            # - query: 搜索关键词/类别
            # - start: 起始索引
            # - max_results: 最大结果数
            # - sortBy: 排序字段（submittedDate表示按提交日期）
            # - sortOrder: 排序方向（desc表示降序）
            # - date_from/date_to: 日期范围（YYYYMMDD格式）
            results = arxiv.Search(
                id_list=[],
                query=query,
                start=0,
                max_results=max_results,
                sortBy="submittedDate",
                sortOrder="descending",
                date_from=start_date,
                date_to=end_date
            )

            return results

        except Exception as e:
            self.logger.error(f"搜索文章时出错: {str(e)}")
            return []

    def filter_by_date(self, results):
        """按日期和类别筛选结果"""
        filtered_results = []
        target_date_str = self.target_date
        
        for result in results:
            # 验证提交日期（旧版API返回的是UTC时间字符串）
            published_str = result.get('published', '')[:10]  # 取YYYY-MM-DD部分
            if published_str != target_date_str:
                continue
                
            # 提取类别
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
                        "published": target_date_str,
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
    
