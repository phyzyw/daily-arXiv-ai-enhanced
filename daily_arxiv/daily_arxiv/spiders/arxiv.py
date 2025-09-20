import os
import logging
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pymed import PubMed

class ArxivAPISpider:
    def __init__(self, categories=None, date=None):
        """
        初始化 Arxiv API 爬虫

        Args:
            categories: 用户指定的类别列表，如 ['cs.CV', 'cs.CL']
            date: 目标日期，格式为 'YYYY-MM-DD'
        """
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

        # 初始化PubMed客户端（支持arXiv）
        self.pubmed = PubMed(tool="ArxivSpider", email="your@email.com")  # 替换为你的邮箱

        self.logger.info(f"目标类别对: {self.target_category_pairs}, 目标日期: {self.target_date}")

    def construct_query(self):
        """构造 API 查询字符串"""
        queries = []
        for target_cat, cross_cat in self.target_category_pairs:
            # 在pymed中，arXiv类别需要用abs:前缀
            query = f"abs:{target_cat} AND abs:{cross_cat}"
            queries.append(query)
        combined_query = " OR ".join([f"({q})" for q in queries])
        # 限制来源为arXiv
        combined_query += " AND source:arXiv"
        return combined_query

    def search_articles(self, max_results=1000):
        """搜索文章"""
        query = self.construct_query()
        self.logger.info(f"执行查询: {query}")

        try:
            # 计算日期范围（目标日期的00:00到23:59）
            target_date_obj = datetime.strptime(self.target_date, "%Y-%m-%d")
            next_day = target_date_obj + timedelta(days=1)
            
            # 执行搜索
            results = self.pubmed.query(
                query,
                max_results=max_results,
                mindate=target_date_obj.strftime("%Y/%m/%d"),
                maxdate=next_day.strftime("%Y/%m/%d")
            )
            return list(results)
        except Exception as e:
            self.logger.error(f"搜索文章时出错: {str(e)}")
            return []

    def filter_by_date(self, results):
        """按日期筛选结果"""
        filtered_results = []
        target_date_obj = datetime.strptime(self.target_date, "%Y-%m-%d").date()
        
        for result in results:
            # 处理发布日期
            published_date = result.publication_date.date() if result.publication_date else None
            if published_date != target_date_obj:
                continue
                
            # 提取类别信息
            categories = []
            if hasattr(result, 'keywords') and result.keywords:
                categories = [kw for kw in result.keywords if kw.startswith('cs.')]
                
            # 检查是否匹配目标类别对
            for target_cat, cross_cat in self.target_category_pairs:
                if target_cat in categories and cross_cat in categories:
                    # 提取论文ID（从PMID或标题中提取）
                    paper_id = result.pmid if result.pmid else result.title[:10].replace(' ', '')
                    
                    filtered_results.append({
                        "id": paper_id,
                        "title": result.title,
                        "authors": [author['name'] for author in result.authors] if result.authors else [],
                        "summary": result.abstract,
                        "published": self.target_date,
                        "categories": categories,
                        "pdf_url": f"https://arxiv.org/pdf/{paper_id}.pdf" if paper_id else "",
                        "primary_category": categories[0] if categories else ""
                    })
                    break
        return filtered_results

    def run(self, output_file=None):
        """运行爬虫并可选地将结果保存到文件"""
        self.logger.info("开始使用 arXiv API 搜索文章...")

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
    
