import arxiv
import os
import logging
from datetime import datetime, timedelta

class ArxivAPISpider:
    def __init__(self, categories=None, date=None):
        """
        初始化Arxiv API爬虫
        
        Args:
            categories: 用户指定的类别列表，如 ['cs.CV', 'cs.CL']
            date: 目标日期，格式为 'YYYY-MM-DD'
        """
        # 设置类别
        if categories is None:
            categories = os.environ.get("CATEGORIES", "")
            self.categories = [cat.strip() for cat in categories.split(",")] if categories else []
        else:
            self.categories = categories
            
        if not self.categories:
            raise ValueError("至少需要指定一个类别")
        
        # 设置目标日期
        if date is None:
            self.target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            self.target_date = date
            
        # 生成交叉学科组合
        cross_categories = ["cs.LG", "cs.AI"]
        self.target_category_pairs = [
            (cat, cross_cat) for cat in self.categories for cross_cat in cross_categories
        ]
        
        # 设置API客户端
        self.client = arxiv.Client(
            page_size=100,
            delay_seconds=3.0,  # 遵守arXiv的API使用礼仪
            num_retries=3
        )
        
        logging.info(f"目标类别对: {self.target_category_pairs}, 目标日期: {self.target_date}")

    def construct_query(self):
        """构造API查询字符串"""
        queries = []
        
        # 为每个类别对创建查询
        for target_cat, cross_cat in self.target_category_pairs:
            # 查找同时属于两个类别的文章
            query = f"cat:{target_cat} AND cat:{cross_cat}"
            queries.append(query)
        
        # 添加日期筛选 - 注意：arXiv API不支持直接按日期筛选，我们稍后处理
        # 将多个查询组合成一个
        combined_query = " OR ".join([f"({q})" for q in queries])
        
        # 添加日期范围筛选（通过提交日期）
        # 注意：arXiv API不直接支持按日期筛选，所以我们先获取更多结果后再过滤
        return combined_query

    def search_articles(self, max_results=500):
        """搜索文章"""
        query = self.construct_query()
        logging.info(f"执行查询: {query}")
        
        search = arxiv.Search(
            query=query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )
        
        results = self.client.results(search)
        return results

    def filter_by_date(self, results):
        """按日期筛选结果"""
        filtered_results = []
        
        for result in results:
            # 将arXiv的日期转换为字符串格式
            published_date = result.published.strftime("%Y-%m-%d")
            
            if published_date == self.target_date:
                # 提取所有类别
                categories = [cat.term for cat in result.categories]
                
                # 检查是否匹配任何目标类别对
                for target_cat, cross_cat in self.target_category_pairs:
                    if target_cat in categories and cross_cat in categories:
                        filtered_results.append({
                            "id": result.entry_id.split('/')[-1],  # 提取arXiv ID
                            "title": result.title,
                            "authors": [author.name for author in result.authors],
                            "summary": result.summary,
                            "published": published_date,
                            "categories": categories,
                            "pdf_url": result.pdf_url,
                            "primary_category": result.primary_category if hasattr(result, 'primary_category') else categories[0] if categories else ""
                        })
                        break
        
        return filtered_results

    def run(self):
        """运行爬虫"""
        logging.info("开始使用arXiv API搜索文章...")
        
        try:
            # 搜索文章
            results = self.search_articles(max_results=1000)
            
            # 按日期筛选
            filtered_results = self.filter_by_date(results)
            
            logging.info(f"找到 {len(filtered_results)} 篇匹配的文章")
            
            # 输出结果
            for result in filtered_results:
                logging.info(f"找到文章: {result['id']}, 标题: {result['title']}, 类别: {result['categories']}")
                
            return filtered_results
            
        except Exception as e:
            logging.error(f"搜索过程中发生错误: {str(e)}")
            return []

# 使用示例
if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 从环境变量获取类别，或使用默认值
    categories = os.environ.get("CATEGORIES", "cs.CV,cs.CL")
    
    # 计算昨天的日期
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 创建并运行爬虫
    spider = ArxivAPISpider(
        categories=categories.split(","),
        date=yesterday
    )
    
    results = spider.run()
    
    # 打印结果摘要
    print(f"\n找到 {len(results)} 篇文章:")
    for result in results:
        print(f"- {result['id']}: {result['title']}")
        print(f"  类别: {result['categories']}")
        print(f"  日期: {result['published']}\n")
