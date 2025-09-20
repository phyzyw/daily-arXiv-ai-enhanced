import scrapy
import os
import re
from datetime import datetime

class ArxivSpider(scrapy.Spider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Get user-specified categories from CATEGORIES environment variable
        categories = os.environ.get("CATEGORIES")
        categories = [cat.strip() for cat in categories.split(",")]
        
        # Generate cross-disciplinary combinations with cs.LG and cs.AI
        cross_categories = ["cs.LG", "cs.AI"]
        self.target_category_pairs = [(cat, cross_cat) for cat in categories for cross_cat in cross_categories]
        
        # Create start URLs for all user-specified categories
        self.start_urls = [
            f"https://arxiv.org/list/{cat}/new" for cat in categories
        ]
        
        # Get target date from environment or default to today
        self.target_date = kwargs.get('date', datetime.now().strftime("%Y-%m-%d"))
        self.logger.info(f"Target category pairs: {self.target_category_pairs}, Target date: {self.target_date}")

    name = "arxiv"
    allowed_domains = ["arxiv.org"]

    def parse(self, response):
        # Extract the current category from the URL
        current_category = response.url.split("/list/")[-1].split("/")[0]
        
        for paper in response.css("dl dt"):
            # Extract paper ID
            paper_anchor = paper.css("a[name^='item']::attr(name)").get()
            if not paper_anchor:
                continue
            paper_id = paper_anchor.replace("item", "")
            
            # Extract submission date
            submit_date = paper.css(".dateline::text").get()
            if submit_date:
                date_parts = submit_date.strip().split()[-3:]  # e.g., "19 Sep 2025"
                try:
                    parsed_date = datetime.strptime(' '.join(date_parts), '%d %b %Y').strftime("%Y-%m-%d")
                    if parsed_date != self.target_date:
                        self.logger.debug(f"Skipped paper {paper_id} due to date mismatch: {parsed_date} != {self.target_date}")
                        continue
                except ValueError:
                    self.logger.warning(f"Could not parse date for paper {paper_id}: {submit_date}")
                    continue
            
            # Extract abstract link and arXiv ID
            abstract_link = paper.css("a[title='Abstract']::attr(href)").get()
            if not abstract_link:
                continue
            arxiv_id = abstract_link.split("/")[-1]
            
            # Get the corresponding dd element
            paper_dd = paper.xpath("following-sibling::dd[1]")
            if not paper_dd:
                continue
            
            # Extract all categories from .list-subjects span
            subjects_text = paper_dd.css(".list-subjects span::text").getall()
            paper_categories = set()
            if subjects_text:
                for text in subjects_text:
                    # Extract categories from text like "(cs.CV) (cs.LG) (cs.AI)"
                    categories = re.findall(r'\(([^)]+)\)', text)
                    paper_categories.update(categories)
            else:
                self.logger.warning(f"Could not extract categories for paper {arxiv_id}, skipping")
                continue
            
            # Check if the paper belongs to at least one target category pair
            matched = False
            for target_cat, cross_cat in self.target_category_pairs:
                if target_cat in paper_categories and cross_cat in paper_categories:
                    yield {
                        "id": arxiv_id,
                        "categories": list(paper_categories),
                        "title": paper_dd.css(".list-title::text").get("").replace("Title:", "").strip(),
                        "authors": paper_dd.css(".list-authors a::text").getall(),
                        "submission_date": parsed_date,
                        "primary_category": current_category
                    }
                    self.logger.info(f"Found paper {arxiv_id} with categories {paper_categories} matching pair ({target_cat}, {cross_cat})")
                    matched = True
                    break
            
            if not matched:
                self.logger.debug(f"Skipped paper {arxiv_id} with categories {paper_categories} (no matching category pair)")
