import scrapy
import os
import re
from itertools import product

class ArxivSpider(scrapy.Spider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Get user-specified categories
        categories = os.environ.get("CATEGORIES", "cs.CV")
        categories = [cat.strip() for cat in categories.split(",")]
        # Generate cross-disciplinary combinations with cs.LG and cs.AI
        cross_categories = ["cs.LG", "cs.AI"]
        self.target_category_pairs = [(cat, cross_cat) for cat in categories for cross_cat in cross_categories]
        # Create start URLs for all user-specified categories (not combinations)
        self.start_urls = [
            f"https://arxiv.org/list/{cat}/new" for cat in categories
        ]
        self.logger.info(f"Target category pairs: {self.target_category_pairs}")

    name = "arxiv"
    allowed_domains = ["arxiv.org"]

    def parse(self, response):
        anchors = []
        for li in response.css("div[id=dlpage] ul li"):
            href = li.css("a::attr(href)").get()
            if href and "item" in href:
                anchors.append(int(href.split("item")[-1]))

        for paper in response.css("dl dt"):
            paper_anchor = paper.css("a[name^='item']::attr(name)").get()
            if not paper_anchor:
                continue

            paper_id = int(paper_anchor.split("item")[-1])
            if anchors and paper_id >= anchors[-1]:
                continue

            abstract_link = paper.css("a[title='Abstract']::attr(href)").get()
            if not abstract_link:
                continue

            arxiv_id = abstract_link.split("/")[-1]
            paper_dd = paper.xpath("following-sibling::dd[1]")
            if not paper_dd:
                continue

            subjects_text = paper_dd.css(".list-subjects .primary-subject::text").get()
            if not subjects_text:
                subjects_text = paper_dd.css(".list-subjects::text").get()

            if subjects_text:
                categories_in_paper = re.findall(r'\(([^)]+)\)', subjects_text)
                paper_categories = set(categories_in_paper)

                # Check if the paper belongs to at least one target category pair
                for target_cat, cross_cat in self.target_category_pairs:
                    if target_cat in paper_categories and cross_cat in paper_categories:
                        yield {
                            "id": arxiv_id,
                            "categories": list(paper_categories),
                        }
                        self.logger.info(f"Found paper {arxiv_id} with categories {paper_categories} matching pair ({target_cat}, {cross_cat})")
                        break
                else:
                    self.logger.debug(f"Skipped paper {arxiv_id} with categories {paper_categories} (no matching category pair)")
            else:
                self.logger.warning(f"Could not extract categories for paper {arxiv_id}, skipping")
