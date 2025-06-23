import scrapy
import re
from w3lib.html import remove_tags


class ChlorineSpider(scrapy.Spider):
    name = "chlorine"
    allowed_domains = ["freshpoolsupply.com"]
    start_urls = ["https://www.freshpoolsupply.com/chemical/chlorine.html"]

    custom_settings = {
        "FEEDS": {
            "chlorine_data.csv": {
                "format": "csv",
                "encoding": "utf-8",
                "overwrite": True,
                "store_empty": False,
                "item_export_kwargs": {
                    "quoting": 1,
                },
            }
        }
    }

    def extract_clean_html(self, response, selector):
        html_block = response.css(selector).get(default="")
        cleaned = remove_tags(html_block)
        cleaned = re.sub(r'\s*\n\s*', ' ', cleaned)
        cleaned = re.sub(r'\s{2,}', ' ', cleaned)
        return cleaned.strip()

    def parse(self, response):
        # 1. Extract all product links on current page
        product_links = response.css("a.product-item-link::attr(href)").getall()
        for link in product_links:
            yield response.follow(link, callback=self.parse_product)

        # 2. Follow the "Next" pagination link
        next_page = response.css('li.pages-item-next a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        def extract(selector):
            return response.css(selector).xpath("string()").get(default="").strip()

        stock_elem = response.css("div.product-info-stock-sku div.stock span::text").get()
        stock = stock_elem.strip() if stock_elem else "N/A"

        description = self.extract_clean_html(response, "div#description div.value")

        data = {
            "title": extract("h1.page-title span.base"),
            "overview": extract("div.product.attribute.overview div.value"),
            "price": extract("span.price"),
            "stock": stock,
            "description": description,
            "link": response.url,
        }

        # Collect More Information table
        rows = response.css("table#product-attribute-specs-table tr")
        for row in rows:
            key = row.css("th::text").get()
            value = row.css("td::text").get()
            if key and value:
                data[key.strip()] = value.strip()

        yield data
