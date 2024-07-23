
import json
import logging

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags


def remove_whitespace(value):
    return value.strip().replace("\n", "")


class ProductItem(scrapy.Item):
    title = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    price = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    url = scrapy.Field(
        output_processor=TakeFirst()
    )
    image = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    description = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    site = scrapy.Field(
        output_processor=TakeFirst()
    )

    def to_dict(self):
        return {field: value for field, value in self.items()}


process = CrawlerProcess(settings={
    "FEEDS": {
        "mics.json": {"format": "json"},
    },
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    "FEED_EXPORT_ENCODING": "utf-8",
    "ROBOTSTXT_OBEY": False,
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
})


class MicsScrapper(scrapy.Spider):
    name = "mobilespider"
    data = []
    start_urls = [
        "https://celltronics.lk/product-category/microphones/",
        "https://lifemobile.lk/product-category/accessories/microphones/",
        "https://otc.lk/product-category/microphones/",
    ]

    def parse(self, response):
        if "celltronics.lk" in response.url:
            yield from self.parse_celltronics(response)
        elif "lifemobile.lk" in response.url:
            yield from self.parse_lifemobile(response)
        elif "otc.lk" in response.url:
            yield from self.parse_otc(response)

    def parse_image_description_otc(self, response):
        loader = response.meta['loader']

        stock = response.css('p.stock::text').get()
        # Skip item if out of stock
        if stock and 'Out of stock' in stock:
            return

        # Extract the first image URL from the provided HTML structure
        image = response.css('div.woocommerce-product-gallery__image:nth-child(1) a img::attr(src)').extract_first()
        description_parts = response.css('div.woocommerce-product-details__short-description ul li::text').extract()

        description = ' | '.join(description_parts)
        loader.add_value('description', description)
        loader.add_value('image', image)
        logging.info(" Inner Page ", loader.load_item())
        self.data.append(loader.load_item())

    def parse_lifemobile(self, response):
        for product in response.css("li.product"):
            loader = ItemLoader(item=ProductItem(), selector=product)
            loader.add_css("title", "h2.woocommerce-loop-product__title")
            loader.add_css("price", "span.woocommerce-Price-amount bdi::text")
            loader.add_value("url", product.css("a.woocommerce-LoopProduct-link::attr(href)").get())
            loader.add_value("site", "life_mobile")

            inner_page = product.css('a.woocommerce-LoopProduct-link::attr(href)').get()
            if inner_page:
                request = response.follow(inner_page, self.parse_image_description_lifemobile)
                request.meta['loader'] = loader
                yield request
            else:
                self.data.append(loader.load_item())

        next_page = response.css('a.next.page-numbers::attr(href)').get()
        if next_page:
            self.logger.info(f"Following next page: {next_page}")
            yield response.follow(next_page, self.parse_lifemobile)
        else:
            self.logger.info("No next page found.")

    def parse_image_description_lifemobile(self, response):
        loader = response.meta['loader']
        stock = response.css('p.stock::text').get()
        # Skip item if out of stock
        if stock and 'Out of stock' in stock:
            return

        image = response.css('div.woocommerce-product-gallery__image a::attr(href)').extract()
        description_parts = response.css('div.woocommerce-Tabs-panel--specification table tr').extract()

        description = ' | '.join(description_parts)
        loader.add_value('description', description)
        loader.add_value('image', image)
        self.data.append(loader.load_item())

    def parse_celltronics(self, response):

        for product in response.css("div.product-wrapper"):
            # Check if the product is out of stock
            if product.css("span.out-of-stock::text").get() == "Sold out":
                continue

            loader = ItemLoader(item=ProductItem(), selector=product)
            loader.add_css("title", "h3.wd-entities-title a::text")
            loader.add_css("price", "span.woocommerce-Price-amount bdi::text")
            loader.add_value("url", product.css("h3.wd-entities-title a::attr(href)").get())
            loader.add_value("site", "celltronic")

            # Extracting the description from the list items within hover-content-inner
            description_items = product.css("div.hover-content-inner ul li::text").getall()
            description = " | ".join(description_items)  # Join items with a separator for clarity
            loader.add_value("description", description)

            # item =  loader.load_item()
            inner_page = product.css('a.product-image-link::attr(href)').get()
            if inner_page:
                request = response.follow(inner_page, self.parse_image_celltronics)
                request.meta['loader'] = loader
                yield request
            else:
                self.data.append(loader.load_item())

        next_page = response.css('a.next.page-numbers::attr(href)').get()
        if next_page:
            self.logger.info(f"Following next page: {next_page}")
            yield response.follow(next_page, self.parse_celltronics)
        else:
            self.logger.info("No next page found.")

    def parse_image_celltronics(self, response):
        loader = response.meta['loader']
        image = response.css('figure.woocommerce-product-gallery__image a::attr(href)').extract()

        loader.add_value('image', image)
        self.data.append(loader.load_item())

    def parse_otc(self, response):
        for product in response.css("li.product"):
            loader = ItemLoader(item=ProductItem(), selector=product)
            loader.add_css("title", "h2.woocommerce-loop-product__title")
            loader.add_css("price", "span.woocommerce-Price-amount bdi::text")
            loader.add_value("url", product.css("a.woocommerce-LoopProduct-link::attr(href)").get())
            loader.add_value("site", "otc")

            inner_page = product.css('a.woocommerce-LoopProduct-link::attr(href)').get()
            if inner_page:
                request = response.follow(inner_page, self.parse_image_description_otc)
                request.meta['loader'] = loader
                yield request
            else:
                self.data.append(loader.load_item())

        next_page = response.css('a.next.page-numbers::attr(href)').get()
        if next_page:
            self.logger.info(f"Following next page: {next_page}")
            yield response.follow(next_page, self.parse_otc)
        else:
            self.logger.info("No next page found.")


process.crawl(MicsScrapper)
process.start()

# Save the data to a JSON file
with open('mics.json', 'w', encoding='utf-8') as f:
    json.dump([item.to_dict() for item in MicsScrapper.data], f, ensure_ascii=False, indent=4)
