import requests
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags
import json
from scrapy.exceptions import DropItem

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
        input_processor=MapCompose(remove_tags, remove_whitespace)
    )
    description = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    modelYear = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    condition = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    transmission = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    manufacturer = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    model = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    fuelType = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    engineCapacity = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    mileage = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    color = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_whitespace),
        output_processor=TakeFirst()
    )
    site = scrapy.Field(
        output_processor=TakeFirst()
    )

    def to_dict(self):
        return {field: value for field, value in self.items()}

    def validate(self):
        required_fields = ['title', 'price', 'url', 'image', 'description', 'site']
        for field in required_fields:
            if not self.get(field):
                raise scrapy.exceptions.DropItem(f"Missing required field: {field}")

class BikeScrapper(scrapy.Spider):
    name = "bike_spider"
    start_urls = [
        "https://riyasewana.com/search/motorcycles",
        # "https://www.patpat.lk/vehicle/filter/bike",
        "https://www.saleme.lk/ads/sri-lanka/motorbikes-&-scooters"
    ]
    page_count = {'riyasewana': 0, 'patpatlk': 0, 'saleme': 0}
    max_pages = {'riyasewana': 20, 'patpatlk': 20, 'saleme': 20}
    description_mapping = {
        "yom": "modelYear",
        "make": "manufacturer",
        "model": "model",
        "gear": "transmission",
        "fuel type": "fuelType",
        "engine (cc)": "engineCapacity",
        "mileage (km)": "mileage",
    }
    data = []

    def parse(self, response):
        if "riyasewana" in response.url:
            yield from self.parse_riyasewana(response)
        elif "patpat.lk" in response.url:
            yield from self.parse_patpatlk(response)
        elif "saleme.lk" in response.url:
            yield from self.parse_saleme(response)

    def parse_image_and_description(self, response):
        loader = response.meta['loader']
        if "riyasewana" in response.url:
            self.parse_riyasewana_image_and_description(response, loader)
        elif "patpat.lk" in response.url:
            self.parse_patpatlk_image_and_description(response, loader)
        elif "saleme.lk" in response.url:
            self.parse_saleme_image_and_description(response, loader)

    def parse_riyasewana(self, response):
        for product in response.css("li.item"):
            loader = ItemLoader(item=ProductItem(), selector=product)
            loader.add_css("title", "h2.more a::text")
            price = product.css("div.boxintxt.b::text").get()
            if price:
                price = price.replace("Rs.", "").strip().replace("\r", "").replace(" ", "")
            loader.add_value("price", price)
            loader.add_value("url", product.css("h2.more a::attr(href)").get())
            loader.add_value('site', 'riyasewana.com')
            inner_page = product.css('h2.more a::attr(href)').get()
            if inner_page:
                request = response.follow(inner_page, self.parse_image_and_description)
                request.meta['loader'] = loader
                yield request
            else:
                item = loader.load_item()
                item.validate()
                if item not in self.data:
                    self.data.append(item)

        next_page = response.css('div.pagination a:contains("Next")::attr(href)').get()
        if next_page and self.page_count['riyasewana'] < self.max_pages['riyasewana']:
            self.page_count['riyasewana'] += 1
            yield response.follow(next_page, self.parse_riyasewana)

    def parse_patpatlk(self, response):
        for product in response.css("div.result-item"):
            loader = ItemLoader(item=ProductItem(), selector=product)
            loader.add_css("title", "h4.result-title span::text")
            price = product.css("h3.clearfix label::text").get()
            if price:
                price = price.replace("Rs", "").strip().replace("\r", "").replace(" ", "")
            loader.add_value("price", price)
            loader.add_value("url", product.css("div.result-img a::attr(href)").get())
            loader.add_value('site', 'patpat.lk')
            inner_page = product.css('div.result-img a::attr(href)').get()
            if inner_page:
                request = response.follow(inner_page, self.parse_image_and_description)
                request.meta['loader'] = loader
                yield request
            else:
                item = loader.load_item()
                item.validate()
                if item not in self.data:
                    self.data.append(item)
        next_page = response.css('ul.pagination a[rel="next"]::attr(href)').get()
        if next_page and self.page_count['patpatlk'] < self.max_pages['patpatlk']:
            self.page_count['patpatlk'] += 1
            yield response.follow(next_page, self.parse_patpatlk)

    def parse_saleme(self, response):
        for product in response.css("div.all-ads-cont > a"):
            loader = ItemLoader(item=ProductItem(), selector=product)
            loader.add_css("title", "h3.item-title::text")
            price = product.css("h4.item-price::text").get()
            if price:
                price = price.replace("RS", "").strip().replace("\r", "").replace(" ", "").replace(",", "")
            loader.add_value("price", price)
            loader.add_css("url", "a::attr(href)")
            loader.add_value('site', 'saleme.lk')
            inner_page = product.css("a::attr(href)").get()
            if inner_page:
                request = response.follow(inner_page, self.parse_image_and_description)
                request.meta['loader'] = loader
                yield request
            else:
                item = loader.load_item()
                item.validate()
                if item not in self.data:
                    self.data.append(item)

        next_page = response.css('ul.pager li a[rel="next"]::attr(href)').get()
        if next_page and self.page_count['saleme'] < self.max_pages['saleme']:
            self.page_count['saleme'] += 1
            yield response.follow(next_page, self.parse_saleme)

    def parse_riyasewana_image_and_description(self, response, loader):
        thumbnail_images = response.css("div.thumb a::attr(href)").getall()
        description = []
        rows = response.css("table.moret tr")
        for row in rows:
            tds = row.css("td")
            if len(tds) == 4:
                header1 = tds[0].css("p.moreh::text").get()
                value1 = tds[1].css("::text").get()
                header2 = tds[2].css("p.moreh::text").get()
                value2 = tds[3].css("::text").get()
                if header1 and value1:
                    field_name = self.description_mapping.get(header1.strip().lower())
                    if field_name:
                        loader.add_value(field_name, value1.strip())
                    description.append(f"{header1.strip()}: {value1.strip()}")
                if header2 and value2:
                    field_name = self.description_mapping.get(header2.strip().lower())
                    if field_name:
                        loader.add_value(field_name, value2.strip())
                    description.append(f"{header2.strip()}: {value2.strip()}")
        description_text = " | ".join(description)
        loader.add_value('image', thumbnail_images)
        loader.add_value("description", description_text)
        item = loader.load_item()
        item.validate()
        if item not in self.data:
            self.data.append(item)


    def parse_patpatlk_image_and_description(self, response, loader):
        image_urls = response.css('div.item-images a img::attr(data-src)').extract()
        loader.add_value('modelYear', response.css('td:contains("Model Year") + td::text').get())
        loader.add_value('condition', response.css('td:contains("Condition") + td::text').get())
        loader.add_value('transmission', response.css('td:contains("Transmission") + td::text').get())
        loader.add_value('manufacturer', response.css('td:contains("Manufacturer") + td::text').get())
        loader.add_value('model', response.css('table.course-info tr:nth-child(6) td:nth-child(2)::text').get())
        loader.add_value('engineCapacity', response.css('td:contains("Engine Capacity") + td::text').get())
        loader.add_value('mileage', response.css('td:contains("Mileage") + td::text').get())
        loader.add_value('image', image_urls)
        item = loader.load_item()
        item.validate()
        if item not in self.data:
            self.data.append(item)

    def parse_saleme_image_and_description(self, response, loader):
        thumbnail_images = response.css("li.gallery-item a::attr(href)").extract()
        loader.add_value('modelYear', response.css('ul.spec-ul li:nth-child(3) span.spec-des::text').get())
        loader.add_value('condition', response.css('div.vap-details-tail:nth-child(1) div.vap-tail-desc '
                                                   'span.vap-tail-values::text').get())
        loader.add_value('manufacturer', response.css('ul.spec-ul li:nth-child(1) span.spec-des::text').get())
        loader.add_value('model', response.css('ul.spec-ul li:nth-child(2) span.spec-des::text').get())
        loader.add_value('engineCapacity', response.css('ul.spec-ul li:nth-child(4) span.spec-des::text').get())
        loader.add_value('mileage', response.css('div.vap-details-tail:nth-child(2) div.vap-tail-desc '
                                                 'span.vap-tail-values::text').get())
        loader.add_value('description', response.css('div.description-div p::text').get())
        loader.add_value('image', thumbnail_images)
        item = loader.load_item()
        item.validate()
        if item not in self.data:
            self.data.append(item)

    def close(self, reason):
        with open("bikes.json", "w") as f:
            json.dump([item.to_dict() for item in self.data], f)

        self.send_data_to_api(self.data, 'http://localhost:8080/api/saveBikes')

        self.data.clear()

    def send_data_to_api(self, data, endpoint):
        chunk_size = 20
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            json_data = json.dumps([item.to_dict() for item in chunk])
            response = requests.post(endpoint, headers={"Content-Type": "application/json", "x-api-key": "your-api-key"}, data=json_data)
            if response.status_code != 201:
                print(response.text)
            else:
                print(f"Data sent successfully: {response.status_code}")

# Configure CrawlerProcess to export to JSON
process = CrawlerProcess(settings={
    "FEEDS": {
        "bikes.json": {"format": "json"},
    },
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    "FEED_EXPORT_ENCODING": "utf-8",
    "ROBOTSTXT_OBEY": False,
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
})

# Start crawling
process.crawl(BikeScrapper)
process.start()

# Write self.data to JSON file
with open('bikes.json', 'w', encoding='utf-8') as f:
    json.dump([item.to_dict() for item in BikeScrapper.data], f, ensure_ascii=False, indent=4)
