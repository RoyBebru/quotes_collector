#!/usr/bin/env python3

from datetime import datetime
import json
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.signalmanager import dispatcher


class AuthorsSpider(scrapy.Spider):
    name = 'authors'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com']

    author_map = {}

    def parse(self, response):
        for quote in response.xpath("/html//div[@class='quote']"):
            author = quote.xpath("span/small/text()").get()
            author_link = quote.xpath("span/a/@href").get()
            if author_link in self.author_map.keys():
                continue
            self.author_map[author] = author_link

        next_link = response.xpath("//li[@class='next']/a/@href").get()
        if next_link:
            yield scrapy.Request(url=self.start_urls[0] + next_link)

        for author_as_key, about in self.author_map.items():
            yield scrapy.Request(url=self.start_urls[0] + about,
                    callback=self.parse_about,
                    cb_kwargs={"author_as_key": author_as_key})

    def parse_about(self, response, author_as_key):
        author = response.xpath("/html//div[@class='author-details']")
        yield {                        # "Alexandre Dumas fils"!="Alexandre Dumas-fils"
            "fullname": author_as_key, # author.xpath("h3/text()").get().strip()
            "born_date":
                author.xpath("p/span[@class='author-born-date']/text()")
                            .get().strip(),
            "born_location":
                author.xpath("p/span[@class='author-born-location']/text()")
                            .get().strip(),
            "description":
                author.xpath("div[@class='author-description']/text()")
                            .get().strip()
        }


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com']

    def parse(self, response):
        for quote in response.xpath("/html//div[@class='quote']"):
            yield {
                "tags": quote.xpath("div[@class='tags']/a/text()").extract(),
                "author": quote.xpath("span/small/text()").get(),
                "quote": quote.xpath("span[@class='text']/text()").get()
            }
        next_link = response.xpath("//li[@class='next']/a/@href").get()
        if next_link:
            yield scrapy.Request(url=self.start_urls[0] + next_link)


def main(file_authors, file_quotes):
    author_list = []
    quote_list = []

    def crawler_results(signal, sender, item, response, spider):
        if "quote" in item.keys():
            quote_list.append(item)
        else:
            author_list.append(item)

    dispatcher.connect(crawler_results, signal=scrapy.signals.item_scraped)
    process = CrawlerProcess(get_project_settings())
    process.crawl(AuthorsSpider)
    process.crawl(QuotesSpider)
    process.start() # the script will block here until the crawling is finished

    try:
        with open(file_authors, "w") as fh:
            json.dump(author_list, fh, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error while write to file {file_authors}: {str(e)}")

    try:
        with open(file_quotes, "w") as fh:
            json.dump(quote_list, fh, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error while write to file {file_quotes}: {str(e)}")


if __name__ == "__main__":
    suff = datetime.now().strftime("%Y-%m-%d_%H-%M")
    main(f"author_{suff}.json", f"quotes_{suff}.json")
