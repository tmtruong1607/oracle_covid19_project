import scrapy
import json
from scrapy import cmdline
from scrapy.spiders import Spider
import os
from pathlib import Path

path_file_json = Path("C:/Users/tmtru/PycharmProjects/apicovid19/covid19news/vnexpress.json")
path_file_py = Path("C:/Users/tmtru/PycharmProjects/apicovid19/covid19news/news.py")
def deletefilejson():
    try:
        os.remove(path_file_json)
    except: pass
def print_json():
    with open(path_file_json,'r',encoding='utf8') as f:
        for i in f:
            print(i["title"])
deletefilejson()
class ExampleSpider1(Spider):
    name = 'vnexpress'
    allowed_domains = ['https://vnexpress.net/']
    start_urls = ['https://vnexpress.net/chu-de/covid-19-1299']

    def parse(self, response):
        for news in response.css('.item-news.item-news-common'):
            yield {'title': news.css('h3.title-news a::text').get(),'link': news.css('h3.title-news a::attr(href)').get(),'date': news.css('p.meta-news span.time-public span::attr(datetime)').get(),'description': news.css('p.description a::text').get()}
            # yield {'title': title}
        # for href in response.css('h3.title-news a::attr(href)').getall():
        #     yield {'href': href}
            # yield scrapy.Request(href, self.parse)
cmd = cmdline
cmd.execute("scrapy runspider C:/Users/tmtru/PycharmProjects/apicovid19/covid19news/news.py -o 'C:/Users/tmtru/PycharmProjects/apicovid19/covid19news/vnexpress.json -t json".split())




