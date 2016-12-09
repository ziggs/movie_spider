# -*- coding: utf-8 -*-
# from __future__ import print_function
import re
# import time
import scrapy
import requests
from lxml import etree
from movie_spider.items import MovieSpiderItem
# import urllib

class MoviespiderSpider(scrapy.Spider):
    name = "MovieSpider"
    allowed_domains = ["mtime.com"]
    start_urls = (
        'http://theater.mtime.com/China_Sichuan_Province_Chengdu/',
        'http://movie.mtime.com/comingsoon/#comingsoon'
    )

    def parse(self, response):
        if re.match(r"^http://theater.*$", response.url):
            sep_html = response.xpath('//*[@id="hotplayContent"]/div[1]').extract()[0]
        else:
            sep_html = response.xpath('//*[@id="comingID"]/div/div[1]/div').extract()[0]
        for url in re.findall(r"(http://movie\.mtime\.com/\d+/)", sep_html):
            yield scrapy.Request(url, callback=self.parse_movie)

    def parse_movie(self, response):
        item = MovieSpiderItem()
        item['movie_id'] = re.findall(r"http://movie\.mtime\.com/(\d+)/", response.url)[0]
        try:
            item['image_url'] = response.xpath('//*[@id="db_head"]/div[1]/div/div/div//img/@src').extract()[0]
        except IndexError:
            item['image_url'] = "无"
        # try:
        #     urllib.urlretrieve(response.xpath('//*[@id="db_head"]/div[1]/div/div/div//img/@src').extract()[0], "movie/%s.jpg" % item['movie_id'])
        # except IndexError:
        #     pass
        # time.sleep(1)
        item['movie_name'] = response.xpath('//*[@id="db_head"]/div[2]/div/div[1]/h1/text()').extract()[0].encode("utf-8").replace(" ", "")
        item['genre'] = "/".join(response.xpath('//*[@id="db_head"]/div[2]/div/div[2]//a[@property="v:genre"]/text()').extract()).encode("utf-8").replace(" ", "")
        return scrapy.Request(response.url + u"details.html", callback=self.parse_movie_detail, meta={'item': item})

    def parse_movie_detail(self, response):
        item = response.meta['item']

        more_name = []
        for options in response.xpath('//div[@class="details_cont"]/div//dd'):
            title = options.xpath("strong/text()").extract()[0]
            if title == u"更多中文名：":
                more_name.append(options.xpath('p/text()').extract()[0].replace(" ", "").encode("utf-8"))
            elif title == u"片长：":
                item['runtime'] = options.xpath("p/text()").extract()[0].replace(" ", "").encode("utf-8")
            elif title == u"对白语言：":
                item['language'] = options.xpath("string(p)").extract()[0].replace(" ", "").encode("utf-8")
        item['title'] = more_name

        release_date = []
        for options in response.xpath('//*[@id="releaseDateRegion"]/dd//li[not(@class)]'):
            country = options.xpath('div[1]/p/text()').extract()[0].replace(" ", "").encode("utf-8")
            date = options.xpath('div[2]/text()').extract()[0].replace(" ", "").encode("utf-8")
            release_date.append(country + "-" + date)
        item['release_date'] = release_date

        producers = []
        for options in response.xpath('//*[@id="companyRegion"]/dd/div/div[1]//li'):
            producer = options.xpath("a/text()").extract()[0].encode("utf-8").replace(" ", "")
            producers.append(producer)
        item['producer'] = producers

        issuers = []
        for options in response.xpath('//*[@id="companyRegion"]/dd/div/div[2]//li'):
            issuer = options.xpath("a/text()").extract()[0].encode("utf-8").replace(" ", "")
            issuers.append(issuer)
        item['issuer'] = issuers
        return scrapy.Request(response.url.replace("details.html", "fullcredits.html"), callback=self.parse_person, meta={'item': item})

    def parse_person(self, response):
        item = response.meta['item']

        person = {}
        actors = []
        chinese_pat = re.compile(u"[\u4e00-\u9fa5·\.\-A-Z]+[\u4e00-\u9fa5]")
        for actor in response.xpath('//div[@class="db_actor"]//dd'):
            actors.append(re.findall(r"^http://people\.mtime\.com/(\d+)/$", actor.xpath('.//a/@href').extract()[0].encode("utf-8"))[0])
            if chinese_pat.findall(actor.xpath('.//h3/a/text()').extract()[0]) == []:
                person[actor.xpath('.//a/@href').extract()[0] + "details.html"] = actor.xpath('.//h3/a/text()').extract()[0].encode("utf-8")
            else:
                person[actor.xpath('.//a/@href').extract()[0] + "details.html"] = chinese_pat.findall(actor.xpath('.//h3/a/text()').extract()[0])[0].encode("utf-8")
        item['actor'] = actors

        directors = []
        for director in response.xpath('//div[@class="credits_r"]/div[1]//p'):
            directors.append(re.findall(r"^http://people\.mtime\.com/(\d+)/$", director.xpath('a/@href').extract()[0].encode("utf-8"))[0])
            if chinese_pat.findall(director.xpath('a/text()').extract()[0]) == []:
                person[director.xpath('a/@href').extract()[0] + "details.html"] = director.xpath('a/text()').extract()[0].encode("utf-8")
            else:
                person[director.xpath('a/@href').extract()[0] + "details.html"] = chinese_pat.findall(director.xpath('a/text()').extract()[0])[0].encode("utf-8")
            break
        try:
            item['director'] = directors[0]
        except IndexError:
            item['director'] = "无"

        writers = []
        for writer in response.xpath('//div[@class="credits_r"]/div[2]//p'):
            writers.append(re.findall(r"^http://people\.mtime\.com/(\d+)/$", writer.xpath('a/@href').extract()[0].encode("utf-8"))[0])
            if chinese_pat.findall(writer.xpath('a/text()').extract()[0]) == []:
                person[writer.xpath('a/@href').extract()[0] + "details.html"] = writer.xpath('a/text()').extract()[0].encode("utf-8")
            else:
                person[writer.xpath('a/@href').extract()[0] + "details.html"] = chinese_pat.findall(writer.xpath('a/text()').extract()[0])[0].encode("utf-8")
        item['writer'] = writers

        item['person_info'] = self.person(person)
        yield item

    def person(self, person_dict):
        person_info = []
        for url, name in person_dict.items():
            response = requests.get(url)
            person_id = re.findall(r"^http://people\.mtime\.com/(\d+)/details\.html$", url)[0]
            selector = etree.HTML(response.content)
            try:
                if selector.xpath('//dl[@class="per_info_cont"]/dt[1]/strong/text()')[0].encode("utf-8") == "出生日期：":
                    birth = re.findall(r"(\d.*\d)[\u4e00-\u9fa5]*", selector.xpath('//dl[@class="per_info_cont"]/dt[1]/text()')[0].encode("utf-8"))[0]
                else:
                    birth = "无"
            except IndexError:
                birth = "无"
            biography = selector.xpath('string(//*[@id="lblAllGraphy"])').encode("utf-8")
            if biography.strip() == "":
                biography = "无"
            response = requests.get(url.replace(u"details.html", u""))
            selector = etree.HTML(response.content)
            try:
                image_url = selector.xpath('//*[@id="personDetailRegion"]/div[1]/span//img/@src')[0]
            except IndexError:
                image_url = "无"
            person_info.append(str(person_id) + "/#" + name + "/#" + birth + "/#" + biography + "/#" + image_url)
            # try:
            #     urllib.urlretrieve(selector.xpath('//*[@id="personDetailRegion"]/div[1]/span//img/@src')[0], "person/%s.jpg" % person_id)
            # except IndexError:
            #     pass
            # time.sleep(1)

        return person_info