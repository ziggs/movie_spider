# -*- coding: utf-8 -*-
# from __future__ import print_function
import re
import scrapy
import requests
from lxml import etree
from movie_spider.items import MovieSpiderItem


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
        chinese_pat =re.compile(u"[\u4e00-\u9fa5·\.\-A-Z]+[\u4e00-\u9fa5]")
        for actor in response.xpath('//div[@class="db_actor"]//dd'):
            if chinese_pat.findall(actor.xpath('.//h3/a/text()').extract()[0]) == []:
                actors.append(actor.xpath('.//h3/a/text()').extract()[0].encode("utf-8"))
                person[actor.xpath('.//a/@href').extract()[0] + "details.html"] = actor.xpath('.//h3/a/text()').extract()[0].encode("utf-8")
            else:
                actors.append(chinese_pat.findall(actor.xpath('.//h3/a/text()').extract()[0])[0].encode("utf-8"))
                person[actor.xpath('.//a/@href').extract()[0] + "details.html"] = chinese_pat.findall(actor.xpath('.//h3/a/text()').extract()[0])[0].encode("utf-8")
            # yield scrapy.Request(actor.xpath('.//a/@href').extract()[0] + "details.html", callback=self.parse_person_detail, meta={'person_name': actor.xpath('.//a/text()').extract()[0].encode("utf-8")})
            # print scrapy.Request(actor.xpath('.//a/@href').extract()[0] + "details.html", callback=self.parse_person_detail, meta={'person_name': actor.xpath('.//a/text()').extract()[0].encode("utf-8")}).body
        item['actor_movie'] = actors

        directors = []
        for director in response.xpath('//div[@class="credits_r"]/div[1]//p'):
            if chinese_pat.findall(director.xpath('a/text()').extract()[0]) == []:
                directors.append(director.xpath('a/text()').extract()[0].encode("utf-8"))
                person[director.xpath('a/@href').extract()[0] + "details.html"] = director.xpath('a/text()').extract()[0].encode("utf-8")
            else:
                directors.append(chinese_pat.findall(director.xpath('a/text()').extract()[0])[0].encode("utf-8"))
                person[director.xpath('a/@href').extract()[0] + "details.html"] = chinese_pat.findall(director.xpath('a/text()').extract()[0])[0].encode("utf-8")
            # yield scrapy.Request(director.xpath('a/@href').extract()[0] + "details.html", callback=self.parse_person_detail, meta={'person_name': director.xpath('a/text()').extract()[0].encode("utf-8")})
        item['director_movie'] = directors

        writers = []
        for writer in response.xpath('//div[@class="credits_r"]/div[2]//p'):
            if chinese_pat.findall(writer.xpath('a/text()').extract()[0]) == []:
                writers.append(writer.xpath('a/text()').extract()[0].encode("utf-8"))
                person[writer.xpath('a/@href').extract()[0] + "details.html"] = writer.xpath('a/text()').extract()[0].encode("utf-8")
            else:
                writers.append(chinese_pat.findall(writer.xpath('a/text()').extract()[0])[0].encode("utf-8"))
                person[writer.xpath('a/@href').extract()[0] + "details.html"] = chinese_pat.findall(writer.xpath('a/text()').extract()[0])[0].encode("utf-8")
            # yield scrapy.Request(writer.xpath('a/@href').extract()[0] + "details.html", callback=self.parse_person_detail, meta={'person_name': writer.xpath('a/text()').extract()[0].encode("utf-8")})
        item['writer_movie'] = writers

        item['person_info'] = self.person(person)
        yield item

    def person(self, person_dict):
        person_info = []
        for url, name in person_dict.items():
            response = requests.get(url)
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
            person_info.append(name + "/" + birth + "/" + biography)
        return person_info
        #     person_info.append(scrapy.Request(url, callback=self.parse_person_detail, meta={'person_name': name, 'person_info': " "}).meta['person_info'])
        # item['person_info'] = person_info
        # return item

    # def parse_person_detail(self, response):
    #     person_name = response.meta['person_name']
    #     if response.status == 403 or response.status == 302:
    #         response.meta['person_info'] = person_name + "/无/无"
    #     else:
    #         birth = response.xpath('//dl[@class="per_info_cont"]/dt[1]/text()').extract()[0].split(" ")[0]
    #         biography = response.xpath('//*[@id="lblAllGraphy"]').xpath('string(.)').extract()[0]
    #         print "-------------------------------------------------------"
    #         print birth.encode("gbk")
    #         print biography.encode("gbk")
    #         print "-------------------------------------------------------"
    #         response.meta['person_info'] = person_name + '/' + birth.encode("utf-8") + '/' + biography.encode("utf-8")
    #     print "response.meta: ", response.meta
    #     return response.meta
        # //*[@id="lblAllGraphy"]
        # //dl[@class="per_info_cont"]/dt[1]/text() //birthday
        # return item
        # response.xpath('//*[@id="lblAllGraphy"]').xpath('String(.)').extract()[0].encode("utf-8")


"""
    def get_movie_detail(self, response):
        item = MovieSpiderItem()
        detail = response.xpath('//*[@id="movie_warp"]/div[2]/div[3]/div/div[4]/div[2]/div[1]/div[2]/div[1]/dl//dd')
        for content in detail:
            if content.xpath('strong/text()').extract()[0] == u"导演：":
                item['movie_director'] = content.xpath('a/text()').extract()[0].encode("utf-8")
            # elif content.xpath('strong/text()').extract()[0] == u"发行公司：":
            #     item['movie_company'] = content.xpath('a/text()').extract()[0].encode("utf-8")
            # elif content.xpath('strong/text()').extract()[0] == u"更多片名：":
            #     item['more_name'] = content.xpath('span/text()').extract()[0].encode("utf-8")
            if item['movie_director'] is None:
                item['movie_director'] = "无"

        item['movie_name'] = response.xpath('//*[@id="db_head"]/div[2]/div/div[1]/h1/text()').extract()[0].encode("utf-8")
        #                                    //*[@id="db_head"]/div[2]/div/div[2]/span
        try:
            item['movie_time'] = response.xpath('//*[@id="db_head"]/div[2]/div/div[2]/span/text()').extract()[0].encode("utf-8")
        except:
            item['movie_time'] = 0
        yield scrapy.Request(response.url + u"fullcredits.html", callback=self.parse_credits, meta={'item': item})
        # return item

    def parse_credits(self, response):
        item = response.meta['item']
        item['actor'] = ",".join(response.xpath("//div[@class='db_actor']//dd/div/div/h3/a/text()").extract()).encode("utf-8")
        return item
"""