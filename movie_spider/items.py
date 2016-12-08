# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MovieSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    movie_id = scrapy.Field()
    movie_name = scrapy.Field()
    genre = scrapy.Field()

    title = scrapy.Field()
    runtime = scrapy.Field()
    language = scrapy.Field()
    release_date = scrapy.Field()

    producer = scrapy.Field()
    issuer = scrapy.Field()

    actor = scrapy.Field()
    director = scrapy.Field()
    writer = scrapy.Field()

    person_info = scrapy.Field()
    # person_name = scrapy.Field()
    # person_birth = scrapy.Field()
    # person_biog = scrapy.Field()
