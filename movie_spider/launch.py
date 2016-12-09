# coding:utf-8
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import re

__author__ = 'shujun'

settings = get_project_settings()
# settings.overrides['FEED_FORMAT'] = 'jsonlines'
# settings.overrides['FEED_URI'] = sys.argv[1]
settings.set("FEED_FORMAT", "jsonlines", priority='cmdline')
# settings.set("FEED_URI", "douban.txt", priority='cmdline')
#settings.set("FEED_URI", sys.argv[1], priority='cmdline')
process = CrawlerProcess(settings)
process.crawl('MovieSpider')
process.start()