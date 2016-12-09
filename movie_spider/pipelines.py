# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb
from scrapy import log
import MySQLdb.cursors
from twisted.enterprise import adbapi


class MovieSpiderPipeline(object):

    @classmethod
    def from_settings(cls, settings):
        dbparams = dict(
            host=settings['MYSQL_HOST'],  # 读取settings中的配置
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',  # 编码要加上，否则可能出现中文乱码问题
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=False,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbparams)
        return cls(dbpool)

    def __init__(self, dbpool):
        self.dbpool = dbpool
        self.person = []
        self.company = []

        # self.movie_id = 0

        # self.person_id = 1
        # self.person_dict = {}

        self.company_id = 1
        self.producer_id = []
        self.issuer_id = []

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self._conditional_insert, item)  # 调用插入的方法
        query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        return item

    def _conditional_insert(self, tx, item):
        self._insert_movie(tx, item)  # 电影
        self._insert_company(tx, item)  # 公司
        self._insert_person(tx, item)  # 人员

        self._insert_title(tx, item)
        self._insert_genre(tx, item)
        self._insert_release_date(tx, item)

        self._insert_issuer(tx, item)
        self._insert_producer(tx, item)

        # self._insert_name(tx, item)
        self._insert_director(tx, item)
        self._insert_writer(tx, item)
        self._insert_actor(tx, item)

    def _insert_movie(self, tx, item):
        # self.movie_id += 1
        sql = "insert into movie(movie_id, runtime, language, movie_name, image_url) values(%s, %s, %s, %s, %s);"
        runtime = "无"
        language = "无"
        if 'runtime' in item.keys():  # item.has_key('runtime'):
            runtime = item['runtime']
        if 'language' in item.keys():  # item.has_key('language'):
            language = item['language']
        # params = (self.movie_id, runtime, language, item['movie_name'])
        params = (item['movie_id'], runtime, language, item['movie_name'], item['image_url'])
        tx.execute(sql, params)

    def _insert_release_date(self, tx, item):
        sql = "insert into release_date(movie_id, country, release_date) values(%s, %s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        if 'release_date' in item.keys():  # item.has_key('release_date'):
            for date in item['release_date']:
                country = date.split("-")[0]
                release_date = date.split("-")[1]
                tx.execute(sql, (item['movie_id'], country, release_date))

    def _insert_genre(self, tx, item):
        sql = "insert into genre(movie_id, genre) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        if 'genre' in item.keys():
            for genre in item["genre"].split("/"):
                tx.execute(sql, (item['movie_id'], genre))

    def _insert_title(self, tx, item):
        sql = "insert into title(movie_id, movie_title) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        if 'title' in item.keys():
            for title in item['title']:
                tx.execute(sql, (item['movie_id'], title))

    def _insert_company(self, tx, item):
        sql = "insert into company(company_id, company_name) values(%s, %s);"
        if 'producer' in item.keys():
            for company in item['producer']:
                if company not in self.company:
                    # print "-----------------"
                    # print self.company_id, company
                    # print "-----------------"
                    tx.execute(sql, (self.company_id, company))
                    self.producer_id.append(self.company_id)
                    self.company_id += 1
                    self.company.append(company)

        if 'issuer' in item.keys():
            for company in item['issuer']:
                if company not in self.company:
                    # print "-----------------"
                    # print self.company_id, company
                    # print "-----------------"
                    tx.execute(sql, (self.company_id, company))
                    self.issuer_id.append(self.company_id)
                    self.company_id += 1
                    self.company.append(company)

    def _insert_issuer(self, tx, item):
        sql = "insert into issuer(movie_id, company_id) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        if 'issuer' in item.keys():
            for issuer in self.issuer_id:
                # company_id = self.get_company_id(tx, issuer)
                # company_id = self.issuer_id.pop()
                tx.execute(sql, (item['movie_id'], issuer))
            self.issuer_id = []

    def _insert_producer(self, tx, item):
        sql = "insert into producer(movie_id, company_id) values(%s, %s);"
        if 'producer' in item.keys():
            for producer in self.producer_id:
                tx.execute(sql, (item['movie_id'], producer))
            self.producer_id = []

    def _insert_person(self, tx, item):
        sql = "insert into person(person_id, person_birthday, person_biography, person_name, image_url) values(%s, %s, %s, %s, %s);"
        if 'person_info' in item.keys():
            for person in item['person_info']:
                person_id = person.split("/#")[0]
                if person_id not in self.person:
                    person_name = person.split("/#")[1]
                    person_birthday = person.split("/#")[2]
                    person_biography = person.split("/#")[3].strip()
                    image_url = person.split("/#")[4]
                    # print image_url
                    tx.execute(sql, (person_id, person_birthday, person_biography, person_name, image_url))
                    self.person.append(person_id)
                    # self.person_id += 1
                    # self.person.append(person_name)

    def _insert_director(self, tx, item):
        sql = "insert into director(movie_id, person_id) values(%s, %s);"
        director = item['director']
        if director not in self.person:
            person_birthday = "无"
            person_biography = "无"
            image_url = "无"
            for person in item['person_info']:
                if director == person.split("/#")[1]:
                    person_birthday = person.split("/#")[2]
                    person_biography = person.split("/#")[3].strip()
                    image_url = person.split("/#")[4]
                    break
            tx.execute("insert into person(person_id, person_birthday, person_biography, person_name, image_url) values(%s, %s, %s, %s, %s);", (director, person_birthday, person_biography, director, image_url))
            self.person.append(director)
                # self.person_id += 1
                # self.person.append(director)
        tx.execute(sql, (item['movie_id'], director))

    def _insert_writer(self, tx, item):
        sql = "insert into writer(movie_id, person_id) values(%s, %s);"
        for writer in item['writer']:
            if writer not in self.person:
                person_birthday = "无"
                person_biography = "无"
                for person in item['person_info']:
                    if writer == person.split("/#")[1]:
                        person_birthday = person.split("/#")[2]
                        person_biography = person.split("/#")[3].strip()
                        image_url = person.split("/#")[4]
                        break
                tx.execute("insert into person(person_id, person_birthday, person_biography, person_name, image_url) values(%s, %s, %s, %s, %s);", (writer, person_birthday, person_biography, writer, image_url))
                self.person.append(writer)
                # self.person_id += 1
                # self.person.append(writer)
            try:
                tx.execute(sql, (item['movie_id'], writer))
            except Exception:
                print "ERROR: I don't know where it is, maybe is two same writers in one movie."

    def _insert_actor(self, tx, item):
        sql = "insert into actor(movie_id, person_id) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        for actor in item['actor']:
            # person_id = self.get_person_id(tx, actor.split("/")[1])
            # person_id = self.actor_id.pop()
            if actor not in self.person:
                person_birthday = "无"
                person_biography = "无"
                for person in item['person_info']:
                    if actor == person.split("/#")[1]:
                        person_birthday = person.split("/#")[2]
                        person_biography = person.split("/#")[3].strip()
                        image_url = person.split("/#")[4]
                        break
                tx.execute("insert into person(person_id, person_birthday, person_biography, person_name, image_url) values(%s, %s, %s, %s, %s);", (actor, person_birthday, person_biography, actor, image_url))
                self.person.append(actor)
                # self.person_id += 1
                # self.person.append(actor)
            tx.execute(sql, (item['movie_id'], actor))

    def _handle_error(self, failue, item, spider):
        print failue
