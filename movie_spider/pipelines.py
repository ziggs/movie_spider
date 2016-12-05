# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# import traceback
# import dj_database_url
import MySQLdb
import MySQLdb.cursors
# import re
# from twisted.internet import defer
from twisted.enterprise import adbapi
# from scrapy.exceptions import NotConfigured


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

        self.movie_id = 0

        self.person_id = 1
        self.person_dict = {}

        self.company_id = 1
        self.producer_id = []
        self.issuer_id = []

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self._conditional_insert, item)  # 调用插入的方法
        # query = self.dbpool.runInteraction(self._insert_movie, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_release_date, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_genre, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_title, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_company, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_issuer, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_producer, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_person, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_name, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_director, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_writer, item)
        # query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        # query = self.dbpool.runInteraction(self._insert_actor, item)
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
        self.movie_id += 1
        sql = "insert into movie(movie_id, runtime, language, movie_name) values(%s, %s, %s, %s);"
        runtime = "无"
        language = "无"
        if 'runtime' in item.keys():  # item.has_key('runtime'):
            runtime = item['runtime']
        if 'language' in item.keys():  # item.has_key('language'):
            language = item['language']
        params = (self.movie_id, runtime, language, item['movie_name'])
        # print (sql % params).decode("utf-8").encode("gbk")
        tx.execute(sql, params)

    def _insert_release_date(self, tx, item):
        sql = "insert into release_date(movie_id, country, release_date) values(%s, %s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        if 'release_date' in item.keys():  # item.has_key('release_date'):
            for date in item['release_date']:
                country = date.split("-")[0]
                release_date = date.split("-")[1]
                tx.execute(sql, (self.movie_id, country, release_date))

    def _insert_genre(self, tx, item):
        sql = "insert into genre(movie_id, genre) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        if 'genre' in item.keys():
            for genre in item["genre"].split("/"):
                tx.execute(sql, (self.movie_id, genre))

    def _insert_title(self, tx, item):
        sql = "insert into title(movie_id, movie_title) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        if 'title' in item.keys():
            for title in item['title']:
                tx.execute(sql, (self.movie_id, title))

    def _insert_company(self, tx, item):
        sql = "insert into company(company_id, company_name) values(%s, %s);"
        if 'producer' in item.keys():
            for company in item['producer']:
                if company not in self.company:
                    tx.execute(sql, (self.company_id, company))
                    self.producer_id.append(self.company_id)
                    self.company_id += 1
                    self.company.append(company)

        if 'issuer' in item.keys():
            for company in item['issuer']:
                if company not in self.company:
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
                tx.execute(sql, (self.movie_id, issuer))
            self.issuer_id = []

    def _insert_producer(self, tx, item):
        sql = "insert into producer(movie_id, company_id) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        if 'producer' in item.keys():
            for producer in self.producer_id:
                # company_id = self.get_company_id(tx, producer)
                # company_id = self.producer_id.pop()
                tx.execute(sql, (self.movie_id, producer))
            self.producer_id = []

    def _insert_person(self, tx, item):
        sql = "insert into person(person_id, person_birthday, person_biography, person_name) values(%s, %s, %s, %s);"
        if 'person_info' in item.keys():
            for person in item['person_info']:
                person_name = person.split("/")[0]
                if person_name not in self.person_dict.keys():
                    person_birthday = person.split("/")[1]
                    person_biography = person.split("/")[2].strip()
                    tx.execute(sql, (self.person_id, person_birthday, person_biography, person_name))
                    self.person_dict[person_name] = self.person_id
                    self.person_id += 1
                    self.person.append(person_name)

    # def _insert_name(self, tx, item):
    #     pass

    def _insert_director(self, tx, item):
        sql = "insert into director(movie_id, person_id) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        for director in item['director_movie']:
            # person_id = self.get_person_id(tx, director.split("/")[1])
            # person_id = self.director_id.pop()
            if director not in self.person_dict.keys():
                person_birthday = "无"
                person_biography = "无"
                for person in item['person_info']:
                    if director == person.split("/")[0]:
                        person_birthday = person.split("/")[1]
                        person_biography = person.split("/")[2].strip()
                        break
                tx.execute("insert into person(person_id, person_birthday, person_biography, person_name) values(%s, %s, %s, %s);",(self.person_id, person_birthday, person_biography, director))
                self.person_dict[director] = self.person_id
                self.person_id += 1
                self.person.append(director)
            tx.execute(sql, (self.movie_id, self.person_dict[director]))

    def _insert_writer(self, tx, item):
        sql = "insert into writer(movie_id, person_id) values(%s, %s);"
        for writer in item['writer_movie']:
            if writer not in self.person_dict.keys():
                person_birthday = "无"
                person_biography = "无"
                for person in item['person_info']:
                    if writer == person.split("/")[0]:
                        person_birthday = person.split("/")[1]
                        person_biography = person.split("/")[2].strip()
                        break
                tx.execute("insert into person(person_id, person_birthday, person_biography, person_name) values(%s, %s, %s, %s);",(self.person_id, person_birthday, person_biography, writer))
                self.person_dict[writer] = self.person_id
                self.person_id += 1
                self.person.append(writer)
            try:
                tx.execute(sql, (self.movie_id, self.person_dict[writer]))
            except Exception:
                print "ERROR: I don't know where it is, maybe is two same writers in one movie."

    def _insert_actor(self, tx, item):
        sql = "insert into actor(movie_id, person_id) values(%s, %s);"
        # movie_id = self.get_movie_id(tx, item['movie_name'])
        # movie_id = self.movie_id
        for actor in item['actor_movie']:
            # person_id = self.get_person_id(tx, actor.split("/")[1])
            # person_id = self.actor_id.pop()
            if actor not in self.person_dict.keys():
                person_birthday = "无"
                person_biography = "无"
                for person in item['person_info']:
                    if actor == person.split("/")[0]:
                        person_birthday = person.split("/")[1]
                        person_biography = person.split("/")[2].strip()
                        break
                tx.execute("insert into person(person_id, person_birthday, person_biography, person_name) values(%s, %s, %s, %s);", (self.person_id, person_birthday, person_biography, actor))
                self.person_dict[actor] = self.person_id
                self.person_id += 1
                self.person.append(actor)
            tx.execute(sql, (self.movie_id, self.person_dict[actor]))

    # def get_movie_id(self, tx, movie_name):
    #     sql = "select movie_id from movie where movie_name = %s;"
    #     tx.execute(sql, movie_name)
    #     # print sql, movie_name
    #     result = tx.fetchone()[0]
    #     return result

    # def get_person_id(self, tx, person_name):
    #     sql = "select person_id from person where person_name = %s;"
    #     try:
    #         tx.execute(sql, person_name)
    #     except Exception, e:
    #         print "ERROR:", e, sql, person_name
    #     result = tx.fetchone()[0]
    #     print result
    #     return result

    # def get_company_id(self, tx, company_name):
    #     sql = "select company_id from company where company_name = %s;"
    #     try:
    #         tx.execute(sql, company_name)
    #     except Exception, e:
    #         print "ERROR:", e, sql, company_name
    #     result = tx.fetchone()[0]
    #     print result
    #     return result

    def _handle_error(self, failue, item, spider):
        print failue

"""
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
        self.actor_id = 1
        self.movie_id = 1
        self.dbpool = dbpool

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self._conditional_insert, item)  # 调用插入的方法
        query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        return item

    def _conditional_insert(self, tx, item):
        # print item['name']
        self._insert_movie(tx, item)
        self._insert_actor(tx, item)

    def _insert_movie(self, tx, item):
        sql = "insert into movie(movie_id, movie_name, runtime, director) values(%s, %s, %s, %s);"
        # params = (self.movie_id, item["movie_name"], item["movie_time"], item["movie_director"])
        runtime = 0
        director = "无"
        # company = "无"
        # if item["movie_company"]:
        #     company = item["movie_company"]
        if item["movie_director"]:
            director = item["movie_director"]
        if item["movie_time"]:
            runtime = re.findall(r"(\d+)", item["movie_time"])[0]
        tx.execute(sql, (self.movie_id, item["movie_name"], runtime, director))
        self.movie_id += 1

    def _insert_actor(self, tx, item):
        sql = "insert into actor(actor_id, name) values(%s, %s);"
        sql_conn = "insert into act_conn(actor_id, movie_id) values(%s, %s);"
        for actor in item["actor"].split(","):
            # params = (self.actor_id, actor)
            # params_conn = (self.actor_id, self.movie_id)
            # try:
            tx.execute(sql, (self.actor_id, actor))
            # except:
            #     self.actor_id -= 1
            tx.execute(sql_conn, (self.actor_id, self.movie_id))
            self.actor_id += 1

    def _handle_error(self, failue, item, spider):
        print failue
"""