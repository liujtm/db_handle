# -*- coding: UTF-8 -*-
import pymysql
import logging
import traceback


class DBHandle:
    """输入配置字典以初始化，例如
    db_conf_dict={'host':'127.0.0.1',
    'user':'username',
    'passwd':'xxxxx',
    'port':3306,
    'db':'dbname',
    'charset':'utf8mb4',
    'local_infile':True,
    'connect_timeout':3,
    'read_timeout':30,
    'write_timeout':5
    }"""

    def __init__(self, db_conf_dict):
        self.__db_conf_dict = db_conf_dict
        self.__db = None
        self.__conn()
        self.log = logging.getLogger(__name__)
        self.log.info("db connect() ok")

    def __conn(self):
        # 已经存在，先关闭连接socket，避免占用资源
        if self.__db:
            try:
                self.log.error("CLOSE previous db and RECONNECT.")
                self.__cursor.close()
                self.__dict_cursor.close()
                self.__db.close()
            except:  # pylint: disable=bare-except
                self.log.error("CLOSE previous db failed: %s" % (traceback.format_exc()))

        self.__db = pymysql.connect(**self.__db_conf_dict)
        self.__cursor = self.__db.cursor()
        self.__dict_cursor = self.__db.cursor(pymysql.cursors.DictCursor)

    def __del__(self):
        self.__db.close()
        # __del__函数里不能调用open打开文件
        # self.log.info('db close()')  #NameError: name 'open' is not defined #https://stackoverflow.com/questions/23422188/why-am-i-getting-nameerror-global-name-open-is-not-defined-in-del/29737870

    def db(self):
        return self.__db

    def cursor(self):
        return self.__cursor

    def dict_cursor(self):
        return self.__dict_cursor

    def __execute(self, query, args=None, cursor_type="dict_cursor"):
        try:
            if cursor_type == "dict_cursor":
                self.__dict_cursor.execute(query, args)
                self.__db.commit()  # 默认commit，后续不用继续commit
                return self.__dict_cursor.fetchall()
            else:
                self.__cursor.execute(query, args)
                self.__db.commit()  # 默认commit，后续不用继续commit
                return self.__cursor.fetchall()

        # 源码 site-packages/pymysql/connections.py
        # pymysql.err.OperationalError: (2006, "MySQL server has gone away (BrokenPipeError(32, 'Broken pipe'))")
        except pymysql.OperationalError:  # 查询失败、超时等，重连DB
            self.log.error("query failed for: %s . TRY RECONNECT DB. %s" % (query, traceback.format_exc()))
            self.__conn()
        return None

    # 返回获取的sql查询数据，已默认commit，后续不用继续commit
    def execute(self, query, args=None):
        return self.__execute(query, args, "dict_cursor")

    # 返回获取的sql查询数据，已默认commit，后续不用继续commit
    def default_execute(self, query, args=None):
        return self.__execute(query, args, "default_cursor")
