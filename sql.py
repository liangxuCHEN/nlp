# -*- coding: utf-8 -*-
import pymssql


"""
use pymssql connect to the sql server
self.host = '192.168.1.253:1433'
        self.user = 'bs-prt'
        self.pwd = '123123'
        self.db = 'Collectiondb'
"""
HOST_253 = '192.168.1.253:1433'
USER = 'bs-prt'
PWD = '123123'
DB = 'Collectiondb'

HOST_190 = '192.168.1.190:1433'
DB_DM = 'DataMining'


class Mssql:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __get_connect(self):
        if not self.db:
            raise (NameError, "do not have db information")
        self.conn = pymssql.connect(
            host=self.host,
            user=self.user,
            password=self.pwd,
            database=self.db,
            charset="utf8"
        )
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "Have some Error")
        else:
            return cur

    def exec_query(self, sql):
        """
         the query will return the list, example;
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__get_connect()
        #print 'get conn'
        cur.execute(sql)
        res_list = cur.fetchall()

        # the db object must be closed
        self.conn.close()
        return res_list

    def exec_non_query(self, sql):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def exec_many_query(self, sql, param):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        try:
            cur.executemany(sql, param)
            self.conn.commit()
        except Exception as e:
            print e
            self.conn.rollback()

        self.conn.close()


def get_project_data(begin_date):
    # 获取项目的状态
    conn = Mssql(HOST_253, USER, PWD, DB)
    sql_text = "select ItemID, ItemStatus from T_Treasure_EvalCustomItem where ModifyTime>'{begin}'".format(
        begin=begin_date)
    return conn.exec_query(sql_text)


def check_jobs_in_db(itemids):
    # 根据状态，选择需要统计的宝贝评论，如果更新时间不是同一天，更新状态，再统计，
    # 同一天，但状态是没有统计，就统计，已经统计，就忽略
    if len(itemids) == 1:
        itemids.append(itemids[0])
    itemids = [str(x) for x in itemids]
    conn = Mssql(HOST_190, USER, PWD, DB_DM)
    sql_text = "select * from T_DCR_CustomCommentStatus where ItemID in {itemids}".format(
        itemids=str(tuple(itemids)))
    return conn.exec_query(sql_text)


def find_treasure_ids(itemids):
    if len(itemids) == 1:
        # 构造一个假的id，令sql结构完整
        itemids.append('aaaaaaaa-5828-4e54-895b-aaaaaaaaaaaa')
    itemids = [str(x) for x in itemids]
    itemids = str(tuple(itemids))
    conn = Mssql(HOST_253, USER, PWD, DB)
    sql_text = "SELECT TreasureID FROM T_Treasure_EvalCustomItem_Detail WHERE ItemID in {ids}".format(ids=itemids)
    return conn.exec_query(sql_text)


def finish_jobs(project_ids, modify_date):
    conn = Mssql(HOST_190, USER, PWD, DB_DM)
    insert_data = list()
    for p_id in project_ids:
        # 先看是否存在, 存在就删除原来数据
        sql_text = "delete T_DCR_CustomCommentStatus where ItemID='%s'" % p_id
        conn.exec_non_query(sql_text)
        insert_data.append((str(p_id), modify_date.strftime('%Y-%m-%d %H:%M:%S')))

    sql_text = "insert into T_DCR_CustomCommentStatus values (%s, %s)"
    conn.exec_many_query(sql_text, insert_data)

if __name__ == "__main__":
    print 'begin'
