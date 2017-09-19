# -*- coding: utf-8 -*-
from datetime import datetime as dt
import os
from snownlp import SnowNLP
import json
import pandas as pd
import codecs
import pymssql
import sqlalchemy
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, bindparam
import logging
from collections import defaultdict
import settings
import jieba.posseg as pseg
from datetime import timedelta
from datetime import datetime as dt
from tgrocery import Grocery

import sql
from pymongo import MongoClient

# 调用 readLines 读取停用词
# BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'chat_data_mining', 'DM_sentiment')
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'nlp')

TABLE = 'T_DCR_Comment'

MONGO_HOST = '192.168.3.172'


def init_mongo_sql(host=MONGO_HOST):
    conn = MongoClient(host, 27017)
    return conn


def log_init(file_name):
    """
    logging.debug('This is debug message')
    logging.info('This is info message')
    logging.warning('This is warning message')
    """
    path = os.path.join(BASE_DIR, 'log')
    file_name = os.path.join(path, file_name)

    level = logging.DEBUG
    logging.basicConfig(level=level,
                        format='%(asctime)s [line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=file_name,
                        filemode='a+')
    return logging


def init_sql():
    conn = pymssql.connect(
        host=settings.HOST_252,
        user=settings.HOST_252_USER,
        password=settings.HOST_PASSWORD,
        database=settings.DB_Comment,
    )
    return conn


def init_connection():
    # 'mysql://uid:pwd@localhost/mydb?charset=utf8'
    engine = create_engine('mssql+pymssql://%s:%s@%s/%s?charset=utf8' % (
        settings.HOST_USER,
        settings.HOST_PASSWORD,
        settings.HOST,
        settings.DB
    ), poolclass=NullPool)

    connection = engine.connect()
    metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
    table_schema = sqlalchemy.Table(TABLE, metadata, autoload=True)
    return engine, connection, table_schema


def insert_data(insert_list, ids, log):
    log.info('Saving the data.....')
    engine, connection, table_schema = init_connection()
    # 创建Session:
    Session = sessionmaker(bind=engine)
    session = Session()

    # 添加之前先删除之前的统计
    sql_text = table_schema.delete().where(table_schema.columns.TreasureID == bindparam('TreasureID'))
    content = list()
    for i in ids:
        content.append({
            'TreasureID': i,
        })
    connection.execute(sql_text, content)
    session.commit()

    # 添加新的统计
    try:
        connection.execute(table_schema.insert(), insert_list)
        session.commit()
    except Exception as e:
        log.error('Having error during the saving....')
        log.error(e)
    finally:
        session.close()
        connection.close()


def nlp_process_with_sw(data, model, s_w):
    content = data['rateContent']
    res_s = SnowNLP(content)
    words = pseg.cut(content)
    new_sent = defaultdict(list)
    sentence = ''
    for w in words:
        if w.word not in s_w:
            new_sent[w.flag].append(w.word)
            sentence += w.word + ' '
    # 标签分类
    # TODO：多标签分类，引入更多维度的标签
    predict_tag = str(model.predict(sentence.strip()))
    return new_sent, res_s.sentiments, unicode(predict_tag, 'utf8')


def load_data(id_list, begin_date, end_date):
    # 整理数据
    conn = init_sql()
    sql_text = "SELECT RateDate,TreasureID, RateContent FROM V_Treasure_Evaluation (nolock) " \
               "WHERE TreasureID in %s and RateDate > '%s' and RateDate < '%s';" % (str(id_list), begin_date, end_date)
    return pd.io.sql.read_sql(sql_text, con=conn)


def load_data_mongo(id_list, begin_date, end_date):
    conn = init_mongo_sql()
    table = conn.CommentDB.commentContentTB
    # commentContentTB
    datas = table.find(
        {
            'TreasureID': {'$in': id_list},
            'RateDate': {"$gte": begin_date},
            'RateDate': {"$lte": end_date},
        },
        {'_id': 0, 'TreasureID': 1, 'rateContent': 1, 'RateDate': 1},
    )
    df = pd.DataFrame(list(datas))
    df['RateDate'] = df['RateDate'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    return df.drop_duplicates()


def load_data_excel():
    df = pd.read_excel('data_treasure_4.xls')
    df['RateDate'] = pd.to_datetime(df['RateDate'])
    df['TreasureID'] = df['TreasureID'].astype('str')
    return df


def get_data(ids,  b_date, end_data, log, stop_word):
    # b_date = b_date.strftime('%Y-%m-%d')
    # end_data = end_data.strftime('%Y-%m-%d')
    # 选择数据来源
    df = load_data_mongo(ids, b_date, end_data)
    # df = load_data_excel()
    # df = pd.read_excel('data_treasure.xls')
    df['RateDate'] = pd.to_datetime(df['RateDate'])
    # df_group = df['RateDate'].groupby([df.RateDate.values.astype('datetime64[D]')]).size()
    res = list()
    log.info('Have %d comments need to process' % len(df))
    # 分类模型导入
    new_grocery = Grocery('sample2')
    new_grocery.load()
    for record_data in range(0, len(df)):
        # 按日期分类摘取内容
        # tmp_df = df[df['RateDate'] > df_group.index[record_data]][df['RateDate'] < df_group.index[record_data + 1]]
        # 自然语言处理
        content_sw, level, tag = nlp_process_with_sw(df.iloc[record_data], new_grocery, stop_word)
        # 记录结果
        res.append({
            'RateContent': json.dumps(content_sw, ensure_ascii=False),
            'RateDate': df.iloc[record_data]['RateDate'],
            'TreasureID': df.iloc[record_data]['TreasureID'],
            'Level': level,
            'Tag': tag,
            'Sentence': df.iloc[record_data]['rateContent'],
        })
    return res


# 读取 filename 路径 的每一行数据 并返回 utf-8
def read_lines(filename):
    fopen = codecs.open(filename, 'r', 'gbk')
    data = []
    for x in fopen.readlines():
        if x.strip() != '':
            data.append(x.strip())
    fopen.close()
    return data


def read_xls(filename):
    df_ids = pd.read_excel(filename)

    ids_list = list()
    for i in range(len(df_ids)):
        ids_list.append(df_ids.iloc[i]['TreasureID'])
    return ids_list


def read_db(begin_date, log):
    begin_date_stamp = (begin_date - timedelta(days=1))
    projects = sql.get_project_data_mongo(begin_date_stamp)
    # log.info('having %d projects today.' % len(projects))
    insert_jobs = list()
    check_jobs = list()
    # 区分任务
    for project in projects:
        if project['ItemStatus'] != 1:
            # 可以去统计
            check_jobs.append(project['ItemID'])
            # 初始所有jobs 都是新的
            insert_jobs.append(project['ItemID'])

    # 检查是否已经统计
    print check_jobs
    jobs = sql.check_jobs_in_db(check_jobs)
    # 根据状态，选择需要统计的宝贝评论，如果更新时间不是同一天，更新状态，再统计，
    # 同一天，就忽略
    project_ids = list()
    today = begin_date.replace(begin_date.year, begin_date.month, begin_date.day, 0, 0)
    for job in jobs:
        tmp_time = dt.strptime(job[1].split('.')[0], '%Y-%m-%d %H:%M:%S')
        if tmp_time < today:
            project_ids.append(job[0])

        # 删掉已经存在job
        if job[0] in insert_jobs:
            insert_jobs.remove(job[0])

    # 插入新任务
    project_ids += insert_jobs
    # find all the treasure ids
    if len(project_ids) == 0:
        log.info('Do not have new work to do and finish .....')
        exit(0)
    res = sql.find_treasure_ids_mongo(project_ids)
    t_ids = list()
    for t_id in res:
        t_ids.append(str(t_id['TreasureID']))
    return tuple(t_ids), project_ids


if __name__ == '__main__':
    created = dt.today()
    begin_date = dt(1900, 2, 16)

    log = log_init('%s.log' % created.strftime('%Y_%m_%d'))
    log.info('initiation the data.....')

    stop_word = read_lines(os.path.join(BASE_DIR, 's_w.txt'))
    # 读取评论ids
    # treasure_ids = read_xls('treasure_ids.xls')

    # 读取数据库的ids
    treasure_ids, p_ids = read_db(created, log)
    # if len(treasure_ids) == 1:
    #     treasure_ids = list(treasure_ids)
    #     treasure_ids.append(treasure_ids[0])
    #     treasure_ids = tuple(treasure_ids)
    log.info('Having %d treasures to do' % len(treasure_ids))
    insert_data(get_data(treasure_ids, begin_date, created, log, stop_word), treasure_ids, log)

    # 更新project状态
    sql.finish_jobs(p_ids, created)

    log.info('------ Finish: %s  -------' % str(treasure_ids))

    log.info('-------------Finish the work---------------')

    # dd = load_data_mongo(("36809342636", "36809342636"), begin_date, created)
    # print dd.head()
    # read_db(begin_date, None)
