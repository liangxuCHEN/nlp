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
from sqlalchemy import create_engine
import logging
from collections import defaultdict
import settings
from tgrocery import Grocery


# 调用 readLines 读取停用词
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'chat_data_mining', 'DM_sentiment')
STOP_WORDS = None
TABLE = 'T_DCR_Comment'
log = None


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


def insert_data(insert_list):
    log.info('Saving the data.....')
    engine, connection, table_schema = init_connection()
    # 创建Session:
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        connection.execute(table_schema.insert(), insert_list)
        session.commit()
    except Exception as e:
        log.error('Having error during the saving....')
        log.error(e)
    finally:
        session.close()
        connection.close()


def nlp_process_with_sw(data, model):
    content = data['RateContent']
    res_s = SnowNLP(content)
    new_sent = defaultdict(list)
    for tag in res_s.tags:
        if tag[0] not in STOP_WORDS:
            new_sent[tag[1]].append(tag[0])
    predict_tag = model.predict(''.join(res_s.words))
    return new_sent, res_s.sentiments, predict_tag


def load_data(id_list, begin_date, end_date):
    # 整理数据
    conn = init_sql()
    sql_text = "SELECT RateDate,TreasureID, RateContent FROM V_Treasure_Evaluation (nolock) " \
               "WHERE TreasureID in %s and RateDate > '%s' and RateDate < '%s';" % (str(id_list), begin_date, end_date)
    return pd.io.sql.read_sql(sql_text, con=conn)


def load_data_excel():
    df = pd.read_excel('data_treasure_4.xls')
    df['RateDate'] = pd.to_datetime(df['RateDate'])
    df['TreasureID'] = df['TreasureID'].astype('str')
    return df


def get_data(ids,  b_date, end_data):
    b_date = b_date.strftime('%Y-%m-%d')
    end_data = end_data.strftime('%Y-%m-%d')
    # 选择数据来源
    df = load_data(ids, b_date, end_data)
    # df = load_data_excel()
    # df = pd.read_excel('data_treasure.xls')
    df['RateDate'] = pd.to_datetime(df['RateDate'])
    # df_group = df['RateDate'].groupby([df.RateDate.values.astype('datetime64[D]')]).size()
    res = list()
    log.info('Have %d comments need to process' % len(df))
    # 分类模型导入
    new_grocery = Grocery('sample')
    new_grocery.load()
    for record_data in range(0, len(df)):
        # 按日期分类摘取内容
        # tmp_df = df[df['RateDate'] > df_group.index[record_data]][df['RateDate'] < df_group.index[record_data + 1]]
        # 自然语言处理
        content_sw, level, tag = nlp_process_with_sw(df.iloc[record_data], new_grocery)
        # 记录结果
        res.append({
            'RateContent': json.dumps(content_sw, ensure_ascii=False),
            'RateDate': df.iloc[record_data]['RateDate'],
            'TreasureID': df.iloc[record_data]['TreasureID'],
            'Level': level,
            'Tag': tag,
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


if __name__ == '__main__':
    created = dt.today()
    begin_date = dt(2017, 4, 16)
    log = log_init('%s.log' % created.strftime('%Y_%m_%d'))
    log.info('initiation the data.....')
    score_var = list()
    STOP_WORDS = read_lines(os.path.join(BASE_DIR, 's_w.txt'))
    # TODO:后面从POST取得
    treasure_ids = ('521307282427', '36809342636')
    insert_data(get_data(treasure_ids, begin_date, created))
    log.info('-------------Finish the work---------------')
