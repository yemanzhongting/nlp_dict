#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import re,random,time
import requests
import sys
from tqdm import tqdm
import traceback
from datetime import datetime
from datetime import timedelta
from lxml import etree
import pymysql
import analysis

cookie = {"Cookie": "WM=73b88340b9ce8c38a5315ca5497ed967; ALF=1557734646; SUB=_2A25xsRgsDeRhGeNL6lsX8izFzjmIHXVTXbhkrDV6PUJbkdAKLWr4kW1NSTRg4mo4J1WgCYfNvnuKsBXRIOE1OboN; SUHB=0D-Vh6ob3Y9mhh; SCF=AkzfUngGM9xTBiRYVQT7ieV8xLX8WmKG-H81KFhttLavT-imt1sfYkvGWja_YbFBYj1A9L32FOLcRtwZAet6fx0.; SSOLoginState=1555392637; MLOGIN=1; WEIBOCN_FROM=1110006030; M_WEIBOCN_PARAMS=luicode%3D20000174%26uicode%3D20000174"}  # 将your cookie替换成自己的cookie

headers={'Upgrade-Insecure-Requests': '1','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',}

def clean_text(text):
    """清除文本中的标签等信息"""
    dr = re.compile(r'(<)[^>]+>', re.S)
    dd = dr.sub('', text)
    dr = re.compile(r'#[^#]+#', re.S)
    dd = dr.sub('', dd)
    dr = re.compile(r'@[^ ]+ ', re.S)
    dd = dr.sub('', dd)
    return dd.strip()

def create_table(db_username,password,dbname):
    """创建数据表"""
    db = pymysql.connect("localhost", db_username,password,dbname)
    cursor = db.cursor()
    sql = """CREATE TABLE IF NOT EXISTS new_weibo (
      weibo_id varchar(255) COLLATE utf8_bin DEFAULT NULL,
      created varchar(255) COLLATE utf8_bin DEFAULT NULL,
      uid_name  varchar(255) COLLATE utf8_bin DEFAULT NULL,
      dianzan varchar(255) COLLATE utf8_bin DEFAULT NULL,
      huifu varchar(255) COLLATE utf8_bin DEFAULT NULL,
      comment varchar(255) COLLATE utf8_bin DEFAULT NULL,
      url varchar(255) COLLATE utf8_bin DEFAULT NULL,
      fenci varchar(255) COLLATE utf8_bin DEFAULT NULL,
      score varchar(255) COLLATE utf8_bin DEFAULT NULL)"""
    try:
       cursor.execute(sql)
    except:
        print('error')
    db.close()
    print('创建表成功')

def write_mysql(db_username,password,dbname,weibo_id, created, uid_name, dianzan, huifu,comment, url,fenci,score):
    """写入微博到数据库"""
    db = pymysql.connect("localhost", db_username, password, dbname)
    cursor = db.cursor()
    try:
        sql = "INSERT INTO new_weibo(weibo_id ,created,uid_name,dianzan,huifu,comment,url,fenci,score)  VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
              (weibo_id ,created, uid_name, dianzan, huifu, comment, url,fenci,score)
        print(sql)
        cursor.execute(sql)
        # 执行sql语句
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
        print('错误')
    db.close()

def get_mysql(base_url,pageNum,word_count,db_username,password,dbname):

    db = pymysql.connect("localhost", db_username, password, dbname)
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    print("爬虫准备就绪...")

    base_url_deal = base_url + '%d'

    base_url_final = str(base_url_deal)

    for page in range(2, pageNum + 1):
        code = 0
        url = base_url_final % (page)

        # SQL 查询语句,进行数据的检索去重，断点续爬

        # SQL 查询语句
        #sql = "SELECT * FROM weibo WHERE URL == %s" % (url)
        sql = 'SELECT * FROM weibo'
        cursor.execute(sql)

        # 获取所有记录列表
        results = cursor.fetchall()
        if results==[]:#检索是否抓取过
            code=1
            print('url已经存在'+url)
        if code==0:
            lxml = requests.get(url, cookies=cookie,headers=headers).content
            print('正在crawling data page' + str(page))
            print(url)
            selector = etree.HTML(lxml)
            weiboitems = selector.xpath('//div[@class="c"][@id]')

            #设置微博抓取休息时间，防止服务器压力过大
            x = random.randint(1, 5)
            time.sleep(int(2 + x))
            print('爬虫正在休息')

            #页面逐条抓取
            for item in weiboitems:
                weibo_id = item.xpath('./@id')[0]
                created = item.xpath('.//span[@class="ct"]/text()')[0]

                uid_name = item.xpath('./a/text()')[0]
                uid = item.xpath('./a')[0].attrib['href']
                uid = uid[3:]
                dianzan = item.xpath('./span[@class="cc"]/a/text()')[0]
                huifu = item.xpath('./span[@class="cc"]/a/text()')[1]
                text = item.xpath('./span[@class="ctt"]/text()')

                try:
                   level = item.xpath('./img/@alt')[0]
                except:
                   level = item.xpath('./img/@alt')

                if len(text)==1:
                    wb_type='评论'
                    comment=text[0]
                else:
                    wb_type='回复'
                    try:
                       comment=text[1]
                    except IndexError:
                        comment=text

                word_count += 1
                data = {
                    'weibo_id':weibo_id,
                    'created':created,
                    'uid_name':uid_name,
                    'dianzan':dianzan,
                    'huifu':huifu,
                    'comment':comment,
                    'url':url
                }
                # 有时候由于xpath有时会抓到列表，由于建表已经设置了格式，这里判断如果是列表则取消
                for key in data:
                    print(key, ' value : ', data[key])
                    if type(data[key]) is dict:
                        data[key]=''
                #插入数据
                insert_data=[data[key] for key in data]

                fenci=str(' '.join(analysis.seg_word(insert_data[5])))
                score=str(analysis.setiment_score(insert_data[5]))
                print(fenci)
                print(score)

                write_mysql(db_username,password,dbname,insert_data[0],insert_data[1],insert_data[2],insert_data[3],insert_data[4],insert_data[5],insert_data[6],fenci,score)

                print('写入一条数据')

    print("成功爬取！")
    print("本事件微博信息入库完毕，共%d条" % (word_count - 4))


if __name__ == '__main__':

    url_list = [
        'https://weibo.cn/comment/HpzmF8bpc?uid=2145291155&rl=0&page=',  # m马云996
    ]

    db_username='root'
    password='mysql'
    dbname='test'

    #创建数据表
    create_table(db_username, password, dbname)

    #抓取url所有微博评论
    for index in range(0, len(url_list)):

        word_count = 1
        base_url = url_list[index]
        print(base_url)

        first_url = base_url + '1'
        html = requests.get(first_url, cookies=cookie).content
        selector = etree.HTML(html)

        controls = selector.xpath('//input[@name="mp"]')
        #判断会抓取多少页码
        if controls:
            pageNum = int(controls[0].attrib['value'])  # word_count初试为1
        else:
            pageNum = 1

        try:
            get_mysql(base_url, pageNum, word_count,db_username,password,dbname)
        except Exception as e:  # 抓住所有错误,一般放在最后
            print("未知错误", e)
            print('此条没有抓取成功' + str(index))
        else:
            print("进行下一条微博爬取...")

        index = index + 1

    print("全部完成！")