from lxml import etree
import requests
import pymysql
import datetime

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'Cookie': '''SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WF7U-x.8qqTTTbE-aYUsGiD; SINAGLOBAL=5659741051489.444.1654159088546; SUB=_2AkMURLLAf8NxqwJRmPAWz2nqZYR2yArEieKiGEMbJRMxHRl-yT9jqmlftRB6P8ScL2mxLdPNo7hLPFuuJwjs39G1I09v; _s_tentry=www.google.com.hk; UOR=,,www.google.com.hk; Apache=4040152324424.4976.1667033968795; ULV=1667033968808:9:2:1:4040152324424.4976.1667033968795:1665203058106'''
}
url = 'https://s.weibo.com/top/summary?cate=realtimehot'
response = requests.get(url, headers=headers)
html = etree.HTML(response.text)
# print(response.status_code)
all = html.xpath('//*[@id="pl_top_realtimehot"]/table/tbody/tr')
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8')
cursor = db.cursor()
for tr in all:
    title = tr.xpath('./td[2]/a/text()')[0]
    # parse_url = tr.xpath('./td[2]/a/@href')[0]
    detail_url = 'https://s.weibo.com' + tr.xpath('./td[2]/a/@href')[0]
    zhishu = tr.xpath('string(.//td/span)')
    port = tr.xpath('./td[1]/text()')
    print("".join(port))
    print(title)
    # print(parse_url)
    print(detail_url)
    print(zhishu)
    print(create_time)
    print("===============================================================================================")
    sql = """INSERT INTO spot_weibo(spot_weibo_title,spot_weibo_url, spot_weibo_port,
     spot_weibo_zhishu,spot_weibo_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, detail_url, port, zhishu, create_time)
    try:
        # 执行sql语句
        cursor.execute(sql, val)
        # 提交到数据库执行
        db.commit()
        print("正常执行")
    except Exception as e:
        #     # 如果发生错误则回滚
        print(e)
        db.rollback()