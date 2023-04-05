import requests
import pymysql
import datetime
from lxml import etree

headers = {
    'Connection': 'Keep-Alive',
    'Accept': 'text/html, application/xhtml+xml, */*',
    'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
    'Host': 'top.baidu.com',
    'Upgrade-Insecure-Requests': '1',
}
url = 'https://top.baidu.com/board?tab=realtime'
response = requests.get(url, headers=headers)
html = etree.HTML(response.text)
all = html.xpath("//div[contains(@class,'category-wrap_iQLoo')]")
dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8')
cursor = db.cursor()
for single in all:
    # 获取热搜排名
    rank = single.xpath('string(./a/div)')
    print('排名是：{}'.format(rank))
    # 获取标题
    title = single.xpath('string(.//div[@class="c-single-text-ellipsis"])')
    print('标题是：{}'.format(title))
    # 获取链接
    url = single.xpath('string(./a/@href)')
    print('链接是：{}'.format(url))
    # 获取图片地址
    # pic = single.xpath('string(./a/img/@src)')
    # print('图片地址是：{}'.format(pic))
    # 获取热搜指数hot-index
    zhishu = single.xpath('string(.//div[contains(@class,"hot-index_1Bl1a")])')
    print('热搜指数是：{}'.format(zhishu))
    print(dt)
    print("===============================================================================================")

    sql = """INSERT INTO spot_baidu(spot_baidu_title,spot_baidu_url, spot_baidu_port,
     spot_baidu_hot,spot_baidu_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, url, rank, zhishu, dt)
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
