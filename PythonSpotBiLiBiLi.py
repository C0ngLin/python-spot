from lxml import etree
import requests
import pymysql
import datetime

url = 'https://www.bilibili.com/v/popular/rank/all'  # 排行榜页面的url
head = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'referer': 'https://www.bilibili.com/',
    'x-csrf-token': '',
    'x-requested-with': 'XMLHttpRequest',
    'cookie': '',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
}
response = requests.get(url, headers=head)
html = etree.HTML(response.text)
all = html.xpath("//li[contains(@class,'rank-item')]")
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8mb4')
cursor = db.cursor()
for item in all:
    # 排名
    port = item.xpath('string(./@data-rank)')
    # print(port)
    # 标题
    title = item.xpath('string(.//div[@class="info"]/a)')
    # print(title)
    # 链接
    url = "https:" + item.xpath('string(./div/div/a/@href)')
    # print(url)
    # 图片地址
    # bilibili_image_url = item.xpath('//a/img/@src')
    # print(bilibili_image_url)
    # 热搜指数
    "./div/div[2]/div/div/span[1]/text()"
    zhisu = item.xpath('string(./div/div/div/div/span/text())')
    finalzhishu = zhisu.strip()
    # print(zhisu.strip())
    # 创建时间
    # print(create_time)
    # print("===============================================================================================")
    sql = """INSERT INTO spot_bilibili(spot_bilibili_title,spot_bilibili_url, spot_bilibili_port,
     spot_bilibili_zhishu,spot_bilibili_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, url, port, finalzhishu, create_time)
    try:
        # 执行sql语句
        cursor.execute(sql, val)
        # 提交到数据库执行
        db.commit()
        # print("正常执行")
    except Exception as e:
        #     # 如果发生错误则回滚
        print(e)
        db.rollback()
