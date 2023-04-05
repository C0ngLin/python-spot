from lxml import etree
import requests
import datetime
import pymysql
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
}
url = 'https://tophub.today/'
r = requests.get(url, headers=headers)
html = etree.HTML(r.text)
# print(r.status_code)
all = html.xpath('//*[@id="node-6"]/div/div[2]/div[1]/a')
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
paiming = 1
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8')
cursor = db.cursor()
for tr in all:
    if (paiming > 50):
        break
    paiming = paiming + 1
    port = tr.xpath('string(./td[1])')
    title = tr.xpath('string(./div/span[2])')
    zhishu = tr.xpath('string(./div/span[3])')
    url = tr.xpath('string(./@href)')
    # finalurl="https://tophub.today" + url
    print(port[:-1])
    print(title)
    # print("https://tophub.today" + url)
    print(zhishu)
    print(create_time)
    print("==============================================")
    sql = """INSERT INTO spot_zhihu(spot_zhihu_title,spot_zhihu_url, spot_zhihu_port,
     spot_zhihu_zhishu,spot_zhihu_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, url, paiming - 1, zhishu, create_time)
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
