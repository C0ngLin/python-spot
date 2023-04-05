from lxml import etree
import requests
import datetime
import pymysql

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
}
url = 'https://hao.360.com/histoday/'
response = requests.get(url, headers=headers)
html = etree.HTML(response.text)
all = html.xpath("//div[2]/div[3]/dl")
print(all)
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
    port = tr.xpath('string(./dt/em)')
    title = tr.xpath('string(./dt/text())')
    url = tr.xpath('string(./dd/div/div[2]/a/@href)')
    # text = tr.xpath('string(./dd/div/div[1]/text())')
    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    finaltitle = title[2:] + ""
    # finaltext = text[25:].strip() + ""
    print(port)
    print(title[2:])
    print(url)
    # print(text[25:].strip())
    print(create_time)
    print("========================")
    sql = """INSERT INTO spot_today(spot_today_title,spot_today_url, spot_today_port,
     spot_today_zhishu,spot_today_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (finaltitle, url, paiming - 1, 0, create_time)
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
