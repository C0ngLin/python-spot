from lxml import etree
import datetime
import requests
import pymysql

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
}
url = 'https://tophub.today/n/LwkvlBqez1'
response = requests.get(url, headers=headers)
html = etree.HTML(response.text)
all = html.xpath('//div/div/table/tbody/tr')
print(all)
paiming = 1
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8')
cursor = db.cursor()
for item in all:
    if (paiming > 10):
        break
    print(paiming)
    paiming = paiming + 1
    title = item.xpath('string(./td[2])')
    print(title)
    # port = item.xpath('string(./td[1])')
    # print(port)
    url = item.xpath('string(./td[2]/a/@href)')
    finallurl = "https://tophub.today" + url
    print("https://tophub.today" + url)
    zhishu = item.xpath('string(./td[3])')
    print(zhishu)
    print(create_time)
    print("==========================================")
    sql = """INSERT INTO spot_douban(spot_douban_title,spot_douban_url, spot_douban_port,
     spot_douban_zhishu,spot_douban_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, finallurl, paiming-1, zhishu, create_time)
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
