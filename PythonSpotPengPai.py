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
all = html.xpath('//*[@id="node-32"]/div/div[2]/div[1]/a')
# "//*[@id="page"]/div[2]/div[2]/div[1]/div[2]/div/div[1]/table/tbody/tr[1]"
print(all)
paiming = 0
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8')
cursor = db.cursor()
for item in all:
    if (paiming >= 15):
        break
    paiming = paiming + 1
    port = paiming
    title = item.xpath('string(./div/span[2])')
    zhishu = item.xpath('string(./div/span[3])')
    url = item.xpath('string(./@href)')
    # finalurl="https://tophub.today" + url
    print(paiming)
    print(title)
    print(url)
    print(create_time)
    print("==============================================")
    sql = """INSERT INTO spot_pengpai(spot_pengpai_title,spot_pengpai_url, spot_pengpai_port,
     spot_pengpai_zhishu,spot_pengpai_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, url, paiming, zhishu, create_time)
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