from lxml import etree
import datetime
import requests
import pymysql
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
}
url = 'https://tophub.today/'
response = requests.get(url, headers=headers)
html = etree.HTML(response.text)
all = html.xpath('//*[@id="node-5"]/div/div[2]/div[1]/a')
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
    if (paiming > 25):
        break
    print(paiming)
    paiming = paiming + 1
    title = item.xpath('string(./div/span[2])')
    # //*[@id="node-5"]/div/div[2]/div[1]/a[1]
    # //*[@id="node-5"]/div/div[2]/div[1]/a[1]/div/span[3]
    # //*[@id="node-5"]/div/div[2]/div[1]/a[1]/div/span[2]
    print(title)
    # port = item.xpath('string(./td[1])')
    # print(port)
    url = item.xpath('string(./@href)')
    # print("https://tophub.today" + url)
    print(url)
    # finalurl="https://tophub.today" + url
    zhishu = item.xpath('string(./div/span[3])')
    print(zhishu)
    print(create_time)
    print("==========================================")
    sql = """INSERT INTO spot_weixin(spot_weixin_title,spot_weixin_url, spot_weixin_port,
     spot_weixin_zhishu,spot_weixin_create_time)
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