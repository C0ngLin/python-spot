from lxml import etree
import requests
import datetime
import pymysql
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
}
url = 'https://tophub.today/'
response = requests.get(url, headers=headers)
html = etree.HTML(response.text)
all = html.xpath('//*[@id="node-3"]/div/div[2]/div[1]/a')
print(all)
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
db = pymysql.connect(host='192.168.96.129',
                     user='root',
                     password='123456',
                     db='spot',
                     charset='utf8')
cursor = db.cursor()
paiming = 1
for item in all:
    # 排名
    print(paiming)
    paiming = paiming + 1
    # 标题
    title = item.xpath('string(./div/span[2])')
    "/html/body/div[1]/div[4]/div[3]/div[10]/div[2]/div/div[2]/div[1]/a[1]/div/span[2]"
    print(title)
    # 链接
    url = item.xpath('string(./@href)')
    print(url)
    # 图片地址
    # tieba_image_url = item.xpath('string(./img/@src)')
    # print(tieba_image_url)
    # 热搜指数
    zhishu = item.xpath('string(./div/span[3])')
    print(zhishu)
    # 创建时间
    print(create_time)
    print("===============================================================================================")
    sql = """INSERT INTO spot_tieba(spot_tieba_title,spot_tieba_url, spot_tieba_port,
     spot_tieba_zhishu,spot_tieba_create_time)
             VALUES (%s,%s,%s,%s,%s)"""
    val = (title, url, paiming-1, zhishu, create_time)
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
