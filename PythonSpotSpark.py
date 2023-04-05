import time

from pyspark.sql import SparkSession ,functions
# 驱动位置
# /Users/hejipei/soft/jar/mysql-connector-java-8.0.11.jar

# 连接spark
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName('My first app') \
    .config('spark.driver.extraClassPath', '/opt/azdir/Python-3.9.16/Lib/site-packages/*') \
    .getOrCreate()
url = 'jdbc:mysql://spot:3306/spot?&useSSL=false'
properties = {'user': 'root', 'password': '123456'}
table = "spot_main_hdfs"  # 写sql

# # 连接mysql
# df1 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_date(spot_main_date) = to_date(now()) ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df1 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df2 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) ORDER BY spot_main_hot DESC LIMIT 20;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df3 = spark.sql("SELECT * FROM spot_main_hdfs WHERE DATE_FORMAT(spot_main_date, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df4 = spark.sql("SELECT * from spot_main_hdfs where QUARTER(spot_main_date)=QUARTER(now()) ORDER BY spot_main_hot DESC LIMIT 50;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df5 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 地区日分析
# df6 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_area = 1 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df7 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_area = 2 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df8 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_area = 3 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df9 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_area = 4 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df10 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_area = 5 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 地区月分析
# df11 = spark.sql("SELECT * FROM spot_main_hdfs WHERE DATE_FORMAT(spot_main_date, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') and spot_main_area = 1 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df12 = spark.sql("SELECT * FROM spot_main_hdfs WHERE DATE_FORMAT(spot_main_date, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') and spot_main_area = 2 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df13 = spark.sql("SELECT * FROM spot_main_hdfs WHERE DATE_FORMAT(spot_main_date, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') and spot_main_area = 3 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df14 = spark.sql("SELECT * FROM spot_main_hdfs WHERE DATE_FORMAT(spot_main_date, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') and spot_main_area = 4 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df15 = spark.sql("SELECT * FROM spot_main_hdfs WHERE DATE_FORMAT(spot_main_date, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') and spot_main_area = 5 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 地区年分析
# df16 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_area = 1 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df17 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_area = 2 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df18 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_area = 3 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df19 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_area = 4 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df20 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_area = 5 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 分区日分析
# df21 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_sort = 1 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df22 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_sort = 2 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df23 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_sort = 3 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df24 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_sort = 4 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df25 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) and spot_main_sort = 5 ORDER BY spot_main_hot DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 分区月分析
# df26 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) and spot_main_sort = 1 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df27 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) and spot_main_sort = 2 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df28 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) and spot_main_sort = 3 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df29 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) and spot_main_sort = 4 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df30 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) and spot_main_sort = 5 ORDER BY spot_main_hot DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 分区年分析
# df31 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_sort = 1 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df32 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_sort = 2 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df33 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_sort = 3 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df34 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_sort = 4 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df35 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) and spot_main_sort = 5 ORDER BY spot_main_hot DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 评论 转发 点赞 榜单 日分析
# df36 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) ORDER BY spot_main_dianzan DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df37 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) ORDER BY spot_main_pinglun DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df38 = spark.sql("SELECT * FROM spot_main_hdfs WHERE to_days(spot_main_date) = to_days(now()) ORDER BY spot_main_zhuanfa DESC LIMIT 10;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 评论 转发 点赞 榜单 月分析
# df39 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) ORDER BY spot_main_dianzan DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df40 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) ORDER BY spot_main_pinglun DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df41 = spark.sql("SELECT * FROM spot_main_hdfs WHERE YEARWEEK(date_format(spot_main_date,'%Y-%m-%d')) = YEARWEEK(now()) ORDER BY spot_main_zhuanfa DESC LIMIT 30;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# # 评论 转发 点赞 榜单 年分析
# df42 =  ("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) ORDER BY spot_main_dianzan DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df43 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) ORDER BY spot_main_pinglun DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df44 = spark.sql("SELECT * from spot_main_hdfs where YEAR(spot_main_date)=YEAR(NOW()) ORDER BY spot_main_zhuanfa DESC LIMIT 100;").write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# 查看需数据
# sql_test = 'select * from test_hjp limit 1'
df = spark.read.jdbc(url=url, table=table, properties=properties)
# 一天内
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now())').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# 一周内
df.select('*').where('spot_main_date > date_sub(now(),7) and spot_main_date < (now())').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# 一月内
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now())').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# 一季度内
df.select('*').where('spot_main_date > date_sub(now(),90) and spot_main_date < (now())').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# 一年内
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now())').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
#
# -- 地区日榜
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_area = 1').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_area = 2').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_area = 3').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_area = 4').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_area = 5').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 地区月榜
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_area = 1').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_area = 2').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_area = 3').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_area = 4').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_area = 5').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 地区年榜
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_area = 1').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_area = 2').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_area = 3').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_area = 4').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_area = 5').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 分区日榜
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_sort = 1').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_sort = 2').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_sort = 3').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_sort = 4').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now()) and spot_main_sort = 5').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 分区月榜
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_sort = 1').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_sort = 2').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_sort = 3').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_sort = 4').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now()) and spot_main_sort = 5').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 分区年榜
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_sort = 1').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_sort = 2').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_sort = 3').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_sort = 4').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now()) and spot_main_sort = 5').sort(desc("spot_main_hot")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 评论 转发 点赞 榜单 日榜
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now())').sort(desc("spot_main_dianzan")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now())').sort(desc("spot_main_pinglun")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),1) and spot_main_date < (now())').sort(desc("spot_main_zhuanfa")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 评论 转发 点赞 榜单 月榜
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now())').sort(desc("spot_main_dianzan")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now())').sort(desc("spot_main_pinglun")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),30) and spot_main_date < (now())').sort(desc("spot_main_zhuanfa")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# -- 评论 转发 点赞 榜单 年榜
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now())').sort(desc("spot_main_dianzan")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now())').sort(desc("spot_main_pinglun")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
df.select('*').where('spot_main_date > date_sub(now(),365) and spot_main_date < (now())').sort(desc("spot_main_zhuanfa")).limit(100).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
table2 = "spot_main_spark"
df2 = spark.read.jdbc(url=url,table = table2 , properties=properties)
df2.distinct().write.jdbc(url, 'spot_main_spark_finally', mode='overwrite', properties=properties)

# df.sort(desc("spot_main_hot")).write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
# df.write.jdbc(url, 'spot_main_spark', mode='append', properties=properties)
spark.stop()
