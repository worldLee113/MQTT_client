import pymysql
import json
data = {"collectTime":"2020-03-12 18:13:43",
"deviceId":"19372077",
"deviceType":0,
"dewPoint":15.48,
"ecs":[],
"farmId":"001041",
"humidity":89.6,
"illuminance":538.0,
"insects":[],
"pictures":["http://f.airag.cn/push/picture-temp/20200312/b247a93a17029aaee751bfeda073f2f2.jpg","http://f.airag.cn/push/picture-temp/20200312/b50250e01cfbf2829982847209aab9b3.jpg"],
"pressure":1016.5,
"rainfall":0.0,
"soils":[{"sensorId":"69380214","soilMoisture":5.6,"soilTemperature":17.5}],
"temperature":17.2,
"windDirection":123.0,
"windDirectionDesc":"\xe4\xb8\x9c\xe4\xb8\x9c\xe5\x8d\x97",
"windScale":1,
"windSpeed":1.1}
for i in data.keys():
    if isinstance(data[i],str):
        pass
    else:
        data[i]=json.dumps(data[i])
print(data)
print(type(data["ecs"]))

db = pymysql.connect(host='127.0.0.1',
         port=3306,
         user='root',
         passwd='123456',
         db='test',
         charset='utf8mb4',  )
cursor = db.cursor()
sql = "INSERT INTO test(collectTime,deviceId,deviceType,dewPoint,ecs,farmId,humidity,illuminance,insects,pictures,pressure,rainfall,soils,temperature,windDirection,windDirectionDesc,windScale,windSpeed)" \
      "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (data["collectTime"],
       data["deviceId"], data["deviceType"], data["dewPoint"], data["ecs"],data["farmId"],data["humidity"],data["illuminance"],data["insects"],data["pictures"],data["pressure"],data["rainfall"],data["soils"],data["temperature"],data["windDirection"],data["windDirectionDesc"],data["windScale"],data["windSpeed"])

cursor.execute(sql)  # 执行sql语句
db.commit()  # 执行sql语句
print('chenggong')
# except:
#     db.rollback()  # 发生错误时回滚
#     print('cuowu')

# 关闭数据库连接
db.close()