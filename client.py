import paho.mqtt.client as mqtt
import pymysql
import json
import re
HOST = "1.71.143.57"
PORT = 1883

def sql_test(data):
    db = pymysql.connect(host='127.0.0.1',
                         port=3306,
                         user='root',
                         passwd='123456',
                         db='test',
                         charset='utf8mb4', )
    cursor = db.cursor()
    sql = "INSERT INTO test(collectTime,deviceId,deviceType,dewPoint,ecs,farmId,humidity,illuminance,insects,pictures,pressure,rainfall,soils,temperature,windDirection,windDirectionDesc,windScale,windSpeed)" \
          "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
          data["collectTime"],
          data["deviceId"], data["deviceType"], data["dewPoint"], data["ecs"], data["farmId"], data["humidity"],
          data["illuminance"], data["insects"], data["pictures"], data["pressure"], data["rainfall"], data["soils"],
          data["temperature"], data["windDirection"], data["windDirectionDesc"], data["windScale"], data["windSpeed"])

    cursor.execute(sql)  # 执行sql语句
    db.commit()  # 执行sql语句
    print('chenggong')
    # except:
    #     db.rollback()  # 发生错误时回滚
    #     print('cuowu')

    # 关闭数据库连接
    db.close()

def on_message_callback(client, userdata, message):

    print(message.topic + " " + ":" + str(message.payload))
    a = re.findall(r'"mag":"(.*?)"}',str(message.payload),re.S)


    sql_test(eval(a[0]))

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(r"lyms/#")


def main():
    client = mqtt.Client('test')
    client.connect(HOST, PORT, 60)
    client.username_pw_set('sxycly', 'lygs0311')
    client.on_connect = on_connect
    client.on_message = on_message_callback
    client.loop_forever()

if __name__ == '__main__':
    main()
