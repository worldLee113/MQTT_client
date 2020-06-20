#!/usr/bin/python
# -*- coding: UTF-8 -*-
import paho.mqtt.client as mqtt
import pymysql
import json
import socket
import requests
import re
import time
import os
import os.path
import datetime
import hashlib
import sys
import subprocess
import threading
import multiprocessing
from DBUtils.PooledDB import PooledDB
import logging
import random

global DB_POOL

HOST_DB     = "1.71.143.57"
HOST_MQTT1  = "1.71.135.106"
HOST_MQTT2  = "1.71.143.57"
#HOST_TCP    = "192.168.0.119"
HOST_TCP    = "202.193.60.215"
HOST_AIOT   = "http://api.xy-aiot.com/rest/api/v1"
PORT_MQTT   = 1883
PORT_TCP    = 8896

AIOT_API_KEY='111111'
AIOT_DEV_SN=[]
AIOT_DEV_LIVE_ADDR={} 
IMAGE_BASE_DIR='/root/Desktop/DataSources/lzawt/video_pics'    # linux  
#IMAGE_BASE_DIR=r'D:\opt\lzawt\video_pics'  # local Windows test

Mutex = threading.Lock() # 创建同步锁

# 初始化日志模块
def logger_init():
    global Logger
    Logger = logging.getLogger()
    Logger.setLevel(logging.INFO)  # Log等级总开关
    # 控制台logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  # 输出到console的log等级的开关
    # 文件logger 
    rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
    log_path = os.path.join(IMAGE_BASE_DIR,'logs_')
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logfile = log_path + rq + '.log' 
    fh = logging.FileHandler(logfile, mode='w')
    fh.setLevel(logging.WARNING)  # 输出到file的log等级的开关
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    Logger.addHandler(fh)

    ch.setFormatter(formatter)
    Logger.addHandler(ch)

# 初始化数据库连接
def db_init():
    global DB_POOL
    try:
        maxconnections = 5  # 最大连接数
        DB_POOL = PooledDB(
        pymysql,
        maxconnections,
        host=HOST_DB,
        user='root',
        port=3306,
        passwd='GSlz123456',
        db='LZAWTLY')
    except Exception as e:
        Logger.critical("Mysql init failed:{}".format(e))
        exit(0) 
    Logger.debug("Mysql init successful!")
    
# 数据库存储接口(多线程)
def db_store(sql):
    try:
        con=DB_POOL.connection()
        cur=con.cursor() 
        cur.execute(sql)
        con.commit()
    except Exception as e:
        con.rollback() # 事务回滚
        Logger.error("DB store error: {}".format(e))

# 初始化mqtt连接
def mqtt_init(host,user,passwd):
    try:
        client = mqtt.Client('blockchain')
        client.connect(host, PORT_MQTT, 60)
        client.username_pw_set(user, passwd)
        return client
    except Exception as e:
        Logger.critical("Mqtt server init failed: {}".format(e))
        exit(0) 
    Logger.info("Mqtt server started!")

def mqtt1_on_message_callback(client, userdata, message):
    Logger.info("获取消息：{}".format(str(message.payload,encoding="utf-8")))
    mqtt_data_store(str(message.payload,encoding="utf-8"))

def mqtt1_on_connect(client, userdata, flags, rc):
    Logger.info("Mqtt1 已连接：{}".format(str(rc)))
    client.subscribe(r"lzly/#")
    Logger.info("Mqtt1 已订阅")

def mqtt2_on_message_callback(client, userdata, message):
    Logger.info("获取消息：{}".format(str(message.payload,encoding="utf-8")))
    mqtt_data_store(str(message.payload,encoding="utf-8"))

def mqtt2_on_connect(client, userdata, flags, rc):
    Logger.info("Mqtt2 已连接：{}".format(str(rc)))
    client.subscribe(r"lyms/#")
    Logger.info("Mqtt2 已订阅")    

# 保存mqtt数据
def mqtt_store_pic(id,name,url):
    date=time.strftime("%Y-%m-%d")
    dir=os.path.join(IMAGE_BASE_DIR,id,date)
    if not os.path.exists(dir):
        os.makedirs(dir)
        Logger.debug("创建新文件夹：{}".format(dir))
    tmp=str(random.randint(1,1000000))+".temp"
    path=os.path.join(dir,tmp)
    try:   #下载 
        r = requests.get(url)
        f = open(path, "wb+")
        f.write(r.content)
        f.flush()
        f.seek(0)
        #计算哈希值
        sha256obj=hashlib.sha256()
        sha256obj.update(f.read())  
        file_name = sha256obj.hexdigest()
        new_path=os.path.join(dir,file_name+'.jpg')
        f.close()
        os.rename(path,new_path)
        size=os.path.getsize(new_path)
        # 存储索引值
        sql='INSERT INTO pic_index(name,size,date,device,point)\
        VALUES ("%s","%s","%s","%s","%s")' %(file_name,size,date,id,name)
        db_store(sql)
    except Exception as e:
       Logger.error("图片存储出错：{}".format(e))

    Logger.info("图片存储成功：{}".format(path))

def mqtt_data_store(data):
    dic=json.loads(data)
 
    if 'deviceId' not in dic.keys():
        return

    if 'co2' not in dic.keys():
        dic['co2']='-1.0'

    sql = 'INSERT INTO mqt_sensor VALUES ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' % (
        dic["collectTime"], dic["deviceId"],    dic["deviceType"],    dic["dewPoint"],           dic["ecs"],        dic["farmId"],    
        dic["humidity"],    dic["illuminance"], dic["insects"],       dic["pressure"],           dic["rainfall"],   dic["soils"],
        dic["temperature"], dic["windDirection"], dic["windDirectionDesc"],  dic["windScale"],   dic["windSpeed"],  dic["co2"])

    db_store(sql)

    for pic in dic["pictures"]:
        Logger.info("图片链接：{}".format(pic))
        mqtt_store_pic(dic["deviceId"],dic["farmId"],pic)

    return

# 通过回调函数来处理mqtt服务
def mqtt1_processor():
    client = mqtt_init(HOST_MQTT1,'lzawt','gslzawt0931')
    client.on_connect = mqtt1_on_connect
    client.on_message = mqtt1_on_message_callback
    client.loop_forever()
    return

# 通过回调函数来处理mqtt服务
def mqtt2_processor():
    client = mqtt_init(HOST_MQTT2,'sxycly','lygs0311')
    client.on_connect = mqtt2_on_connect
    client.on_message = mqtt2_on_message_callback
    client.loop_forever()
    return

# aiot post 服务
def aiot_poster(data):
    header = {'X-Access-Token':AIOT_API_KEY,'Content-Type':'application/json'}
    req = requests.post(HOST_AIOT, data, headers=header)
    res = json.loads(str(req.content,encoding="utf-8"))
    if res['stateCode'] == "200":
        Logger.info("AIOT response ok: {}".format(res['data']))
        return res['data']
    else:
        Logger.error("AIOT response err: {}, msg:{}".format(res['code'],res['msg']))
        return {}

# 初始化aiot
def aiot_init():
    # 查询授权码绑定的设备列表
    data = aiot_poster(json.dumps({'apiCode':'GetDeviceList'}))
    if any(data) == True:
        Logger.info("设备列表查询成功！")
        name=data['productName']
        for device in data["deviceList"]:
            Logger.info("设备SN: {}".format(device['SN']))
            AIOT_DEV_SN.append(device['SN'])
        # 获取aiot的直播地址
        for sn in AIOT_DEV_SN:            
            data = aiot_poster(json.dumps({'apiCode':'GetLiveInfo','SN':sn}))        
            if any(data) == True:
                AIOT_DEV_LIVE_ADDR[sn]=data['rtmpLivePlayUrl']
                Logger.info("设备直播地址查询成功：{}->{}".format(sn,AIOT_DEV_LIVE_ADDR[sn]))  
            return name

    Logger.critical("AIOT init failed!")           
    return ''

# aiot保存图片 路径：deviceID/date/hash.jpg  id:deviceId(设备序列号) name:deviceName(采集点名),直播链接
def rmtp_pic_store(id,name,link):  #图片存储本地
    date=time.strftime("%Y-%m-%d")
    dir=os.path.join(IMAGE_BASE_DIR,id,date)
    if not os.path.exists(dir):
        os.makedirs(dir)
    tmp=str(random.randint(100000,1000000))+".temp"
    path=os.path.join(dir,tmp)
    ret = subprocess.run("ffmpeg -i "+link+" -ss 0 -y -vframes 1 -f image2 "+path,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE).returncode
    if ret == 0:
        #os.popen("ffmpeg -i "+link+" -ss 0 -y -vframes 1 -f image2 "+path)
        Logger.info("正在截图: {}".format(path))
        #计算哈希值
        if os.path.exists(path) == True:
            f = open(path,'rb')
            sha256obj=hashlib.sha256()
            sha256obj.update(f.read())
            file_name = sha256obj.hexdigest()
            f.close()
            new_path=os.path.join(dir,file_name+'.jpg')
            os.rename(path,new_path)
            size=os.path.getsize(new_path)
            #存储索引值
            sql='INSERT INTO pic_index(name,size,date,device,point)\
                VALUES ("%s","%s","%s","%s","%s")' %(file_name,size,date,id,name)
            db_store(sql)
        else:
            Logger.error('截图失败， 路径不存在！')
    else:
        if os.path.exists(path) == True:
            os.remove(path)
        Logger.error('截图失败，CMD error：{}'.format(ret))
    
    return

# 处理摄像头服务
def aiot_processor():
    # 初始化
    name=aiot_init()
    if name != '':
        while  True:   # 定时获取每个链接的截图
            for dev in  AIOT_DEV_SN:
                if AIOT_DEV_LIVE_ADDR[dev] != None:
                    rmtp_pic_store(dev,name+'_'+dev, AIOT_DEV_LIVE_ADDR[dev])  # 开启子线程
        # time.sleep(2*60*60)
            time.sleep(20) # 测试十秒钟一次

# 初始化tcp连接
def tcp_init():
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setblocking(False)   # 将socket设置为非阻塞. 在创建socket对象后就进行该操作.
        server.bind((HOST_TCP, PORT_TCP))
        server.listen(5)
        return server
    except Exception as e:
        Logger.critical("Tcp server init failed: {}".format(e))
        exit(0) 
    Logger.info("Tcp server init success.")

# 保存tcp数据，一组
m_tcp_data_reg=re.compile(r'^#(?P<sensor_c>[^&]*)&(?P<family_id1>[^@]*)@(?P<family_id2>[^*]*)*(?P<value>[^%]*)%(?P<soc_time>[^$]*)')
def tcp_data_store(data):
    global m_tcp_data_reg
    reg_match=m_tcp_data_reg.match(data)
    d_map=reg_match.groupdict()
    s_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sql='INSERT INTO tcp_sensor(sensor_c,family_id,value,soc_time,sys_time)\
        VALUES("%s","%s","%s","%s","%s")' %(d_map["sensor_c"],d_map["family_id1"]+d_map["family_id2"],d_map["value"],d_map["soc_time"],s_time)
    Logger.debug("Tcp data store: {}".format(sql))
    db_store(sql)
    return  

# 处理tcp服务
def tcp_processor():
    server=tcp_init()        
    client_list = []
    while True:
        try:
            connection, addr = server.accept()
            client_list.append((connection, addr))
            Logger.info("connected:{}".format(addr))

        # accept原本是阻塞的, 等待connect, 设置setblocking(False)后, accept不再阻塞,
        # 它会(不断的轮询)要求必须有connect来连接, 不然就引发BlockingIOError, 所以为了在没有connect时,
        # 我们捕捉这个异常并pass掉.
        except BlockingIOError:
            pass

        for client_socket, client_addr in client_list:
            try:
                client_recv = client_socket.recv(1024)
                if client_recv:
                    Logger.info("receive:{}>>>{}".format(client_addr, client_recv))
                    tcp_data_store(str(client_recv, encoding = "utf-8"))
                else:
                    client_socket.close()
                    Logger.info("offline:{}".format(client_addr))
                    client_list.remove((client_socket, client_addr))

            except (BlockingIOError, ConnectionResetError):
                pass


def main():
    # 初始化日志模块
    logger_init()
    # 初始化数据库连接
    db_init()
    # 多线程执行任务
    t1 = threading.Thread(target=tcp_processor,args=())
    t2 = threading.Thread(target=mqtt1_processor,args=())
    t3 = threading.Thread(target=mqtt2_processor,args=())
    t4 = threading.Thread(target=aiot_processor,args=())
    t1.start()
    Logger.info('线程1 已启动')
    t2.start()
    Logger.info('线程2 已启动')
    t3.start()
    Logger.info('线程3 已启动')
    t4.start()
    Logger.info('线程4 已启动')


if __name__ == '__main__':
    main()

