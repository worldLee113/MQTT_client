# encoding: utf-8


import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(r"lyms/#")


def on_message(client, userdata, msg):
    print(msg.topic + " " + ":" + str(msg.payload))



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("1.71.143.57", 1883, 60)
client.username_pw_set('sxycly', 'lygs0311')
client.loop_forever()
