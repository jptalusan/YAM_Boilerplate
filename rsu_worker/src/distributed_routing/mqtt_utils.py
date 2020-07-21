import paho.mqtt.client as mqtt
import threading
import time

# https://github.com/dm8tbr/org.eclipse.paho.mqtt.python/blob/master/examples/sub-class.py
class MyMQTTClass:

    mqtt.Client.connected_flag = False

    def __init__(self, client_id=None, host="localhost", port=1883, keep_alive=60, clean_session=True):
        self._mqttc = mqtt.Client(client_id=client_id, clean_session=clean_session)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_disconnect = self.mqtt_on_disconnect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe

        self._client_id = client_id
        self._host = host
        self._port = port
        self._keep_alive = keep_alive
        self._sub_thread_flag = False

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        self._mqttc.connected_flag = True
        # if __debug__:
        #     print("Connected to MQTT")

    def mqtt_on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self._mqttc.connected_flag = False
            print("Unexpected disconnection.")
            client.loop_stop()

    def mqtt_on_message(self, mqttc, obj, msg):
        print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
        if msg.topic == "test/topic2":
            self._mqttc.publish("test/topic", msg.payload, qos=0, retain=False)

    def mqtt_on_publish(self, mqttc, obj, mid):
        # if __debug__:
        #     print("Sent message: "+str(mid))
        pass

    def mqtt_on_subscribe(self, mqttc, obj, mid, granted_qos):
        # if __debug__:
        #     print("Subscribed: "+str(mid)+" "+str(granted_qos))
        pass

    def mqtt_on_log(self, mqttc, obj, level, string):
        if __debug__:
            print(string)

    def connect(self, debug=False):
        try:
            if debug:
                self._mqttc.connect("test.mosquitto.org", 1883, 60)
            else:
                self._mqttc.connect(self._host, 1883, 60)
        except ConnectionRefusedError as e:
            print(e)
            return e

    def open(self):
        if not self._sub_thread_flag:
            self._mqttc.loop_start()

    def send(self, topic, payload):
        # If not yet connected will not send. But if with this, the query code gets stuck
        while not self._mqttc.connected_flag:
            if __debug__:
                print("in wait loop")
            time.sleep(1)
        self._mqttc.publish(topic, payload, qos=0, retain=False)

    def close(self):
        rc = self._mqttc.loop_stop()
        return rc

    def sub_thread(self, topic_arr):
        self._sub_thread_flag = True
        for topic in topic_arr:
            self._mqttc.subscribe(topic, 0)
        self._mqttc.loop_forever()

    def start_sub_thread(self, topic_arr):
        mqtt_t = threading.Thread(target=self.sub_thread, args=(topic_arr,))
        mqtt_t.start()