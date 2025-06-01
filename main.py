import os
from datetime import datetime
from typing import Dict
from zoneinfo import ZoneInfo
from apscheduler.schedulers.background import BackgroundScheduler
import paho.mqtt.client as mqtt
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from models.models import DeviceStatusInfo, DeviceStatus, RoomStatusAndTimestamp, DeviceIdAndPulseTimeStamp, \
    OutletStatusEnum
import sys

DEVICE_TZ = ZoneInfo("Asia/Manila")
NODE_HEART_PULSE_BASE = "heart_pulse"
NODE_ROOM_STATUS_BASE = "room_status"
map_of_pulse: Dict[str, DeviceIdAndPulseTimeStamp] = {}
map_of_room_status: Dict[str, RoomStatusAndTimestamp] = {}
map_of_resolved_status: Dict[str, DeviceStatusInfo] = {
    "RM301/SOCKET-1": DeviceStatusInfo(device_id="SOCKET-1", room="RM301")
}


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    topics = (
        f"{NODE_HEART_PULSE_BASE}/#",
        f"{NODE_ROOM_STATUS_BASE}/#"
    )
    for topic in topics:
        client.subscribe(topic)


def on_message(client, userdata, msg):
    topic = msg.topic
    now = datetime.now(tz=DEVICE_TZ)
    if NODE_HEART_PULSE_BASE in topic:
        try:
            topic_base, room_id, device_id = topic.split("/")
            map_of_pulse[room_id + "/" + device_id] = DeviceIdAndPulseTimeStamp(
                device_id=device_id,
                room_id=room_id,
                timestamp=now)
        except ValueError:
            print("Malformed topic.")
    elif NODE_ROOM_STATUS_BASE in topic:
        try:
            topic_base, room_id, device_id = topic.split("/")
            map_of_room_status[room_id + "/" + device_id] = RoomStatusAndTimestamp(
                device_id=device_id,
                time_received=now,
                is_turned_on=msg.payload.decode == "1")
            print(map_of_room_status)
        except ValueError:
            print("Malformed topic.")


if __name__ == '__main__':
    load_dotenv(sys.argv[1])
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    url = os.getenv("MQTT_URL")
    port = os.getenv("MQTT_PORT")
    password = os.getenv("MQTT_PASSWORD")
    username = os.getenv("MQTT_USERNAME")
    service_acc_path = os.getenv("SERVICE_ACCOUNT_PATH")

    my_scheduler = BackgroundScheduler()
    my_scheduler.start()

    from firestore.device_firestore import set_device_status


    def eval_pulse():
        now = datetime.now(tz=DEVICE_TZ)
        dupe_dict = map_of_resolved_status.copy()
        map_of_changed_id: dict[str, DeviceStatusInfo] = {}
        for key in map_of_pulse:
            status_info = dupe_dict.get(key)
            if status_info is None:
                key_split = key.split("/")
                dupe_dict[key] = DeviceStatusInfo(device_id=key_split[1], room=key_split[0])
                status_info = dupe_dict.get(key)
            device_id_and_pulse_timestamp = map_of_pulse[key]
            diff = (now - device_id_and_pulse_timestamp.timestamp).total_seconds()
            status: bool
            if diff <= 5:
                status = True
            else:
                status = False
            if status != status_info.is_board_active:
                status_info.is_board_active = status
                map_of_changed_id[key] = status_info
        for key in map_of_room_status:
            status_info = dupe_dict.get(key)
            room_status_and_timestamp = map_of_room_status[key]
            diff = now - room_status_and_timestamp.time_received
            new_room_status: bool

            # Check if timestamp is within the 5-second threshold
            if diff.total_seconds() <= 5:

                # Check determine status
                if room_status_and_timestamp.is_turned_on:
                    new_room_status = True
                else:
                    new_room_status = False
            else:
                new_room_status = False

            # Check if the new_room_status is diff from the current one, ignore if not different.
            if new_room_status != status_info.room_status:
                changed_status = map_of_changed_id.get(key)

                # Check if the obj was previously modified from previous check for device status.
                if changed_status is None:

                    # If it does not yet exist, add.
                    status_info.room_status = new_room_status
                    map_of_changed_id[key] = status_info
                else:

                    # If it does exist, update.
                    changed_status.room_status = new_room_status

        # Update firestore
        for id_of_changed in map_of_changed_id:
            map_of_resolved_status[id_of_changed] = map_of_changed_id.get(id_of_changed)
            set_device_status(map_of_changed_id[id_of_changed])


    my_scheduler.add_job(eval_pulse, trigger=IntervalTrigger(seconds=2))
    mqttc.username_pw_set(
        username=username,
        password=password)
    mqttc.connect(url, int(port), 60)
    mqttc.loop_forever()
