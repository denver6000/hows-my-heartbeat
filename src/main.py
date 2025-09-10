import asyncio
from dataclasses import asdict
import json
import os
import threading
from datetime import datetime
from typing import Dict
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from src.firestore.device_firestore import extract_resolved_slots_by_day_v2, process_changes_from_firestore, process_schedule_raw_from_firestore, process_schedule_raw_with_tracking, set_device_status, set_hub_status
from src.models.models import DeviceStatusInfo, RoomStatusAndTimestamp, DeviceIdAndPulseTimeStamp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

DEVICE_TZ = ZoneInfo("Asia/Manila")
NODE_HEART_PULSE_BASE = "heart_pulse"
NODE_ROOM_STATUS_BASE = "room_status"
HUB_HEARTBEAT = "hub_heartbeat"

# Thread locks for thread-safe access
pulse_lock = threading.Lock()
room_status_lock = threading.Lock()
hub_pulse_lock = threading.Lock()
hub_status_lock = threading.Lock()
resolved_status_lock = threading.Lock()

map_of_pulse: Dict[str, DeviceIdAndPulseTimeStamp] = {}
map_of_room_status: Dict[str, RoomStatusAndTimestamp] = {}
map_of_resolved_status: Dict[str, DeviceStatusInfo] = {
    "RM301/SOCKET-1": DeviceStatusInfo(device_id="SOCKET-1", room="RM301")
}
map_of_hub_pulse: Dict[str, datetime] = {}
map_of_hub_status: Dict[str, bool] = {
}


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    topics = (
        f"{NODE_HEART_PULSE_BASE}/#",
        f"{NODE_ROOM_STATUS_BASE}/#",
        f"{HUB_HEARTBEAT}"
    )
    for topic in topics:
        client.subscribe(topic)


def on_message(client, userdata, msg):
    topic = msg.topic
    now = datetime.now(tz=DEVICE_TZ)
    if NODE_HEART_PULSE_BASE in topic:
        try:
            topic_base, room_id, device_id = topic.split("/")
            with pulse_lock:
                map_of_pulse[room_id + "/" + device_id] = DeviceIdAndPulseTimeStamp(
                    device_id=device_id,
                    room_id=room_id,
                    timestamp=now)
        except ValueError:
            print("Malformed topic.")
    elif NODE_ROOM_STATUS_BASE in topic:
        try:
            topic_base, room_id, device_id = topic.split("/")
            with room_status_lock:
                map_of_room_status[room_id + "/" + device_id] = RoomStatusAndTimestamp(
                    device_id=device_id,
                    time_received=now,
                    is_turned_on=msg.payload.decode() == "1")
        except ValueError:
            print("Malformed topic.")
    elif HUB_HEARTBEAT in topic:
        with hub_pulse_lock:
            map_of_hub_pulse[HUB_HEARTBEAT] = now


def eval_pulse(firestore_db):
    now = datetime.now(tz=DEVICE_TZ)

    # Create thread-safe copies of the data
    with pulse_lock:
        pulse_copy = map_of_pulse.copy()
    with room_status_lock:
        room_status_copy = map_of_room_status.copy()
    with resolved_status_lock:
        dupe_dict = map_of_resolved_status.copy()

    map_of_changed_id: dict[str, DeviceStatusInfo] = {}

    for key in pulse_copy:
        status_info = dupe_dict.get(key)
        if status_info is None:
            key_split = key.split("/")
            dupe_dict[key] = DeviceStatusInfo(device_id=key_split[1], room=key_split[0])
            status_info = dupe_dict[key]

        device_id_and_pulse_timestamp = pulse_copy[key]
        diff = (now - device_id_and_pulse_timestamp.timestamp).total_seconds()
        status: bool = diff <= 5

        if status != status_info.is_board_active:
            status_info.is_board_active = status
            map_of_changed_id[key] = status_info

    for key in room_status_copy:
        status_info = dupe_dict.get(key)
        if status_info is None:
            key_split = key.split("/")
            dupe_dict[key] = DeviceStatusInfo(device_id=key_split[1], room=key_split[0])
            status_info = dupe_dict[key]

        room_status_and_timestamp = room_status_copy[key]
        diff = now - room_status_and_timestamp.time_received

        # Check if timestamp is within the 5-second threshold
        if diff.total_seconds() <= 5:
            new_room_status = room_status_and_timestamp.is_turned_on
        else:
            new_room_status = False

        # Check if the new_room_status is different from the current one
        if new_room_status != status_info.room_status:
            changed_status = map_of_changed_id.get(key)

            # Check if the obj was previously modified from previous check for device status
            if changed_status is None:
                # If it does not yet exist, add
                status_info.room_status = new_room_status
                map_of_changed_id[key] = status_info
            else:
                # If it does exist, update
                changed_status.room_status = new_room_status

    # Update firestore and resolved status
    with resolved_status_lock:
        for id_of_changed in map_of_changed_id:
            map_of_resolved_status[id_of_changed] = map_of_changed_id[id_of_changed]
            set_device_status(firestore_db=firestore_db, device_status_info=map_of_changed_id[id_of_changed])


def eval_hub_status(firestore_db):
    # Get current pulse timestamp with lock
    with hub_pulse_lock:
        last_hub_pulse = map_of_hub_pulse.get(HUB_HEARTBEAT)
    # Evaluate what the status should be
    evaluated_status: bool
    if last_hub_pulse is None:
        evaluated_status = False
    else:
        now = datetime.now(tz=DEVICE_TZ)
        diff = (now - last_hub_pulse).total_seconds()
        if diff > 10:
            evaluated_status = False
        else:
            evaluated_status = True
    # Check current status and update if needed with lock
    with hub_status_lock:
        current_val = map_of_hub_status.get(HUB_HEARTBEAT)
        if current_val is None:
            map_of_hub_status[HUB_HEARTBEAT] = evaluated_status
            set_hub_status(firestore_db=firestore_db, is_online=evaluated_status)
        elif current_val != evaluated_status:
            map_of_hub_status[HUB_HEARTBEAT] = evaluated_status
            set_hub_status(firestore_db=firestore_db, is_online=evaluated_status)


if __name__ == '__main__':
    acc_path = os.getenv("ENV_FILE")
    load_dotenv(acc_path)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    url = os.getenv("MQTT_URL")
    port = os.getenv("MQTT_PORT")
    password = os.getenv("MQTT_PASSWORD")
    username = os.getenv("MQTT_USERNAME")
    mqttc.username_pw_set(username=username, password=password)
    mqttc.connect(host=url, port=int(port), keepalive=60)
    mqttc.loop_start()


    async def main():
        service_acc_path = os.getenv("SERVICE_ACCOUNT_PATH")

        my_scheduler = AsyncIOScheduler()
        my_scheduler.start()

        from src.firestore.device_firestore import init_firestore

        firestore = init_firestore(cred_path=service_acc_path)

        my_scheduler.add_job(eval_pulse, trigger=IntervalTrigger(seconds=2), args=[firestore])
        my_scheduler.add_job(eval_hub_status, trigger=IntervalTrigger(seconds=2), args=[firestore])
        
        def on_temp_schedule_updated(col_snapshot, changes, read_time):
            # Handle different types of changes to temporary_schedules collection
            added_changes = []
            modified_changes = []
            
            for change in changes:
                if change.type.name == 'ADDED':
                    added_changes.append(change)
                elif change.type.name == 'MODIFIED':
                    modified_changes.append(change)
                elif change.type.name == 'REMOVED':
                    print(f"Temporary schedule REMOVED: {change.document.id}")
            
            # Process added/modified changes
            all_changes = added_changes + modified_changes
            if all_changes:
                normalized_schedule = process_changes_from_firestore(all_changes)
                normalized_timeslots = extract_resolved_slots_by_day_v2(normalized_schedule)
                timeslots_in_dict = [asdict(slot) for slot in normalized_timeslots]
                
                mqttc.publish("schedule_temp_update", qos=2, payload=json.dumps(timeslots_in_dict), retain=False)
                print(f"Published temporary schedule update: {len(all_changes)} changes")
        
        def on_schedule_raw_updated(col_snapshot, changes, read_time):
            # Handle different types of changes to schedule_raw collection
            added_docs = []
            modified_docs = []
            removed_docs = []
            
            for change in changes:
                if change.type.name == 'ADDED':
                    added_docs.append(change.document)
                elif change.type.name == 'MODIFIED':
                    modified_docs.append(change.document)
                elif change.type.name == 'REMOVED':
                    removed_docs.append(change.document)
            
            # Process added documents
            if added_docs:
                payloads = process_schedule_raw_with_tracking(added_docs)
                payloads_in_dict = [asdict(payload) for payload in payloads]
                print(f"Published NEW schedule: {payloads_in_dict}")
                for payload in payloads:
                    payload_dict = asdict(payload)
                    mqttc.publish(
                        f"schedule_update/{payload.schedule_id}", 
                        qos=2, 
                        payload=json.dumps(payload_dict), 
                        retain=False
                    )
            if removed_docs:
                for doc in removed_docs:
                    doc_data = doc.to_dict()
                    if doc_data:
                        schedule_id = doc_data.get("schedule_id", doc.id)
                        # Publish removal notification
                        removal_payload = {
                            "schedule_id": schedule_id,
                            "action": "REMOVED",
                            "timestamp_epoch": datetime.now().timestamp()
                        }
                        mqttc.publish("schedule_removed", qos=2, payload=json.dumps(removal_payload), retain=False)
                        print(f"Published REMOVED schedule: {schedule_id}")
                
            
        # firestore.collection("temporary_schedules").on_snapshot(
        #     callback=on_temp_schedule_updated
        # )
        
        firestore.collection("schedule_raw").on_snapshot(
            callback=on_schedule_raw_updated
        )
        
        
        while True:
            await asyncio.sleep(1000)


    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
