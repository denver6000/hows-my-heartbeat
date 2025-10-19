# pylint: disable=too-many-lines,too-many-locals,broad-except,global-statement
import asyncio
import json
import os
import threading
from datetime import datetime
from typing import Dict
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from src.firestore.device_firestore import  init_firestore, set_device_status, set_hub_status, set_schedule_document_as_received, set_schedule_temp_document_as_received, set_settings_firebase_doc, get_settings_request, set_settings_request_as_received, set_room_status, set_timeslot_status, set_cancellation_as_received
from src.models.models import DeviceStatusInfo, RoomStatusAndTimestamp, DeviceIdAndPulseTimeStamp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

DEVICE_TZ = ZoneInfo("Asia/Manila")
NODE_HEART_PULSE_BASE = "heart_pulse"
ROOM_STATUS_BASE = "room_status"
HUB_CONNECTION_STATE = "broker/connection/#"
TIMESLOTS_BASE = "timeslots"
SCHEDULE_UPDATE_ACK = "schedule_update_ack"
SCHEDULE_TEMP_UPDATE = "schedule_temp_update"
SCHEDULE_TEMP_ACK = "schedule_temp_update_ack"
SCHEDULE_UPDATE = "schedule_update"
SCHEDULE_PROCESS = "schedule_update_process"
SETTINGS_UPDATE_ACK = "settings_update_ack"
CANCEL_SCHEDULE_ACK = "cancel_schedule_ack"

# Thread locks for thread-safe access
pulse_lock = threading.Lock()
room_status_lock = threading.Lock()
hub_status_lock = threading.Lock()
resolved_status_lock = threading.Lock()

map_of_pulse: Dict[str, DeviceIdAndPulseTimeStamp] = {}
map_of_room_status: Dict[str, RoomStatusAndTimestamp] = {}
map_of_resolved_status: Dict[str, DeviceStatusInfo] = {
    "RM301/SOCKET-1": DeviceStatusInfo(device_id="SOCKET-1", room="RM301")
}
hub_is_online: bool = False

def normalize_class_cancellation_data(doc_data):
    """Convert Firestore document data to the expected field structure for MQTT publishing"""
    return {
        "day": doc_data.get("day"),
        "day_of_month": doc_data.get("day_of_month"),
        "id": doc_data.get("id"),
        "cancellationId": doc_data.get("cancellation_id"),
        "month": doc_data.get("month"),
        "reason": doc_data.get("reason"),
        "roomId": doc_data.get("room_id"),
        "teacherEmail": doc_data.get("teacher_email"),
        "teacherId": doc_data.get("teacher_id"),
        "teacherName": doc_data.get("teacher_name"),
        "timeSlot": doc_data.get("time_slot"),
        "timeslotId": doc_data.get("timeslot_id"),
        "year": doc_data.get("year"),
        "accepted": doc_data.get("accepted"),
        "is_temporary": doc_data.get("is_temporary")
    }

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    topics = (
        f"{NODE_HEART_PULSE_BASE}/#",
        f"{ROOM_STATUS_BASE}/#",
        f"{TIMESLOTS_BASE}/#",
        f"{SCHEDULE_UPDATE_ACK}/#",
        f"{SCHEDULE_TEMP_ACK}/#",
        f"{SETTINGS_UPDATE_ACK}/#",
        f"{CANCEL_SCHEDULE_ACK}/#",
        "$SYS/broker/connection/remote_id/#"
    )
    for topic in topics:
        result = client.subscribe(topic)
        print(f"Subscribed to topic: {topic}, result: {result}")


def eval_pulse(firestore_db):
    now = datetime.now(tz=DEVICE_TZ)

    # Create thread-safe copies of the data
    with pulse_lock:
        pulse_copy = map_of_pulse.copy()
    with room_status_lock:
        _ = map_of_room_status.copy()  # Not currently used but kept for future use
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

    # Room status is now handled directly via MQTT in real-time
    # No need to track room status with timestamps here

    # Update firestore and resolved status
    with resolved_status_lock:
        for id_of_changed in map_of_changed_id:
            map_of_resolved_status[id_of_changed] = map_of_changed_id[id_of_changed]
            set_device_status(firestore_db=firestore_db, device_status_info=map_of_changed_id[id_of_changed])


def update_hub_status(firestore_db, is_online: bool):
    """Update hub status only if it has changed"""
    global hub_is_online
    with hub_status_lock:
        if hub_is_online != is_online:
            hub_is_online = is_online
            set_hub_status(firestore_db=firestore_db, is_online=is_online)
            print(f"Hub status changed to: {'ONLINE' if is_online else 'OFFLINE'}")




if __name__ == '__main__':
    acc_path = os.getenv("ENV_FILE")
    load_dotenv(acc_path)
    mqttc = mqtt.Client(transport="websockets")
    mqttc.on_connect = on_connect
    
    # Check if we should use Firestore emulator
    use_emulator = os.getenv("FIRESTORE_EMULATOR_HOST") is not None
    service_acc_path = os.getenv("SERVICE_ACCOUNT_PATH")
    
    if not use_emulator and not service_acc_path:
        raise ValueError("SERVICE_ACCOUNT_PATH environment variable is required for production mode")

    firestore = init_firestore(cred_path=service_acc_path, use_emulator=use_emulator)
    
    def on_message(client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()
        now = datetime.now(tz=DEVICE_TZ)
        
        # Debug: Log all incoming messages
        print(f"[DEBUG] Received message - Topic: {topic}, Payload: {payload}")
        
        if NODE_HEART_PULSE_BASE in topic:
            try:
                _, room_id, device_id = topic.split("/")
                with pulse_lock:
                    map_of_pulse[room_id + "/" + device_id] = DeviceIdAndPulseTimeStamp(
                        device_id=device_id,
                        room_id=room_id,
                        timestamp=now)
            except ValueError:
                print("Malformed topic.")
        elif ROOM_STATUS_BASE in topic:
            try:
                _, room_id = topic.split("/")
                is_turned_on = msg.payload.decode() == "1"
                # Update room status in Firestore (only if changed)
                set_room_status(firestore_db=firestore, room_id=room_id, is_turned_on=is_turned_on)
            except ValueError:
                print("Malformed room status topic.")
        elif "broker/connection/" in topic and topic.endswith("/state"):
            try:
                # Extract remote_id from broker/connection/remote_id/state
                connection_state = msg.payload.decode()
                is_online = connection_state == "1" or connection_state.lower() == "online"
                update_hub_status(firestore_db=firestore, is_online=is_online)
                print(f"Hub connection state: {connection_state}") 
            except Exception as e:
                print(f"Error processing hub connection state: {e}")
        elif TIMESLOTS_BASE in topic:
            print(f"[DEBUG] Processing timeslots topic: {topic}")
            try:
                # Extract timeslot_id from timeslots/timeslot_id/status
                parts = topic.split("/")
                print(f"[DEBUG] Topic parts: {parts}")
                
                if len(parts) >= 3 and parts[0] == "timeslots":
                    if parts[2] == "status":
                        timeslot_id = parts[1]
                        status = int(payload)
                        print(f"[DEBUG] Parsed - Timeslot ID: {timeslot_id}, Status: {status}")
                        # Update timeslot status in Firestore (only if changed)
                        set_timeslot_status(firestore_db=firestore, timeslot_id=timeslot_id, status=status)
                        print(f"✅ Timeslot {timeslot_id} status: {status}")
                    else:
                        print(f"[DEBUG] Ignoring timeslot topic (not /status): {topic}")
                else:
                    print(f"[DEBUG] Invalid timeslot topic format: {topic}")
            except (ValueError, IndexError) as e:
                print(f"❌ Malformed timeslot topic or invalid payload: {e}")
                print(f"    Topic: {topic}, Payload: {payload}")
            except Exception as e:
                print(f"❌ Error processing timeslot status: {e}")
                print(f"    Topic: {topic}, Payload: {payload}")
        elif SCHEDULE_UPDATE_ACK in topic:
            try:
                _, schedule_id = topic.split("/")
                print(f"Received schedule update ACK for schedule ID: {schedule_id} with payload: {json.loads(msg.payload.decode())}")
                set_schedule_document_as_received(
                    firestore_db=firestore,
                    is_received=True,
                    schedule_id=schedule_id
                )
                # Clear the retained message for the corresponding schedule_update topic
                mqttc.publish(f"{SCHEDULE_UPDATE}/{schedule_id}", payload=None, qos=2, retain=True)
                print(f"🧹 Cleared retained message for topic: {SCHEDULE_UPDATE}/{schedule_id}")
            except ValueError:
                print("Malformed topic for schedule update ACK.")
        elif SCHEDULE_TEMP_ACK in topic:
            try:
                _, temp_schedule_id = topic.split("/")
                print(f"Received temporary schedule ACK for ID: {temp_schedule_id}")
                set_schedule_temp_document_as_received(
                    firestore_db=firestore,
                    is_received=True,
                    temporary_schedule_id=temp_schedule_id
                )
                # Clear the retained message for the corresponding schedule_temp_update topic
                mqttc.publish(f"{SCHEDULE_TEMP_UPDATE}/{temp_schedule_id}", payload=None, qos=2, retain=True)
                print(f"🧹 Cleared retained message for topic: {SCHEDULE_TEMP_UPDATE}/{temp_schedule_id}")
            except ValueError:
                print("Malformed topic for temporary schedule ACK.")
        elif SETTINGS_UPDATE_ACK in topic:
            try: 
                _, request_id = topic.split("/")
                print(f"Received settings update ACK for ID: {request_id}")
                
                # Get the settings request from Firestore
                settings_request = get_settings_request(firestore_db=firestore, request_id=request_id)
                
                if settings_request:
                    # Apply the settings from the request to the system settings
                    set_settings_firebase_doc(
                        firestore_db=firestore,
                        minute_mark_to_warn=settings_request.minute_mark_to_warn,
                        minute_mark_to_skip=settings_request.minute_mark_to_skip,
                        bypass_admin_approval=settings_request.bypass_admin_approval,
                        request_id=request_id
                    )
                    
                    # Mark the settings request as received by system hub
                    set_settings_request_as_received(
                        firestore_db=firestore,
                        request_id=request_id,
                        is_received=True
                    )
                    
                    print(f"Applied settings from request {request_id} to system settings and marked as received")
                else:
                    print(f"Settings request {request_id} not found in Firestore")
                    
            except ValueError:
                print("Malformed topic for settings update ACK.")
            except Exception as e:
                print(f"Error processing settings update ACK: {e}")
                print("Malformed topic for settings update ACK.")
        elif CANCEL_SCHEDULE_ACK in topic:
            try:
                _, cancellation_id = topic.split("/")
                print(f"Received cancellation ACK for ID: {cancellation_id}")
                set_cancellation_as_received(
                    firestore_db=firestore,
                    cancellation_id=cancellation_id,
                    is_received=True
                )
                # Clear the retained message for the corresponding cancel_schedule topic
                mqttc.publish(f"cancel_schedule/{cancellation_id}", payload=None, qos=2, retain=True)
                print(f"🧹 Cleared retained message for topic: cancel_schedule/{cancellation_id}")
                print(f"✅ Marked cancellation {cancellation_id} as received by hub")
            except ValueError:
                print("Malformed topic for cancellation ACK.")
            except Exception as e:
                print(f"❌ Error processing cancellation ACK: {e}")
        
    mqttc.on_message = on_message
    url = os.getenv("MQTT_URL")
    port_str = os.getenv("MQTT_PORT")
    password = os.getenv("MQTT_PASSWORD")
    username = os.getenv("MQTT_USERNAME")
    
    if not all([url, port_str, username, password]):
        raise ValueError("MQTT configuration environment variables are required")
    
    # Type assertions after validation
    assert url is not None
    assert port_str is not None
    assert username is not None
    assert password is not None
    
    mqttc.username_pw_set(username=username, password=password)
    mqttc.connect(host=url, port=int(port_str), keepalive=60)
    mqttc.loop_start()


    async def main():

        my_scheduler = AsyncIOScheduler()
        my_scheduler.start()

        # my_scheduler.add_job(eval_pulse, trigger=IntervalTrigger(seconds=2), args=[firestore])
        
        # Set up Firestore listener for entire temporary_schedules_v2 collection
        temp_collection_ref = firestore.collection('temporary_schedules_v2')
        
        def on_temporary_schedule_update(doc_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'MODIFIED':
                    doc_data = change.document.to_dict()
                    if doc_data and doc_data.get('is_approved') is True and doc_data.get("received_by_hub") is False:
                        # Serialize document data to JSON and publish on MQTT
                        try:
                            json_payload = json.dumps(doc_data, default=str)  # default=str handles datetime objects
                            result = mqttc.publish(
                                f"{SCHEDULE_TEMP_UPDATE}/{doc_data.get('timeslot_id')}", 
                                json_payload, 
                                qos=2,
                                retain=True)
                            print(f"📤 Published approved temporary schedule to MQTT topic '{SCHEDULE_TEMP_UPDATE}'")
                            print(f"    Document ID: {change.document.id}")
                            print(f"    Publish result: {result}")
                        except Exception as e:
                            print(f"❌ Error publishing temporary schedule to MQTT: {e}")

        # Set up Firestore listener for entire schedule_raw collection
        schedule_collection_ref = firestore.collection('schedule_raw')
        
        def on_schedule_raw_update(doc_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'MODIFIED':
                    doc_data = change.document.to_dict()
                    if doc_data and doc_data.get("in_use") is True and doc_data.get("received_by_hub") is False:
                        # Wrap the data in ScheduleWrapper structure like Kotlin code
                        try:
                            import time
                            
                            schedule_wrapper = {
                                "scheduleId": doc_data.get("schedule_id"), 
                                "schedules": doc_data.get("schedules", []), 
                                "uploadDate": int(time.time()),  # Current time in epoch seconds
                                "isTemporary": doc_data.get("is_temporary", False),  
                                "receivedByHub": False,  
                                "receivedTimestamp": None  
                            }
                            
                            json_payload = json.dumps(schedule_wrapper, default=str)
                            result = mqttc.publish(
                                f"{SCHEDULE_UPDATE}/{schedule_wrapper['scheduleId']}", 
                                json_payload, 
                                qos=2,
                                retain=True
                            )
                        except Exception as e:
                            print(f"❌ Error publishing schedule to MQTT: {e}")

        class_cancellations_ref = firestore.collection('classCancellationsRequest')
    
        def on_class_cancellation_update(doc_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'ADDED':
                    
                    doc_data = change.document.to_dict()
                    if doc_data:
                        # Normalize data early for consistent property access
                        normalized_data = normalize_class_cancellation_data(doc_data)
                        timeslot_id = normalized_data.get('timeslotId')
                        cancellation_id = normalized_data.get('cancellationId')
                        
                        if normalized_data.get('is_temporary') is True:
                            try:
                                # Check if accepted is null and is_temporary is true
                                if normalized_data.get('accepted') is None:
                                    # Update the document to set accepted = true
                                    change.document.reference.update({'accepted': True})
                                    print(f"✅ Auto-accepted temporary class cancellation: {change.document.id}")
                                    
                                    # Update normalized data for MQTT publishing
                                    normalized_data['accepted'] = True
                                    
                                    # Publish using normalized data
                                    json_payload = json.dumps(doc_data, default=str)
                                    result = mqttc.publish(
                                        f"cancel_schedule/{cancellation_id}", 
                                        json_payload, 
                                        retain=True,
                                        qos=2)
                                if timeslot_id:
                                    # Publish using normalized data
                                    json_payload = json.dumps(doc_data, default=str)
                                    result = mqttc.publish(
                                        topic=f"cancel_schedule/{cancellation_id}", 
                                        payload=json_payload, 
                                        qos=2,
                                        retain=True
                                    )
                                    print(f"📤 Published temporary class cancellation to MQTT topic 'cancel_schedule/{timeslot_id}'")
                                    print(f"    Document ID: {change.document.id}")
                                    print(f"    Publish result: {result}")
                                else:
                                    print(f"❌ No timeslot_id found in temporary class cancellation document {change.document.id}")
                            except Exception as e:
                                print(f"❌ Error publishing temporary class cancellation to MQTT: {e}")
                
                elif change.type.name == 'MODIFIED':
                    doc_data = change.document.to_dict()
                    if doc_data:
                        # Normalize data early for consistent property access
                        normalized_data = normalize_class_cancellation_data(doc_data)
                        
                        if normalized_data.get('is_temporary') is False and normalized_data.get('accepted') is True:
                            try:
                                timeslot_id = normalized_data.get('timeslotId')
                                if timeslot_id:
                                    # Publish using normalized data
                                    json_payload = json.dumps(doc_data, default=str)
                                    result = mqttc.publish(
                                        f"cancel_schedule/{normalized_data.get('cancellationId')}", 
                                        json_payload, 
                                        qos=2,
                                        retain=True)
                                    print(f"📤 Published regular class cancellation to MQTT topic 'cancel_schedule/{timeslot_id}'")
                                    print(f"    Document ID: {change.document.id}")
                                    print(f"    Publish result: {result}")
                                else:
                                    print(f"❌ No timeslot_id found in regular class cancellation document {change.document.id}")
                            except Exception as e:
                                print(f"❌ Error publishing regular class cancellation to MQTT: {e}")
        
        temp_collection_watch = temp_collection_ref.on_snapshot(on_temporary_schedule_update)
        schedule_collection_watch = schedule_collection_ref.on_snapshot(on_schedule_raw_update)
        class_cancellation_watch = class_cancellations_ref.on_snapshot(on_class_cancellation_update)
        
        try:
            while True:
                await asyncio.sleep(1000)
        finally:
            # Clean up the listeners when the application shuts down
            temp_collection_watch.unsubscribe()
            schedule_collection_watch.unsubscribe()
            class_cancellation_watch.unsubscribe()
            print("🔌 Firestore collection listeners unsubscribed")


    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
