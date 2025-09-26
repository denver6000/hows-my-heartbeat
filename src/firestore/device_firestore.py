from datetime import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import client as firestore_client
from zoneinfo import ZoneInfo
from src.models.models import DeviceStatusInfo, TimeSlot, ScheduleOfDay, ScheduleV2, ResolvedScheduleSlot, ProcessedSchedulePayload, SettingsRequest, RoomStatus, TimeslotStatus
from google.cloud.firestore_v1 import DocumentSnapshot, FieldFilter, DocumentReference
from google.cloud.firestore_v1 import Client as firestore_client
from typing import List

# Define timezone constant locally to avoid circular import
DEVICE_TZ = ZoneInfo("Asia/Manila")
# Singleton Firestore instance


cred_path = os.getenv("SERVICE_ACCOUNT_PATH")
# cred = credentials.Certificate(cred_path)
# firebase_admin.initialize_app(cred)
# _firestore = firestore.client()

# Constants
COLLECTION_NAME = "deviceStatus"


def init_firestore(cred_path: str) -> firestore_client:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    _firestore = firestore.client()
    return _firestore


def set_device_status(firestore_db: firestore_client, device_status_info: DeviceStatusInfo):
    doc_ref = firestore_db.collection(COLLECTION_NAME).document(
        f"{device_status_info.room}_{device_status_info.device_id}")
    if doc_ref.get().exists:
        doc_ref.update(device_status_info.__dict__)
    else:
        doc_ref.create(device_status_info.__dict__)
    print(f"[FIRESTORE] Updated device {device_status_info.device_id} status.")


def set_hub_status(firestore_db: firestore_client, is_online: bool):
    doc_ref = firestore_db.collection(COLLECTION_NAME).document("hub_status")
    doc_ref.set({"isOnline": is_online})
    print(f"[FIRESTORE] Set hub status to {is_online}")


def get_device_status(firestore_db: firestore_client, device_id: str):
    doc = firestore_db.collection(COLLECTION_NAME).document(device_id).get()
    if doc.exists:
        return doc.to_dict()
    else:
        return None

def set_schedule_document_as_received(firestore_db: firestore_client, schedule_id: str, is_received: bool):
    result = firestore_db.collection("schedule_raw").where(filter=FieldFilter("schedule_id", "==", schedule_id)).get()
    for doc in result:
        doc.reference.update({
            "received_by_hub": is_received,
            "received_timestamp": datetime.now(
                tz=DEVICE_TZ
            )
        })
    
def set_schedule_temp_document_as_received(firestore_db: firestore_client, temporary_schedule_id: str, is_received: bool):
    result = firestore_db.collection(
            "temporary_schedules_v2"
        ).where(
            filter=FieldFilter(
                "timeslot_id", 
                "==", 
                temporary_schedule_id
        )).where(
            filter=FieldFilter(
                "is_completed",
                "==",
                False
            )
        ).get()
    for doc in result:
        doc.reference.update({
            "received_by_hub": True,
            "received_timestamp": datetime.now(
                tz=DEVICE_TZ
            )
        })
        
def set_settings_firebase_doc(
    firestore_db: firestore_client, 
    minute_mark_to_warn: int, 
    minute_mark_to_skip: int, 
    bypass_admin_approval: bool,
    request_id: str
):
    ref: DocumentReference = firestore_db.collection("settings").document("system")
    ref.update({
        "minute_mark_to_skip": minute_mark_to_skip,
        "minute_mark_to_warn": minute_mark_to_warn,
        "bypass_admin_approval": bypass_admin_approval,
        "request_in_use": request_id
    })
    
def get_settings_request(firestore_db: firestore_client, request_id: str):
    """
    Retrieve a settings request from Firestore by request_id.
    
    Args:
        firestore_db (firestore_client): Firestore client instance
        request_id (str): Unique request identifier
        
    Returns:
        SettingsRequest | None: Settings request object or None if not found
    """
    result = firestore_db.collection("settings_request").where(
        filter=FieldFilter("request_id", "==", request_id)
    ).get()
    
    if result:
        # Get the first matching document
        doc = result[0]
        doc_data = doc.to_dict()
        if doc_data:
            return SettingsRequest(
                request_id=request_id,
                minute_mark_to_warn=doc_data.get("minute_mark_to_warn", 0),
                minute_mark_to_skip=doc_data.get("minute_mark_to_skip", 0),
                bypass_admin_approval=doc_data.get("bypass_admin_approval", False),
                date_requested=doc_data.get("date_requested"),
                is_received_by_system_hub=doc_data.get("is_received_by_system_hub", False)
            )
    
    return None


def set_settings_request_as_received(firestore_db: firestore_client, request_id: str, is_received: bool):
    """
    Mark a settings request as received by the system hub.
    
    Args:
        firestore_db (firestore_client): Firestore client instance
        request_id (str): Unique request identifier
        is_received (bool): Whether the request has been received by system hub
    """
    doc_ref = firestore_db.collection("settings_request").where(
        filter=FieldFilter(
            field_path="request_id",
            op_string="==",
            value=request_id,
        )
    ).get()
    for doc in doc_ref:
        doc.reference.update({
                "is_received_by_system_hub": is_received,
                "received_timestamp": datetime.now(tz=DEVICE_TZ)
            })
        print(f"[FIRESTORE] Updated settings request {request_id} received status to {is_received}")


def get_room_status(firestore_db: firestore_client, room_id: str):
    """
    Get current room status from Firestore.
    
    Args:
        firestore_db (firestore_client): Firestore client instance
        room_id (str): Room identifier
        
    Returns:
        RoomStatus | None: Current room status or None if not found
    """
    doc = firestore_db.collection("room_status").document(room_id).get()
    if doc.exists:
        doc_data = doc.to_dict()
        if doc_data:
            return RoomStatus(
                room_id=room_id,
                is_turned_on=doc_data.get("is_turned_on", False),
                last_updated=doc_data.get("last_updated", datetime.now(tz=DEVICE_TZ))
            )
    return None


def set_room_status(firestore_db: firestore_client, room_id: str, is_turned_on: bool):
    """
    Update room status in Firestore only if status has changed.
    
    Args:
        firestore_db (firestore_client): Firestore client instance
        room_id (str): Room identifier
        is_turned_on (bool): Room power status
    """
    current_status = get_room_status(firestore_db, room_id)
    
    # Only update if status is different or document doesn't exist
    if current_status is None or current_status.is_turned_on != is_turned_on:
        doc_ref = firestore_db.collection("room_status").document(room_id)
        doc_ref.set({
            "room_id": room_id,
            "is_turned_on": is_turned_on,
            "last_updated": datetime.now(tz=DEVICE_TZ)
        })
        print(f"[FIRESTORE] Updated room {room_id} status to {'ON' if is_turned_on else 'OFF'}")
    # If status is the same, don't update to save read/write operations


def get_timeslot_status(firestore_db: firestore_client, timeslot_id: str):
    """
    Get current timeslot status from Firestore.
    
    Args:
        firestore_db (firestore_client): Firestore client instance
        timeslot_id (str): Timeslot identifier
        
    Returns:
        TimeslotStatus | None: Current timeslot status or None if not found
    """
    doc = firestore_db.collection("timeslots_status").document(timeslot_id).get()
    if doc.exists:
        doc_data = doc.to_dict()
        if doc_data:
            return TimeslotStatus(
                timeslot_id=timeslot_id,
                status=doc_data.get("status", 0),
                last_updated=doc_data.get("last_updated", datetime.now(tz=DEVICE_TZ))
            )
    return None


def set_timeslot_status(firestore_db: firestore_client, timeslot_id: str, status: int):
    """
    Update timeslot status in Firestore only if status has changed.
    
    Args:
        firestore_db (firestore_client): Firestore client instance
        timeslot_id (str): Timeslot identifier
        status (int): Timeslot status (0 or 1)
    """
    current_status = get_timeslot_status(firestore_db, timeslot_id)
    
    # Only update if status is different or document doesn't exist
    if current_status is None or current_status.status != status:
        doc_ref = firestore_db.collection("timeslots_status").document(timeslot_id)
        doc_ref.set({
            "timeslot_id": timeslot_id,
            "status": status,
            "last_updated": datetime.now(tz=DEVICE_TZ)
        })
        print(f"[FIRESTORE] Updated timeslot {timeslot_id} status to {status}")
    # If status is the same, don't update to save read/write operations

# --- Utility Function ---

def extract_resolved_slots_by_day_v2(schedules: List[ScheduleV2]) -> List[ResolvedScheduleSlot]:
    resolved_slot_list: List[ResolvedScheduleSlot] = []
    for sched in schedules:
        for day in sched.schedule_days:
            for slot in day.hours:
                resolved = ResolvedScheduleSlot(
                    timeslot_id=slot.timeslot_id or "",
                    room_id=sched.room_id,
                    day_name=day.day_name,
                    day_order=day.day_order,
                    start_time=slot.start_time,
                    end_time=slot.end_time,
                    subject=slot.subject or "",
                    teacher=slot.teacher or "",
                    teacher_email=slot.teacher_email or "",
                    start_hour=slot.start_hour,
                    start_minute=slot.start_minute,
                    end_hour=slot.end_hour,
                    end_minute=slot.end_minute,
                    start_time_seconds=slot.time_start_in_seconds,
                    start_date_in_seconds_epoch=slot.start_time_date_epoch,
                    end_date_in_seconds_epoch=slot.end_time_date_epoch,
                    is_temporary=getattr(slot, 'is_temporary', False)
                )
                resolved_slot_list.append(resolved)
    return resolved_slot_list


# --- New Consolidated Function ---
def process_schedule_raw_with_tracking(docs: List[DocumentSnapshot]) -> List[ProcessedSchedulePayload]:
    """
    Processes schedule_raw documents and returns complete payloads with tracking information.
    Each payload includes schedule_id, upload_date_epoch, and all resolved slots for that schedule.
    """
    payloads = []
    
    for doc in docs:
        doc_data = doc.to_dict()
        if doc_data is None:
            continue
            
        # Extract metadata
        schedule_id = doc_data.get("schedule_id", doc.id)  # Use document ID as fallback
        upload_date = doc_data.get("upload_date")
        is_temporary = doc_data.get("is_temporary", False)
        upload_date_epoch = upload_date.timestamp() if upload_date else datetime.now().timestamp()
        
        # Process schedules
        schedules_array = doc_data.get("schedules", [])
        all_schedules = []
        
        for schedule in schedules_array:
            room_id = schedule.get("roomId")
            schedule_map = schedule.get("scheduleOfDayMap", {})
            schedule_days = []
            
            if isinstance(schedule_map, list):
                for day in schedule_map:
                    day_name = day.get("dayName")
                    day_order = day.get("dayOrder", 0)
                    raw_hours = day.get("hours", [])
                    
                    time_slots = [
                        TimeSlot(
                            timeslot_id=slot.get("timeslotId"),
                            start_time=slot.get("startTime"),
                            end_time=slot.get("endTime"),
                            subject=slot.get("subject"),
                            teacher=slot.get("teacher"),
                            start_hour=slot.get("startHour"),
                            start_minute=slot.get("startMinute"),
                            end_hour=slot.get("endHour"),
                            end_minute=slot.get("endMinute"),
                            teacher_email=slot.get("teacherEmail"),

                            time_start_in_seconds=int(slot.get("timeStartInSeconds", 0))
                        )
                        for slot in raw_hours
                    ]
                    
                    time_slots.sort(key=lambda t: t.time_start_in_seconds)
                    
                    schedule_days.append(ScheduleOfDay(
                        day_name=day_name,
                        day_order=day_order,
                        hours=time_slots
                    ))
            
            schedule_days.sort(key=lambda d: d.day_order)
            
            all_schedules.append(ScheduleV2(
                room_id=room_id,
                schedule_days=schedule_days
            ))
        
        # Extract resolved slots
        resolved_slots = extract_resolved_slots_by_day_v2(all_schedules)
        
        # Create payload
        payload = ProcessedSchedulePayload(
            schedule_id=schedule_id,
            upload_date_epoch=upload_date_epoch,
            resolved_slots=resolved_slots,
            is_temporary=is_temporary
        )
        
        payloads.append(payload)
    
    return payloads