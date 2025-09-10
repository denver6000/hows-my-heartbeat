from datetime import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import client as firestore_client
from src.models.models import DeviceStatusInfo, TimeSlot, ScheduleOfDay, ScheduleV2, ResolvedScheduleSlot, ScheduleDocument, ProcessedSchedulePayload
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import DocumentChange
from google.cloud.firestore_v1 import Client as firestore_client
from dataclasses import dataclass, field
from typing import Dict, List, Optional
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

# --- Utility Function ---
def process_changes_from_firestore(docs: List[DocumentChange]) -> List[ScheduleV2]:
    all_schedules = []

    for doc in docs:
        doc_data = doc.document.to_dict()
        room_id = doc_data.get("roomId")
        schedule_map = doc_data.get("scheduleOfDayMap", {})
        schedule_days = []

        for day_key, day in schedule_map.items():
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
                    teacher_email=slot.get("teacherEmail"),
                    time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                    start_time_date_epoch=slot.get("startDateTime").timestamp() if slot.get("startDateTime") else None,
                    end_time_date_epoch=slot.get("endDateTime").timestamp() if slot.get("endDateTime") else None,
                )
                for slot in raw_hours
            ]

            # Sort time slots within the day
            time_slots.sort(key=lambda t: t.time_start_in_seconds)

            schedule_days.append(ScheduleOfDay(
                day_name=day_name,
                day_order=day_order,
                hours=time_slots
            ))

        # Sort days by day_order
        schedule_days.sort(key=lambda d: d.day_order)

        all_schedules.append(ScheduleV2(
            room_id=room_id,
            schedule_days=schedule_days
        ))

    return all_schedules

# --- New Utility Function for schedule_raw ---
def process_schedule_raw_from_firestore(docs: List[DocumentSnapshot]) -> List[ScheduleV2]:
    """
    Processes the new schedule_raw collection structure.
    Each document contains a 'schedules' array, where each item is a schedule for a room.
    """
    all_schedules = []
    for doc in docs:
        doc_data = doc.to_dict()
        if doc_data is None:
            continue
        schedules_array = doc_data.get("schedules", [])
        # Optionally: schedule_id = doc_data.get("schedule_id")
        # Optionally: upload_date = doc_data.get("upload_date")
        
        for schedule in schedules_array:
            room_id = schedule.get("roomId")
            schedule_map = schedule.get("scheduleOfDayMap", {})
            schedule_days = []
            
            # Handle both dict and list formats for scheduleOfDayMap
            if isinstance(schedule_map, list):
                # If it's a list, iterate directly
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
                            teacher_email=slot.get("teacherEmail"),
                            time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                            start_time_date_epoch=slot.get("startDateTime").timestamp() if slot.get("startDateTime") else None,
                            end_time_date_epoch=slot.get("endDateTime").timestamp() if slot.get("endDateTime") else None,
                        )
                        for slot in raw_hours
                    ]
                    
                    time_slots.sort(key=lambda t: t.time_start_in_seconds)
                    
                    schedule_days.append(ScheduleOfDay(
                        day_name=day_name,
                        day_order=day_order,
                        hours=time_slots
                    ))
            else:
                # If it's a dict, iterate over items (original logic)
                for day_key, day in schedule_map.items():
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
                            teacher_email=slot.get("teacherEmail"),
                            time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                            start_time_date_epoch=slot.get("startDateTime").timestamp() if slot.get("startDateTime") else None,
                            end_time_date_epoch=slot.get("endDateTime").timestamp() if slot.get("endDateTime") else None,
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
    
    return all_schedules

# --- Utility Function ---
def extract_resolved_slots_by_day(schedules: List[ScheduleV2]) -> Dict[str, List[ResolvedScheduleSlot]]:
    resolved_slots_by_day: Dict[str, List[ResolvedScheduleSlot]] = {}
    for sched in schedules:
        for day in sched.schedule_days:
            resolved_list = resolved_slots_by_day.setdefault(day.day_name, [])
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
                    time_start_in_seconds=slot.time_start_in_seconds,
                    start_date_in_seconds_epoch=slot.start_time_date_epoch,
                    end_date_in_seconds_epoch=slot.end_time_date_epoch,
                    is_temporary=getattr(slot, 'is_temporary', False)
                )
                resolved_list.append(resolved)

    # Optional: sort time slots for each day by time
    for day, slots in resolved_slots_by_day.items():
        slots.sort(key=lambda x: x.time_start_in_seconds)

    return resolved_slots_by_day

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
                    time_start_in_seconds=slot.time_start_in_seconds,
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
            
            # Handle both dict and list formats for scheduleOfDayMap
            if isinstance(schedule_map, list):
                # If it's a list, iterate directly
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
                            teacher_email=slot.get("teacherEmail"),
                            time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                            start_time_date_epoch=slot.get("startDateTime").timestamp() if slot.get("startDateTime") else None,
                            end_time_date_epoch=slot.get("endDateTime").timestamp() if slot.get("endDateTime") else None,
                        )
                        for slot in raw_hours
                    ]
                    
                    time_slots.sort(key=lambda t: t.time_start_in_seconds)
                    
                    schedule_days.append(ScheduleOfDay(
                        day_name=day_name,
                        day_order=day_order,
                        hours=time_slots
                    ))
            else:
                # If it's a dict, iterate over items (original logic)
                for day_key, day in schedule_map.items():
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
                            teacher_email=slot.get("teacherEmail"),
                            time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                            start_time_date_epoch=slot.get("startDateTime").timestamp() if slot.get("startDateTime") else None,
                            end_time_date_epoch=slot.get("endDateTime").timestamp() if slot.get("endDateTime") else None,
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