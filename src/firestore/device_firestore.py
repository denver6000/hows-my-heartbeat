from datetime import datetime
import os
import firebase_admin
from firebase_admin import credentials as admin_creds, firestore
from firebase_admin.firestore import client as firestore_client
from src.models.models import DeviceStatusInfo, SettingsRequest
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import DocumentChange
from google.cloud.firestore_v1 import Client as firestore_client
# Singleton Firestore instance
from google.cloud import firestore
from google.auth import credentials

cred_path = os.getenv("SERVICE_ACCOUNT_PATH")
# cred = credentials.Certificate(cred_path)
# firebase_admin.initialize_app(cred)
# _firestore = firestore.client()

# Constants
COLLECTION_NAME = "deviceStatus"


def init_firestore(cred_path: str | None = None, use_emulator: bool = False) -> firestore_client:
    if use_emulator:
        project_id = os.environ["GCLOUD_PROJECT"]

        # Create a no-op credential that doesn't need a PEM or ADC
        # class NoCred(credentials.Base):
        #     def get_credential(self):
        #         return AnonymousCredentials()

        # cred = NoCred()
        firebase_admin.initialize_app(credential=credentials.AnonymousCredentials(), options= {"projectId": project_id})
        print("Emulator mode")
    else:
        # Production: use real service account
        print("Prod Modes")
        if not cred_path:
            raise ValueError("cred_path is required for production mode")
        cred = admin_creds.Certificate(cred_path)
        try:
            firebase_admin.initialize_app(cred)
        except ValueError:
            pass  # already initialized

    # Return the Firebase Admin Firestore client (works for both emulator and production)
    return firestore_client()


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

# --- Dataclasses ---
from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class TimeSlot:
    start_time: str
    end_time: str
    subject: Optional[str] = None
    teacher: Optional[str] = None
    teacher_email: Optional[str] = None
    time_start_in_seconds: int = 0
    start_time_date: Optional[datetime] = None
    end_time_date: Optional[datetime] = None

@dataclass
class ScheduleOfDay:
    day_name: str
    day_order: int
    hours: List[TimeSlot] = field(default_factory=list)

@dataclass
class ScheduleV2:
    room_id: str
    schedule_days: List[ScheduleOfDay] = field(default_factory=list)

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
                    start_time=slot.get("startTime"),
                    end_time=slot.get("endTime"),
                    subject=slot.get("subject"),
                    teacher=slot.get("teacher"),
                    teacher_email=slot.get("teacherEmail"),
                    time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                    end_time_date=slot.get("endDateTime"),
                    start_time_date=slot.get("startDateTime"),
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

# --- Dataclass ---
@dataclass
class ResolvedScheduleSlot:
    room_id: str
    day_name: str
    day_order: int
    start_time: str
    end_time: str
    subject: str
    teacher: str
    teacher_email: str
    time_start_in_seconds: int
    start_date_in_seconds_epoch: float | None
    end_date_in_seconds_epoch: float | None

# --- Utility Function ---
def extract_resolved_slots_by_day(schedules: List[ScheduleV2]) -> Dict[str, List[ResolvedScheduleSlot]]:
    resolved_slots_by_day: Dict[str, List[ResolvedScheduleSlot]] = {}
    for sched in schedules:
        for day in sched.schedule_days:
            resolved_list = resolved_slots_by_day.setdefault(day.day_name, [])
            for slot in day.hours:
                
                start_time_date = None
                end_time_date = None
                if slot.start_time_date is not None:
                    start_time_date = slot.start_time_date.timestamp()
                if slot.end_time_date is not None:
                    end_time_date = slot.end_time_date.timestamp()
                    
                    
                resolved = ResolvedScheduleSlot(
                    room_id=sched.room_id,
                    day_name=day.day_name,
                    day_order=day.day_order,
                    start_time=slot.start_time,
                    end_time=slot.end_time,
                    subject=slot.subject or "",
                    teacher=slot.teacher or "",
                    teacher_email=slot.teacher_email or "",
                    time_start_in_seconds=slot.time_start_in_seconds,
                    start_date_in_seconds_epoch=start_time_date,
                    end_date_in_seconds_epoch=end_time_date
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
                start_time_date = None
                end_time_date = None
                if slot.start_time_date is not None:
                    start_time_date = slot.start_time_date.timestamp()
                if slot.end_time_date is not None:
                    end_time_date = slot.end_time_date.timestamp()
                    
                resolved = ResolvedScheduleSlot(
                    room_id=sched.room_id,
                    day_name=day.day_name,
                    day_order=day.day_order,
                    start_time=slot.start_time,
                    end_time=slot.end_time,
                    subject=slot.subject or "",
                    teacher=slot.teacher or "",
                    teacher_email=slot.teacher_email or "",
                    time_start_in_seconds=slot.time_start_in_seconds,
                    start_date_in_seconds_epoch=start_time_date,
                    end_date_in_seconds_epoch=end_time_date
                )
                resolved_slot_list.append(resolved)
    return resolved_slot_list


# --- Additional Firestore Functions ---

def set_schedule_document_as_received(firestore_db: firestore_client, is_received: bool, schedule_id: str):
    """Mark a schedule document as received by the hub"""
    doc_ref = firestore_db.collection('schedule_raw').document(schedule_id)
    doc_ref.update({'received_by_hub': is_received})
    print(f"[FIRESTORE] Set schedule {schedule_id} received status to {is_received}")


def set_schedule_temp_document_as_received(firestore_db: firestore_client, is_received: bool, temporary_schedule_id: str):
    """Mark a temporary schedule document as received by the hub"""
    doc_ref = firestore_db.collection('temporary_schedules_v2').document(temporary_schedule_id)
    doc_ref.update({'received_by_hub': is_received})
    print(f"[FIRESTORE] Set temporary schedule {temporary_schedule_id} received status to {is_received}")


def set_settings_firebase_doc(firestore_db: firestore_client, minute_mark_to_warn: int, 
                             minute_mark_to_skip: int, bypass_admin_approval: bool, request_id: str):
    """Update system settings from a settings request"""
    settings_ref = firestore_db.collection('settings').document('system_settings')
    settings_data = {
        'minute_mark_to_warn': minute_mark_to_warn,
        'minute_mark_to_skip': minute_mark_to_skip,
        'bypass_admin_approval': bypass_admin_approval,
        'last_updated': datetime.now(),
        'last_request_id': request_id
    }
    settings_ref.set(settings_data, merge=True)
    print(f"[FIRESTORE] Updated system settings from request {request_id}")


def get_settings_request(firestore_db: firestore_client, request_id: str):
    """Get a settings request by ID"""
    doc_ref = firestore_db.collection('settings_requests').document(request_id)
    doc = doc_ref.get()
    if doc.exists:
        data = doc.to_dict()
        return SettingsRequest(
            request_id=request_id,
            minute_mark_to_warn=data.get('minute_mark_to_warn', 0),
            minute_mark_to_skip=data.get('minute_mark_to_skip', 0),
            bypass_admin_approval=data.get('bypass_admin_approval', False),
            date_requested=data.get('date_requested'),
            is_received_by_system_hub=data.get('is_received_by_system_hub', False)
        )
    return None


def set_settings_request_as_received(firestore_db: firestore_client, request_id: str, is_received: bool):
    """Mark a settings request as received by the hub"""
    doc_ref = firestore_db.collection('settings_requests').document(request_id)
    doc_ref.update({'is_received': is_received})
    print(f"[FIRESTORE] Set settings request {request_id} received status to {is_received}")


def set_room_status(firestore_db: firestore_client, room_id: str, is_turned_on: bool):
    """Update room status in Firestore (only if changed)"""
    doc_ref = firestore_db.collection('rooms').document(room_id)
    doc = doc_ref.get()
    
    # Check if the status has actually changed
    if doc.exists:
        current_data = doc.to_dict()
        if current_data.get('is_turned_on') == is_turned_on:
            return  # No change, skip update
    
    # Update with new status and timestamp
    room_data = {
        'room_id': room_id,
        'is_turned_on': is_turned_on,
        'last_updated': datetime.now()
    }
    doc_ref.set(room_data, merge=True)
    print(f"[FIRESTORE] Updated room {room_id} status to {'ON' if is_turned_on else 'OFF'}")


def set_timeslot_status(firestore_db: firestore_client, timeslot_id: str, status: int):
    """Update timeslot status in Firestore (only if changed)"""
    doc_ref = firestore_db.collection('timeslots_status').document(timeslot_id)
    doc = doc_ref.get()
    
    # Check if the status has actually changed
    if doc.exists:
        current_data = doc.to_dict()
        if current_data.get('status') == status:
            return  # No change, skip update
    
    # Update with new status and timestamp
    timeslot_data = {
        'timeslot_id': timeslot_id,
        'status': status,
        'last_updated': datetime.now()
    }
    doc_ref.set(timeslot_data, merge=True)
    print(f"[FIRESTORE] Updated timeslot {timeslot_id} status to {status}")


def set_cancellation_as_received(firestore_db: firestore_client, cancellation_id: str, is_received: bool):
    """Mark a class cancellation as received by the hub"""
    doc_ref = firestore_db.collection('classCancellationsRequest').document(cancellation_id)
    doc_ref.update({
        'received_by_hub': is_received,
        'received_timestamp': datetime.now()
    })
    print(f"[FIRESTORE] Set cancellation {cancellation_id} received status to {is_received}")