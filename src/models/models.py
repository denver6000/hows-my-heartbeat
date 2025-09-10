from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


class HubStatus:
    is_active: bool
    timestamp: datetime | None

    def __init__(self, is_active: bool, timestamp: Optional[datetime]):
        self.is_active = is_active
        self.timestamp = timestamp


class OutletStatusEnum:
    ON = "ON"
    OFF = "OFF"
    INACTIVE = "INACTIVE"


class DeviceStatus:
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


@dataclass
class RoomStatusInfo:
    is_active_false: bool
    room_id: str
    device_id: str


@dataclass
class RoomStatusAndTimestamp:
    device_id: str
    time_received: datetime
    is_turned_on: bool


@dataclass
class DeviceIdAndPulseTimeStamp:
    device_id: str
    room_id: str
    timestamp: datetime


@dataclass
class DeviceStatusInfo:
    device_id: str
    room: str
    is_board_active: bool = False
    room_status: bool = False


# --- Schedule Models ---

@dataclass
class TimeSlot:
    timeslot_id: str
    start_time: str
    end_time: str
    subject: Optional[str] = None
    teacher: Optional[str] = None
    teacher_email: Optional[str] = None
    time_start_in_seconds: int = 0
    start_time_date_epoch: Optional[float] = None  # Convert to epoch seconds
    end_time_date_epoch: Optional[float] = None    # Convert to epoch seconds
    is_temporary: bool = False


@dataclass
class ScheduleOfDay:
    day_name: str
    day_order: int
    hours: List[TimeSlot] = field(default_factory=list)


@dataclass
class ScheduleV2:
    room_id: str
    schedule_days: List[ScheduleOfDay] = field(default_factory=list)


@dataclass
class ResolvedScheduleSlot:
    timeslot_id: str
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
    is_temporary: bool = False


@dataclass 
class ScheduleDocument:
    """Parent structure for schedule_raw documents"""
    schedule_id: str
    upload_date_epoch: float  # Timestamp in epoch seconds
    schedules: List[ScheduleV2] = field(default_factory=list)


@dataclass
class ProcessedSchedulePayload:
    """Final payload structure for MQTT publishing"""
    schedule_id: str
    upload_date_epoch: float
    is_temporary: bool
    resolved_slots: List[ResolvedScheduleSlot] = field(default_factory=list)
