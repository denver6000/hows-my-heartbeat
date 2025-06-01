from dataclasses import dataclass, field
from datetime import datetime


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
