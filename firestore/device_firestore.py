import os
import firebase_admin
from firebase_admin import credentials, firestore

from models.models import DeviceStatusInfo

# Singleton Firestore instance


cred_path = os.getenv("SERVICE_ACCOUNT_PATH")
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred)
_firestore = firestore.client()

# Constants
COLLECTION_NAME = "deviceStatus"


def set_device_status(device_status_info: DeviceStatusInfo):
    doc_ref = _firestore.collection(COLLECTION_NAME).document(f"{device_status_info.room}_{device_status_info.device_id}")
    if doc_ref.get().exists:
        doc_ref.update(device_status_info.__dict__)
    else:
        doc_ref.create(device_status_info.__dict__)
    print(f"[FIRESTORE] Updated device {device_status_info.device_id} status.")


def get_device_status(device_id: str):
    doc = _firestore.collection(COLLECTION_NAME).document(device_id).get()
    if doc.exists:
        return doc.to_dict()
    else:
        return None
