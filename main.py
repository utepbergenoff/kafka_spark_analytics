"""Synthetic research-job events from Assignment 4."""

EVENTS = [
    {"event_id": "E001", "timestamp": "14:00:01", "research_group": "G01", "job_type": "ml", "duration_sec": 42, "status": "completed", "cpu_pct": 61},
    {"event_id": "E002", "timestamp": "14:00:02", "research_group": "G02", "job_type": "image", "duration_sec": 75, "status": "completed", "cpu_pct": 74},
    {"event_id": "E003", "timestamp": "14:00:03", "research_group": "G01", "job_type": "text", "duration_sec": 18, "status": "completed", "cpu_pct": 38},
    {"event_id": "E004", "timestamp": "14:00:04", "research_group": "G03", "job_type": "simulation", "duration_sec": 95, "status": "failed", "cpu_pct": 92},
    {"event_id": "E005", "timestamp": "14:00:05", "research_group": "G02", "job_type": "ml", "duration_sec": 51, "status": "completed", "cpu_pct": 68},
    {"event_id": "E006", "timestamp": "14:00:06", "research_group": "G01", "job_type": "image", "duration_sec": 88, "status": "completed", "cpu_pct": 81},
    {"event_id": "E007", "timestamp": "14:00:07", "research_group": "G03", "job_type": "ml", "duration_sec": 64, "status": "failed", "cpu_pct": 89},
    {"event_id": "E008", "timestamp": "14:00:08", "research_group": "G02", "job_type": "text", "duration_sec": 22, "status": "completed", "cpu_pct": 44},
    {"event_id": "E009", "timestamp": "14:00:09", "research_group": "G01", "job_type": "simulation", "duration_sec": 120, "status": "completed", "cpu_pct": 94},
    {"event_id": "E010", "timestamp": "14:00:10", "research_group": "G03", "job_type": "image", "duration_sec": 70, "status": "completed", "cpu_pct": 78},
]


def validate_event(event):
    required_keys = {"event_id", "timestamp", "research_group", "job_type", "duration_sec", "status", "cpu_pct"}
    if not isinstance(event,dict):
        return False
    if not required_keys.issubset(event):
        return False
    
    return True
    print("ok")

    

validate_event(EVENTS[0])