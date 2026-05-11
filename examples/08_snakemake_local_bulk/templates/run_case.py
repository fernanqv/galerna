from pathlib import Path
import time

case_id = "{{ case_id }}"
station = "{{ station }}"
sleep_seconds = {{ sleep_seconds }}

print(f"START {case_id} station={station}", flush=True)
time.sleep(sleep_seconds)

Path(f"result_{case_id}.txt").write_text(
    f"case_id={case_id}\nstation={station}\n"
)

print(f"DONE {case_id}", flush=True)
