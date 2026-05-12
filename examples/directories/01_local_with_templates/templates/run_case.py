from pathlib import Path
import time


print("START case={{ case_id }} station={{ station }} compiler={{ compiler }}", flush=True)
time.sleep({{ sleep_seconds }})
Path("result.txt").write_text(
    "case={{ case_id }} station={{ station }} compiler={{ compiler }}\n"
)
print("DONE case={{ case_id }}", flush=True)
