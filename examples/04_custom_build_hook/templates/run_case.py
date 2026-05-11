from pathlib import Path
import time


print("START case={{ case_id }} derived={{ derived_value }}", flush=True)
time.sleep(1)
Path("result.txt").write_text("derived={{ derived_value }}\n")
print("DONE case={{ case_id }}", flush=True)
