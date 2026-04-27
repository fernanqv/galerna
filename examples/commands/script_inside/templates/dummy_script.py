import sys
import time

station = {{ station }}

filename = f"station_results_{station}.txt"
with open(filename, "w") as f:
    print(f"Station {station} started.", file=f)
    time.sleep(1)
    print(f"Station {station} FINISHED_SUCCESSFULLY", file=f)

