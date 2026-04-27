import sys
import time



if __name__ == "__main__":
    if len(sys.argv) > 1:
        station = sys.argv[1]
        filename = f"station_results_{station}.txt"
        with open(filename, "w") as f:
            print(f"Station {station} started.", file=f)
            time.sleep(1)
            print(f"Station {station} FINISHED_SUCCESSFULLY", file=f)
    else:
        with open("station_results.txt", "w") as f:
            print("No station provided.", file=f)
