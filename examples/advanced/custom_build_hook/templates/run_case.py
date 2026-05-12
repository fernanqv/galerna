from pathlib import Path


Path("result.txt").write_text(
    "case={{ case_id }} station={{ station }} derived={{ derived }}\n"
)
print("DONE case={{ case_id }} station={{ station }} derived={{ derived }}")
