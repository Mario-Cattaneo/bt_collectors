from datetime import datetime, timezone

def log(self, msg, level="INFO"):
    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    if levels.index(level) >= 1:
        now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        print(f"[{now_iso}] [{level}] {msg}")