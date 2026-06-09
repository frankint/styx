import threading
from collections import deque
import re
import requests

class MigrationTracker:
    def __init__(self, metrics_url: str, poll_interval: float = 1.0):
        self.metrics_url = metrics_url
        self.poll_interval = poll_interval
        self.migrations: deque = deque()
        self._stop = threading.Event()
        self._thread = None
        self._last_count = 0
        self._current_migration = None
    
    def _parse_metric(self, text: str, name: str) -> float | None:
        match = re.search(rf'^{name}\s+(\S+)', text, re.MULTILINE)
        return float(match.group(1)) if match else None
    
    def _poll(self):
        while not self._stop.wait(self.poll_interval):
            try:
                resp = requests.get(self.metrics_url, timeout=0.5)
                start_count = self._parse_metric(resp.text, "migration_start_total") or 0
                end_count = self._parse_metric(resp.text, "migration_end_total") or 0
                start_time = self._parse_metric(resp.text, "migration_start_time_ms")
                end_time = self._parse_metric(resp.text, "migration_end_time_ms")
                
                # Detect new migration started
                if start_count > self._last_count:
                    self._current_migration = {"start": start_time, "end": None}
                    self._last_count = start_count
                
                # Detect migration completed
                if self._current_migration and end_time and end_time > (self._current_migration.get("start") or 0):
                    self._current_migration["end"] = end_time
                    self.migrations.append(self._current_migration)
                    self._current_migration = None
            except Exception:
                pass
    
    def start(self):
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
    
    def stop(self) -> list[dict]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        return list(self.migrations)