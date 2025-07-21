from enum import Enum
from threading import Lock


class Downloadstatus(str, Enum):
    idle = 'idle'
    running = 'running'


class ExecutionState:
    def __init__(self):
        self.status = Downloadstatus.idle
        self._lock = Lock()

    def set_running(self):
        with self._lock:
            self.status = Downloadstatus.running

    def set_idle(self):
        with self._lock:
            self.status = Downloadstatus.idle

    def get_status(self) -> Downloadstatus:
        with self._lock:
            return self.status
