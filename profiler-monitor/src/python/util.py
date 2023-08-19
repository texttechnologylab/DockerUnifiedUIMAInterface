from asyncio import Condition, Lock
import threading as th

def format_bytes(num_bytes: float) -> str:
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}B"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} YB"

def format_time(num_time: float, format_str: str) -> str:
    units = {
        "seconds": lambda x: f"{num_time:.2f} s",
        "milliseconds": format_milli,
        "microseconds": lambda x: f"{num_time:.2f} us",
        "nanoseconds": format_nano
    }
    for unit in ['seconds', 'milliseconds', 'microseconds', 'nanoseconds']:
        if format_str == unit:
            return units[unit](num_time)

def format_milli(num_time: float) -> str:
    if num_time < 1000:
        return f"{num_time:.2f} ms"
    else:
        return f"{num_time / 1000:.2f} s"

def format_nano(num_time: float) -> str:
    if num_time < 1000:
        return f"{num_time:.2f} ns"
    elif num_time < 1_000_000:
        return f"{num_time / 1000:.2f} μs"
    elif num_time < 1_000_000_000:
        return f"{num_time / 1000000:.2f} ms"
    else:
        return f"{num_time / 1000000000:.2f} s"



def format_state(state: str):

    def label(color: str, state: str):
        return f"""
            <small class="d-inline-flex my-auto px-2 py-1 fw-semibold text-{color}-emphasis bg-{color}-subtle border border-{color}-subtle rounded-0">
                {state}
            </small>
        """
    state_to_color = {
        "RUNNING": "success",
        "RUNNABLE": "success",
        "NEW": "success",
        "WAITING": "warning",
        "TIMED_WAITING": "warning",
        "BLOCKED": "warning",
        "PAUSED": "warning",
        "RESTARTING": "warning",
        "EXITED": "danger",
        "IRRETRIEVABLE": "danger",
        "REMOVING": "danger"
    }

    return label(state_to_color[state.upper()], state.upper())


# simple countdown latch, starts closed then opens once count is reached
class CountDownLatch2():
    # constructor
    def __init__(self, count):
        # store the count
        self.count = count
        # control access to the count and notify when latch is open
        self.condition = th.Condition()
 
    # count down the latch by one increment
    async def count_down(self):
        # acquire the lock on the condition
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # decrement the counter
            self.count -= 1
            # check if the latch is now open
            if self.count == 0:
                # notify all waiting threads that the latch is open
                self.condition.notify_all()
 
    # wait for the latch to open
    async def wait(self, timeout: float = -1.0) -> bool:
        # acquire the lock on the condition
        
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return True
            if timeout == -1.0:
                return self.condition.wait()


            return self.condition.wait(timeout)

class CountDownLatch():
    # constructor
    def __init__(self, count):
        # store the count
        self.count = count
        self.sync = Lock()
        # control access to the count and notify when latch is open
        self.condition = Condition()
 
    # count down the latch by one increment
    async def count_down(self):
        # acquire the lock on the condition
        async with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # decrement the counter
            self.count -= 1
            # check if the latch is now open
            if self.count == 0:
                # notify all waiting threads that the latch is open
                self.condition.notify_all()
 
    # wait for the latch to open
    async def wait(self, timeout: float = -1.0) -> bool:
        # acquire the lock on the condition
        
        async with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return True
            await self.condition.wait()
            return True

