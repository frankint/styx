import os
import multiprocessing

import uvloop

from worker.worker_service import Worker


N_THREADS = int(os.getenv('WORKER_THREADS', 1))
DEBUGPY_ENABLED: bool = os.getenv("STYX_DEBUGPY", "0").lower() in ("1", "true", "yes", "y")
DEBUGPY_HOST: str = os.getenv("STYX_DEBUGPY_HOST", "0.0.0.0")
# When WORKER_THREADS>1, each subprocess listens on BASE_PORT + thread_idx
DEBUGPY_BASE_PORT: int = int(os.getenv("STYX_DEBUGPY_BASE_PORT", "5678"))
DEBUGPY_WAIT: bool = os.getenv("STYX_DEBUGPY_WAIT", "1").lower() in ("1", "true", "yes", "y")


class BootStyx(object):

    def __init__(self):
        self.worker_threads_pool: list[multiprocessing.Process] = []

    @staticmethod
    def start_worker_thread(thread_idx: int):
        # NOTE: this runs in a separate OS process (multiprocessing.Process).
        # If you want breakpoints in worker code, you need to start debugpy in *this* process.
        if DEBUGPY_ENABLED:
            try:
                import debugpy  # type: ignore
                port = DEBUGPY_BASE_PORT + thread_idx
                debugpy.listen((DEBUGPY_HOST, port))
                if DEBUGPY_WAIT:
                    # This will pause startup until you attach from your IDE.
                    debugpy.wait_for_client()
                # Optional: break immediately after attach to ensure the debugger is alive.
                if os.getenv("STYX_DEBUGPY_BREAK_ON_START", "0").lower() in ("1", "true", "yes", "y"):
                    debugpy.breakpoint()
            except Exception as e:
                # Don't crash the worker if debugpy isn't available.
                # (e.g. image not rebuilt with debugpy installed)
                print(f"[debugpy] failed to start on thread_idx={thread_idx}: {e}", flush=True)
        worker = Worker(thread_idx)
        uvloop.run(worker.main())

    def main(self):
        for thread_idx in range(N_THREADS):
            self.worker_threads_pool.append(
                multiprocessing.Process(
                    target=self.start_worker_thread,
                    args=(thread_idx, )
                )
            )
        for worker_thread in self.worker_threads_pool:
            worker_thread.start()
        for worker_thread in self.worker_threads_pool:
            worker_thread.join()


if __name__ == "__main__":
    boot = BootStyx()
    boot.main()
