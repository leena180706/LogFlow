
import math
import threading
import time

from queue_manager import SQSQueueManager
from storage import save_log, initialize_database
from indexer import build_index, add_log_to_index


class DynamicWorkerManager:

    def __init__(
        self,
        min_workers=2,
        max_workers=40,
        messages_per_worker=8,
        check_interval=0.75,
        sqs_manager=None
    ):

        # Reuse the application's SQS manager when supplied.
        # Otherwise create one for standalone execution.
        self.sqs_manager = sqs_manager or SQSQueueManager()

        self.min_workers = min_workers
        self.max_workers = max_workers
        self.messages_per_worker = messages_per_worker
        self.check_interval = check_interval

        self.workers = {}
        self.next_worker_id = 1

        self.manager_thread = None
        self.manager_stop_event = threading.Event()

        self.stats_lock = threading.Lock()
        self.workers_lock = threading.RLock()

        self.total_processed = 0
        self.total_failed = 0

        self.desired_workers = min_workers
        self.running = False
        self.last_error = None

    # --------------------------------------------------
    # START WORKER MANAGER
    # --------------------------------------------------

    def start(self):

        if self.running:
            print("[MANAGER] Already running.")
            return

        print("\n[MANAGER] Initializing worker system...")

        try:

            initialize_database()
            build_index()

            self.sqs_manager.test_connection()

            self.manager_stop_event.clear()

            self.running = True
            self.desired_workers = self.min_workers

            # Start minimum workers.
            self._add_workers(self.min_workers)

            # Start supervisor.
            self.manager_thread = threading.Thread(
                target=self._manager_loop,
                name="DynamicWorkerSupervisor",
                daemon=True
            )

            self.manager_thread.start()

            print("[MANAGER] Dynamic worker manager started.")
            print(
                f"[MANAGER] Worker range: "
                f"{self.min_workers}-{self.max_workers}"
            )

        except Exception:

            self.running = False
            self.manager_stop_event.set()

            raise

    # --------------------------------------------------
    # GET ACTUAL ACTIVE WORKER COUNT
    # --------------------------------------------------

    def _active_worker_count(self):

        with self.workers_lock:

            return sum(
                1
                for worker in self.workers.values()
                if worker["thread"].is_alive()
                and not worker["stop_event"].is_set()
            )

    # --------------------------------------------------
    # DYNAMIC WORKER SUPERVISOR
    # --------------------------------------------------

    def _manager_loop(self):

        print("[MANAGER] Supervisor monitoring started.")

        while not self.manager_stop_event.is_set():

            try:

                self._remove_finished_workers()

                # Get actual AWS SQS backlog.
                metrics = self.sqs_manager.get_queue_metrics()

                waiting = metrics["messages_waiting"]
                processing = metrics["messages_processing"]

                backlog = waiting + processing

                # Calculate desired worker count.
                desired = math.ceil(
                    backlog / self.messages_per_worker
                )

                desired = max(
                    self.min_workers,
                    desired
                )

                desired = min(
                    self.max_workers,
                    desired
                )

                current = self._active_worker_count()

                self.desired_workers = desired
                self.last_error = None

                print(
                    f"[SCALING] "
                    f"Waiting={waiting} | "
                    f"In-flight={processing} | "
                    f"Backlog={backlog} | "
                    f"Active={current} | "
                    f"Target={desired}"
                )

                # Scale up.
                if desired > current:

                    self._add_workers(
                        desired - current
                    )

                # Scale down.
                elif desired < current:

                    self._remove_workers(
                        current - desired
                    )

            except Exception as error:

                self.last_error = str(error)

                print(
                    "[MANAGER ERROR]",
                    error
                )

            self.manager_stop_event.wait(
                self.check_interval
            )

        print("[MANAGER] Supervisor stopped.")

    # --------------------------------------------------
    # ADD WORKERS
    # --------------------------------------------------

    def _add_workers(self, count):

        added = 0

        for _ in range(count):

            if self.manager_stop_event.is_set():
                break

            with self.workers_lock:

                active = sum(
                    1
                    for worker in self.workers.values()
                    if worker["thread"].is_alive()
                    and not worker["stop_event"].is_set()
                )

                if active >= self.max_workers:
                    break

                worker_id = self.next_worker_id
                self.next_worker_id += 1

                stop_event = threading.Event()

                worker_info = {
                    "thread": None,
                    "stop_event": stop_event,
                    "state": "starting",
                    "processed": 0,
                    "failed": 0
                }

                thread = threading.Thread(
                    target=self._worker_loop,
                    args=(worker_id, worker_info),
                    name=f"LogWorker-{worker_id}",
                    daemon=True
                )

                worker_info["thread"] = thread

                self.workers[worker_id] = worker_info

                thread.start()

                added += 1

            print(
                f"[SCALING UP] Worker {worker_id} started."
            )

        return added

    # --------------------------------------------------
    # REMOVE WORKERS
    # --------------------------------------------------

    def _remove_workers(self, count):

        stopped = 0

        with self.workers_lock:

            candidates = sorted(
                self.workers.items(),
                key=lambda item: item[0],
                reverse=True
            )

            for worker_id, worker in candidates:

                if stopped >= count:
                    break

                if (
                    worker["thread"].is_alive()
                    and not worker["stop_event"].is_set()
                ):

                    worker["stop_event"].set()

                    stopped += 1

                    print(
                        f"[SCALING DOWN] "
                        f"Worker {worker_id} stopping."
                    )

        return stopped

    # --------------------------------------------------
    # REMOVE FINISHED WORKERS
    # --------------------------------------------------

    def _remove_finished_workers(self):

        with self.workers_lock:

            finished = [
                worker_id
                for worker_id, worker in self.workers.items()
                if not worker["thread"].is_alive()
            ]

            for worker_id in finished:

                del self.workers[worker_id]

                print(
                    f"[MANAGER] Worker {worker_id} removed."
                )

    # --------------------------------------------------
    # PROCESS ONE MESSAGE
    # --------------------------------------------------

    def _process_message(
        self,
        worker_id,
        worker_info,
        message
    ):

        log = message["log"]
        receipt_handle = message["receipt_handle"]

        worker_info["state"] = "processing"

        try:

            # Save log in SQLite.
            database_id = save_log(log)

            if database_id is None:
                raise RuntimeError(
                    "Database did not return a log ID."
                )

            # Update inverted index.
            add_log_to_index(
                database_id,
                log.get("server", ""),
                log.get("level", ""),
                log.get("message", "")
            )

            # Delete SQS message only after successful storage
            # and indexing.
            self.sqs_manager.delete_message(
                receipt_handle
            )

            with self.stats_lock:

                self.total_processed += 1

            worker_info["processed"] += 1

            print(
                f"[WORKER {worker_id}] "
                f"Log processed successfully | "
                f"Database ID: {database_id}"
            )

            return True

        except Exception as error:

            with self.stats_lock:

                self.total_failed += 1

            worker_info["failed"] += 1

            print(
                f"[WORKER {worker_id}] "
                f"Processing failed: {error}"
            )

            # Do not delete failed messages.
            # SQS can make them visible again after
            # their visibility timeout.

            return False

    # --------------------------------------------------
    # WORKER THREAD
    # --------------------------------------------------

    def _worker_loop(self, worker_id, worker_info):

        stop_event = worker_info["stop_event"]

        print(
            f"[WORKER {worker_id}] Started."
        )

        try:

            while not stop_event.is_set():

                worker_info["state"] = "idle"

                try:

                    messages = self.sqs_manager.receive_messages(
                        max_messages=10,
                        wait_time_seconds=2
                    )

                    if not messages:
                        continue

                    worker_info["state"] = "processing"

                    for message in messages:

                        # Finish already-received messages
                        # before exiting the worker.
                        self._process_message(
                            worker_id,
                            worker_info,
                            message
                        )

                    worker_info["state"] = "idle"

                except Exception as error:

                    worker_info["state"] = "error"

                    print(
                        f"[WORKER {worker_id}] "
                        f"Receive error: {error}"
                    )

                    stop_event.wait(2)

        except Exception as error:

            worker_info["state"] = "error"

            print(
                f"[WORKER {worker_id}] "
                f"Fatal worker error: {error}"
            )

        finally:

            worker_info["state"] = "stopped"

            print(
                f"[WORKER {worker_id}] Stopped."
            )

    # --------------------------------------------------
    # GET WORKER STATUS
    # --------------------------------------------------

    def get_status(self):

        with self.stats_lock:

            processed = self.total_processed
            failed = self.total_failed

        worker_details = []

        with self.workers_lock:

            for worker_id, worker in self.workers.items():

                thread = worker["thread"]

                if thread.is_alive():

                    worker_details.append({

                        "id": worker_id,

                        "name": thread.name,

                        "state": worker["state"],

                        "processed": worker["processed"],

                        "failed": worker["failed"]

                    })

            active = sum(
                1
                for worker in self.workers.values()
                if worker["thread"].is_alive()
                and not worker["stop_event"].is_set()
            )

        return {

            "running": self.running,

            "active_workers": active,

            # Actual scaling target, not active count.
            "desired_workers": self.desired_workers,

            "min_workers": self.min_workers,

            "max_workers": self.max_workers,

            "total_processed": processed,

            "total_failed": failed,

            "last_error": self.last_error,

            "workers": worker_details

        }

    # --------------------------------------------------
    # STOP WORKER MANAGER
    # --------------------------------------------------

    def stop(self):

        if not self.running:
            return

        print("[MANAGER] Stopping workers...")

        self.manager_stop_event.set()

        with self.workers_lock:

            for worker in self.workers.values():

                worker["stop_event"].set()

        if self.manager_thread:

            self.manager_thread.join(
                timeout=5
            )

        with self.workers_lock:

            worker_threads = [
                worker["thread"]
                for worker in self.workers.values()
            ]

        for thread in worker_threads:

            thread.join(timeout=5)

        self._remove_finished_workers()

        self.running = False

        print("[MANAGER] Worker manager stopped.")

        print(
            "[MANAGER] Final statistics:",
            self.get_status()
        )


# --------------------------------------------------
# STANDALONE TESTING
# --------------------------------------------------

if __name__ == "__main__":

    manager = DynamicWorkerManager()

    try:

        manager.start()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        print("Stopping requested.")

    finally:

        manager.stop()
