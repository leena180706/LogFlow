
import json
import os
import threading
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


# --------------------------------------------------
# ENVIRONMENT CONFIGURATION
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")


# --------------------------------------------------
# SQS QUEUE MANAGER
# --------------------------------------------------

class SQSQueueManager:

    def __init__(self):

        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.queue_url = os.getenv("SQS_QUEUE_URL")

        if not self.queue_url:
            raise ValueError(
                "SQS_QUEUE_URL is missing from environment variables."
            )

        # Configure connection timeouts and retries.
        boto_config = Config(
            connect_timeout=5,
            read_timeout=15,
            retries={
                "max_attempts": 5,
                "mode": "standard"
            },
            max_pool_connections=50
        )

        self.sqs = boto3.client(
            "sqs",
            region_name=self.region,
            config=boto_config
        )

        # Publisher configuration.
        self.batch_size = 10
        self.flush_interval = 0.15

        self.pending_logs = []
        self.pending_lock = threading.Lock()

        self.flush_event = threading.Event()
        self.stopping = threading.Event()

        # Publisher metrics.
        self.total_sent = 0
        self.total_send_failed = 0
        self.total_batches = 0

        self.stats_lock = threading.Lock()

        self.publisher_thread = threading.Thread(
            target=self._publisher_loop,
            name="SQSBatchPublisher",
            daemon=True
        )

        self.publisher_thread.start()

        print("[SQS] Queue manager initialized.")
        print("[SQS] Batch size:", self.batch_size)
        print("[SQS] Flush interval:", self.flush_interval)

    # --------------------------------------------------
    # TEST AWS CONNECTION
    # --------------------------------------------------

    def test_connection(self):

        try:

            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["QueueArn"]
            )

            queue_arn = response.get(
                "Attributes", {}
            ).get("QueueArn", "Unknown")

            print("\n[SQS CONNECTION SUCCESSFUL]")
            print("Region:", self.region)
            print("Queue ARN:", queue_arn)

            return True

        except (ClientError, BotoCoreError) as error:

            print("[SQS CONNECTION FAILED]", error)
            raise

    # --------------------------------------------------
    # ADD ONE LOG TO LOCAL PUBLISHING BUFFER
    # --------------------------------------------------

    def send_log(self, log):

        if self.stopping.is_set():
            raise RuntimeError("SQS publisher is stopping.")

        with self.pending_lock:

            self.pending_logs.append(log)

            should_flush = (
                len(self.pending_logs) >= self.batch_size
            )

        if should_flush:
            self.flush_event.set()

        # True means accepted into local buffer,
        # not necessarily delivered to AWS yet.
        return True

    # --------------------------------------------------
    # SEND LOGS IN BATCHES OF MAXIMUM 10
    # --------------------------------------------------

    def send_logs_batch(self, logs):

        total_sent = 0

        for start in range(0, len(logs), 10):

            group = logs[start:start + 10]

            entries = []

            for index, log in enumerate(group):

                entries.append({
                    "Id": str(index),
                    "MessageBody": json.dumps(log)
                })

            try:

                response = self.sqs.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )

                successful = response.get(
                    "Successful", []
                )

                failed = response.get(
                    "Failed", []
                )

                total_sent += len(successful)

                with self.stats_lock:

                    self.total_sent += len(successful)
                    self.total_send_failed += len(failed)
                    self.total_batches += 1

                if failed:

                    print(
                        "[SQS BATCH PARTIAL FAILURE]",
                        failed
                    )

            except (ClientError, BotoCoreError) as error:

                print("[SQS BATCH SEND ERROR]", error)

                with self.stats_lock:
                    self.total_send_failed += len(group)

                raise

        return total_sent

    # --------------------------------------------------
    # PUBLISHER: FLUSH PENDING LOGS
    # --------------------------------------------------

    def _flush_pending(self):

        with self.pending_lock:

            if not self.pending_logs:
                return

            batch = self.pending_logs[:]
            self.pending_logs.clear()

        failed_logs = []

        # Send batches of maximum 10 messages.
        for start in range(0, len(batch), 10):

            group = batch[start:start + 10]

            entries = [
                {
                    "Id": str(index),
                    "MessageBody": json.dumps(log)
                }
                for index, log in enumerate(group)
            ]

            try:

                response = self.sqs.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )

                successful = response.get(
                    "Successful", []
                )

                failed = response.get(
                    "Failed", []
                )

                failed_ids = {
                    item["Id"] for item in failed
                }

                successful_count = len(successful)

                for index, log in enumerate(group):

                    if str(index) in failed_ids:
                        failed_logs.append(log)

                with self.stats_lock:

                    self.total_sent += successful_count
                    self.total_send_failed += len(failed)
                    self.total_batches += 1

                if failed:

                    print(
                        "[SQS] Failed messages will be retried:",
                        failed
                    )

            except (ClientError, BotoCoreError) as error:

                print("[SQS BATCH SEND ERROR]", error)

                failed_logs.extend(group)

                with self.stats_lock:
                    self.total_send_failed += len(group)

        # Return failed messages to the pending buffer.
        if failed_logs and not self.stopping.is_set():

            with self.pending_lock:

                self.pending_logs = (
                    failed_logs + self.pending_logs
                )

            print(
                f"[SQS RETRY] "
                f"{len(failed_logs)} messages returned to buffer."
            )

    # --------------------------------------------------
    # BACKGROUND PUBLISHER LOOP
    # --------------------------------------------------

    def _publisher_loop(self):

        print("[SQS] Background publisher started.")

        while not self.stopping.is_set():

            self.flush_event.wait(
                timeout=self.flush_interval
            )

            self.flush_event.clear()

            try:

                self._flush_pending()

            except Exception as error:

                print(
                    "[SQS PUBLISHER ERROR]",
                    error
                )

            # Retry failed messages after a short delay.
            with self.pending_lock:

                has_pending = bool(self.pending_logs)

            if has_pending and not self.stopping.is_set():

                self.stopping.wait(1)

        # Final flush when stopping.
        try:

            self._flush_pending()

        except Exception as error:

            print(
                "[SQS FINAL FLUSH ERROR]",
                error
            )

        print("[SQS] Background publisher stopped.")

    # --------------------------------------------------
    # RECEIVE MESSAGES
    # --------------------------------------------------

    def receive_messages(
        self,
        max_messages=10,
        wait_time_seconds=2
    ):

        try:

            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max(
                    1,
                    min(max_messages, 10)
                ),
                WaitTimeSeconds=max(
                    0,
                    min(wait_time_seconds, 20)
                ),
                AttributeNames=["All"],
                MessageAttributeNames=["All"]
            )

            messages = []

            for message in response.get("Messages", []):

                try:

                    log = json.loads(
                        message["Body"]
                    )

                    messages.append({
                        "log": log,
                        "receipt_handle": message[
                            "ReceiptHandle"
                        ],
                        "message_id": message[
                            "MessageId"
                        ]
                    })

                except (json.JSONDecodeError, KeyError) as error:

                    print(
                        "[SQS INVALID MESSAGE]",
                        error
                    )

                    # Leave invalid messages in SQS.
                    # They can be handled by a dead-letter
                    # queue if one is configured.

            return messages

        except (ClientError, BotoCoreError) as error:

            print("[SQS RECEIVE ERROR]", error)
            raise

    # --------------------------------------------------
    # DELETE PROCESSED MESSAGE
    # --------------------------------------------------

    def delete_message(self, receipt_handle):

        try:

            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )

            return True

        except (ClientError, BotoCoreError) as error:

            print("[SQS DELETE ERROR]", error)
            raise

    # --------------------------------------------------
    # GET QUEUE METRICS
    # --------------------------------------------------

    def get_queue_metrics(self):

        try:

            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed"
                ]
            )

            attributes = response.get(
                "Attributes", {}
            )

            return {
                "messages_waiting": int(
                    attributes.get(
                        "ApproximateNumberOfMessages", 0
                    )
                ),

                "messages_processing": int(
                    attributes.get(
                        "ApproximateNumberOfMessagesNotVisible", 0
                    )
                ),

                "messages_delayed": int(
                    attributes.get(
                        "ApproximateNumberOfMessagesDelayed", 0
                    )
                )
            }

        except (ClientError, BotoCoreError) as error:

            print("[SQS METRICS ERROR]", error)
            raise

    # --------------------------------------------------
    # GET PUBLISHER STATUS
    # --------------------------------------------------

    def get_publisher_status(self):

        with self.pending_lock:

            pending = len(self.pending_logs)

        with self.stats_lock:

            sent = self.total_sent
            failed = self.total_send_failed
            batches = self.total_batches

        return {
            "pending_local": pending,
            "total_sent": sent,
            "total_send_failed": failed,
            "total_batches": batches
        }

    # --------------------------------------------------
    # STOP PUBLISHER
    # --------------------------------------------------

    def stop(self):

        if self.stopping.is_set():
            return

        print("[SQS] Stopping publisher...")

        self.stopping.set()
        self.flush_event.set()

        if self.publisher_thread.is_alive():

            self.publisher_thread.join(
                timeout=15
            )

        with self.pending_lock:

            remaining = len(self.pending_logs)

        if remaining:

            print(
                f"[SQS WARNING] {remaining} "
                "messages remain in local buffer."
            )
