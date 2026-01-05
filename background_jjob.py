#A simple in-memory Python background job queue system supporting concurrency, retries, and scheduling.

import threading
import time
import uuid
import heapq
from datetime import datetime, timedelta

class Job:
    def __init__(self, fn, args=(), kwargs=None, retry_limit=3, scheduled_for=None):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.retry_limit = retry_limit
        self.retries = 0
        self.id = str(uuid.uuid4())
        self.scheduled_for = scheduled_for or datetime.now()
        self.last_exception = None

    def run(self):
        try:
            self.fn(*self.args, **self.kwargs)
            return True
        except Exception as e:
            self.last_exception = e
            self.retries += 1
            return False
    
    # For the heap queue, jobs scheduled earlier come first
    def __lt__(self, other):
        return self.scheduled_for < other.scheduled_for

class JobQueue:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs_heap = []

    def push(self, job):
        with self._lock:
            heapq.heappush(self._jobs_heap, job)

    def pop(self):
        with self._lock:
            now = datetime.now()
            if not self._jobs_heap:
                return None
            if self._jobs_heap[0].scheduled_for <= now:
                return heapq.heappop(self._jobs_heap)
            else:
                return None

    def peek_next_time(self):
        with self._lock:
            if not self._jobs_heap:
                return None
            return self._jobs_heap[0].scheduled_for

    def not_empty(self):
        with self._lock:
            return len(self._jobs_heap) > 0

# The worker that pulls and executes jobs
class JobWorker(threading.Thread):
    def __init__(self, queue, worker_id):
        super().__init__()
        self.queue = queue
        self.daemon = True
        self.worker_id = worker_id
        self._stop_flag = threading.Event()

    def stop(self):
        self._stop_flag.set()

    def run(self):
        while not self._stop_flag.is_set():
            job = self.queue.pop()
            if not job:
                # Sleep briefly until next job needs to be scheduled, or jobs are empty
                next_time = self.queue.peek_next_time()
                if next_time:
                    now = datetime.now()
                    delay = (next_time - now).total_seconds()
                    if delay > 0:
                        time.sleep(min(delay, 1))
                    else:
                        time.sleep(0.1)
                else:
                    time.sleep(0.2)
                continue

            result = job.run()
            if not result:
                if job.retries < job.retry_limit:
                    # Re-schedule for immediate retry (or schedule after some delay)
                    job.scheduled_for = datetime.now() + timedelta(seconds=2 ** job.retries) # exponential backoff
                    self.queue.push(job)
                else:
                    print(f"[Worker {self.worker_id}] Job {job.id} failed after {job.retry_limit} retries. Last exception: {job.last_exception}")
            else:
                print(f"[Worker {self.worker_id}] Job {job.id} succeeded.")

class JobServer:
    def __init__(self, concurrency_limit=2):
        self.queue = JobQueue()
        self.workers = []
        self.concurrency_limit = concurrency_limit

    def start(self):
        for i in range(self.concurrency_limit):
            worker = JobWorker(self.queue, worker_id=i)
            worker.start()
            self.workers.append(worker)
        print(f"JobServer started with concurrency limit: {self.concurrency_limit}")

    def stop(self):
        for worker in self.workers:
            worker.stop()
        for worker in self.workers:
            worker.join()
        print("JobServer stopped.")

    def push_job(self, fn, args=(), kwargs=None, retry_limit=3, scheduled_for=None):
        job = Job(fn, args=args, kwargs=kwargs, retry_limit=retry_limit, scheduled_for=scheduled_for)
        self.queue.push(job)
        print(f"Pushed job {job.id} (scheduled_for={job.scheduled_for})")
        return job.id

# --------------- Client usage example ---------------
# Example jobs
def send_email(email_address):
    print(f"Sending email to {email_address}")
    # Simulate a potential failure
    if email_address == "fail@example.com":
        raise Exception("SMTP error!")
    print(f"Email sent to {email_address}")

def scrape_web(url):
    print(f"Scraping URL: {url}")
    # Simulate web scraping
    time.sleep(0.2)
    print(f"Scraping complete: {url}")

if __name__ == "__main__":
    server = JobServer(concurrency_limit=3)
    server.start()

    # Push jobs (including a job that will fail and trigger retries)
    server.push_job(send_email, args=("alice@example.com",))
    server.push_job(send_email, args=("fail@example.com",), retry_limit=2)
    server.push_job(scrape_web, args=("https://example.com/page1",))
    server.push_job(scrape_web, args=("https://example.com/page2",))

    # Schedule a job for 5 seconds later
    future_time = datetime.now() + timedelta(seconds=5)
    server.push_job(send_email, args=("delayed@example.com",), scheduled_for=future_time)

    try:
        # Let the server run jobs for 10 seconds
        time.sleep(10)
    finally:
        server.stop()
