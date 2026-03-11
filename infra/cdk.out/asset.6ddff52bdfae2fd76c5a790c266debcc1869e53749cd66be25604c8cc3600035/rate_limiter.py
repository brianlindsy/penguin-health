"""
Rate Limiter for Bedrock API calls.

Enforces a sliding window rate limit to stay within AWS Bedrock's
requests-per-minute limits.
"""

import time
from threading import Lock


class RateLimiter:
    """Rate limiter to enforce max requests per minute."""

    def __init__(self, max_requests_per_minute=10):
        self.max_requests = max_requests_per_minute
        self.request_times = []
        self.lock = Lock()

    def wait_if_needed(self):
        """Block until a request can be made within rate limits."""
        with self.lock:
            now = time.time()
            # Remove requests older than 60 seconds
            self.request_times = [t for t in self.request_times if now - t < 60]

            if len(self.request_times) >= self.max_requests:
                # Wait until oldest request expires
                sleep_time = 60 - (now - self.request_times[0]) + 1
                print(f"Rate limit reached. Waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                # Clean up again after sleeping
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < 60]

            self.request_times.append(time.time())
