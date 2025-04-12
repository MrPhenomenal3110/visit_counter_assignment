import time
import asyncio
from typing import Dict, Tuple
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        self.redis_manager = RedisManager()
        self.local_memory: Dict[str, Tuple[int, float]] = {}
        self.local_ttl = 5  # seconds (TTL for cache)
        self.cleanup_interval = 3  # cleanup every 3 seconds

        # Task to clean expired cache
        asyncio.create_task(self._cleanup_expired_local_cache())

        # Task to batch process updates to Redis
        asyncio.create_task(self._batch_update_to_redis())

        # Store the keys that need to be updated in Redis (within the TTL)
        self.local_updates: Dict[str, int] = {}

    async def _cleanup_expired_local_cache(self):
        """Periodically clean up expired entries from local memory."""
        while True:
            now = time.time()
            keys_to_delete = [
                key for key, (_, timestamp) in self.local_memory.items()
                if now - timestamp > self.local_ttl
            ]
            for key in keys_to_delete:
                del self.local_memory[key]
            await asyncio.sleep(self.cleanup_interval)

    async def _batch_update_to_redis(self):
        """Periodically batch update the local memory to Redis after TTL (5 seconds)."""
        while True:
            # Wait for 5 seconds before flushing to Redis
            await asyncio.sleep(self.local_ttl)

            # Batch update all local memory updates to Redis
            if self.local_updates:
                redis_keys = []
                redis_values = []
                for key, count in self.local_updates.items():
                    redis_keys.append(key)
                    redis_values.append(count)

                # Perform a batch set operation to Redis (mset)
                if redis_keys and redis_values:
                    redis_conn = self.redis_manager.get_connection(redis_keys[0])  # Use any connection
                    redis_conn.mset(dict(zip(redis_keys, redis_values)))
                    print(f"Batch updated to Redis: {redis_keys}")

                # Clear the local updates after syncing with Redis
                self.local_updates.clear()

    async def increment_visit(self, page_id: str) -> None:
        """Increment the visit count, store locally, and delay the Redis update."""
        redis_key = f"visit_count:{page_id}"
        new_count = self.local_memory.get(page_id, (0, time.time()))[0] + 1

        # Update local memory
        self.local_memory[page_id] = (new_count, time.time())
        
        # Keep track of local updates to sync later with Redis
        self.local_updates[redis_key] = new_count

    async def get_visit_count(self, page_id: str) -> Tuple[int, str]:
        """Retrieve the visit count from local cache or Redis."""
        data = self.local_memory.get(page_id)
        if data:
            value, _ = data
            return value, 'in_memory'

        # Fallback to Redis if expired or not in cache
        redis_key = f"visit_count:{page_id}"
        visit_count = await self.redis_manager.get(redis_key)
        return visit_count, 'redis'
