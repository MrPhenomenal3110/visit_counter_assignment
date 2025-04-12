import time
import asyncio
from typing import Dict, Tuple
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        self.redis_manager = RedisManager()
        self.local_memory: Dict[str, Tuple[int, float]] = {}
        self.local_ttl = 5  # seconds
        self.cleanup_interval = 3  # cleanup every 3 seconds

        asyncio.create_task(self._cleanup_expired_local_cache())

    async def _cleanup_expired_local_cache(self):
        while True:
            now = time.time()
            keys_to_delete = [
                key for key, (_, timestamp) in self.local_memory.items()
                if now - timestamp > self.local_ttl
            ]
            for key in keys_to_delete:
                del self.local_memory[key]
            await asyncio.sleep(self.cleanup_interval)

    async def increment_visit(self, page_id: str) -> None:
        redis_key = f"visit_count:{page_id}"
        new_count = await self.redis_manager.increment(redis_key)

        self.local_memory[page_id] = (new_count, time.time())

    async def get_visit_count(self, page_id: str) -> Tuple[int, str]:
        data = self.local_memory.get(page_id)
        if data:
            value, _ = data
            return (value, 'in_memory')

        redis_key = f"visit_count:{page_id}"
        visit_count = await self.redis_manager.get(redis_key)
        self.local_memory[page_id] = (visit_count, time.time())
        return (visit_count, 'redis')
