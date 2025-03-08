from typing import Dict
from ..core.redis_manager import RedisManager

class VisitCounterService:
    visit_counts: Dict[str, int] = {}

    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()

    async def increment_visit(self, page_id: str) -> None:
        await self.redis_manager.increment(f"visit_count:{page_id}")

    async def get_visit_count(self, page_id: str) -> int:
        visit_count = await self.redis_manager.get(f"visit_count:{page_id}")
        return visit_count

