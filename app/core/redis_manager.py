import redis
from typing import Dict, Optional
from .config import settings

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        # Parse Redis nodes from settings
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        print(redis_nodes)

        # Create connection pools and clients for each Redis node
        for node in redis_nodes:
            pool = redis.ConnectionPool.from_url(node)
            self.connection_pools[node] = pool
            self.redis_clients[node] = redis.Redis(connection_pool=pool)

    def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key.
        
        Args:
            key: The key to determine which Redis node to use.

        Returns:
            Redis client.
        """

        node_index = hash(key) % len(self.redis_clients)
        node = list(self.redis_clients.keys())[node_index]
        return self.redis_clients[node]

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis.
        
        Args:
            key: The key to increment.
            amount: Amount to increment by.
            
        Returns:
            New value of the counter.
        """
        redis_conn = self.get_connection(key)
        return redis_conn.incrby(key, amount)

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis.
        
        Args:
            key: The key to get.
            
        Returns:
            Value of the key or None if not found.
        """
        redis_conn = self.get_connection(key)
        value = redis_conn.get(key)
        
        if value is None:
            return 0  # Return 0 if key doesn't exist
        
        if isinstance(value, bytes):
            value = value.decode('utf-8')  # Decode bytes to string
        
        return int(value)
