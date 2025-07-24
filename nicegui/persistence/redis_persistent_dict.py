from .. import background_tasks, core, json, optional_features
from ..logging import log
from .persistent_dict import PersistentDict

try:
    import redis as redis_sync
    import redis.asyncio as redis

    optional_features.register('redis')
except ImportError:
    pass


class RedisPersistentDict(PersistentDict):

    def __init__(self, *, url: str, id: str, key_prefix: str = 'nicegui:') -> None:  # pylint: disable=redefined-builtin
        if not optional_features.has('redis'):
            raise ImportError('Redis is not installed. Please run "pip install nicegui[redis]".')
        self.url = url
        '''
        ISSUE: Creating self.redis_client in __init__ with no regards as to how this storage is being used.
        
        This contributes to the overall client connection count, and again we have no control over this in
        the current implementation.
        
        SUGGESTED FIX: Move creating this client connection to a callable function, and create it *only* when
          it is required to create a pubsub connection:
          1) When redis already has data for this key, and we need to watch it for obvious reasons and we need a 
             client connection to initiate said pubsub watch.
          2) All other redis calls, including the ones in self.initialize() and self.initialize_sync()
             should be using context managers and not keeping long-lived database connections open.
             
        I understand the cost of creating client connections may slow things down a bit, but we have to
        respect the redis client connection limit.
        '''
        self.redis_client = redis.from_url(
            url,
            health_check_interval=10,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            socket_keepalive=True,
        )
        self.pubsub = self.redis_client.pubsub()
        self.key = key_prefix + id
        super().__init__(data={}, on_change=self.publish)
        # This *kind* of works, but creating a connection just to close it like this
        # when we *know* we don't want to use storage is not ideal.
        # if not self:
            # self.redis_client.close()
            # self.clear()

    async def initialize(self) -> None:
        """Load initial data from Redis and start listening for changes."""
        try:
            data = await self.redis_client.get(self.key)
            self.update(json.loads(data) if data else {})
            # We could try something like this, but this is problematic as well. What if it's a legit key
            # that isn't even user storage, like tab or general? Another instance could spin up a key
            # from this browser and this instance wouldn't know about any changes. So not a great idea either.
            # if data:
            #     self._start_listening()
            self._start_listening()
        except Exception:
            log.warning(f'Could not load data from Redis with key {self.key}')

    def initialize_sync(self) -> None:
        """Load initial data from Redis and start listening for changes in a synchronous context."""
        with redis_sync.from_url(
                self.url,
                health_check_interval=10,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                socket_keepalive=True,
        ) as redis_client_sync:
            try:
                data = redis_client_sync.get(self.key)
                self.update(json.loads(data) if data else {})
                self._start_listening()
            except Exception:
                log.warning(f'Could not load data from Redis with key {self.key}')

    def _start_listening(self) -> None:
        async def listen():
            await self.pubsub.subscribe(self.key + 'changes')
            async for message in self.pubsub.listen():
                if message['type'] == 'message':
                    new_data = json.loads(message['data'])
                    if new_data != self:
                        self.update(new_data)

        if core.loop and core.loop.is_running():
            background_tasks.create(listen(), name=f'redis-listen-{self.key}')
        else:
            core.app.on_startup(listen())

    def publish(self) -> None:
        """Publish the data to Redis and notify other instances."""
        '''
        Let's suppose you put in:
        
        if not self: 
          return
        
        All this does is prevent the key from being created, and the pubsub client connection persists.
        '''

        async def backup() -> None:
            pipeline = self.redis_client.pipeline()
            pipeline.set(self.key, json.dumps(self))
            pipeline.publish(self.key + 'changes', json.dumps(self))
            await pipeline.execute()

        if core.loop:
            background_tasks.create_lazy(backup(), name=f'redis-{self.key}')
        else:
            core.app.on_startup(backup())

    async def close(self) -> None:
        """Close Redis connection and subscription."""
        await self.pubsub.unsubscribe()
        await self.pubsub.close()
        await self.redis_client.close()

    def clear(self) -> None:
        super().clear()
        if core.loop:
            background_tasks.create_lazy(self.redis_client.delete(self.key), name=f'redis-delete-{self.key}')
        else:
            core.app.on_startup(self.redis_client.delete(self.key))
