from geoumk.wsserver.utils import get_params_from_url
import asyncio
import websockets
from django.conf import settings
from django.core.cache import get_cache
import asyncio_redis

__author__ = 'vvf'

cache = get_cache(settings.STOPS_CONFIG['CACHE_NAME'])
redis_url = settings.CACHES[settings.STOPS_CONFIG['CACHE_NAME']]['LOCATION']


@asyncio.coroutine
def dispatcher(ws, path):
    #depending on path just add to needed queue and loop forever (ignoring messages)
    print('Connected to "{}"'.format(path))
    #subscriber = Subscriber(path, ws)
    yield from Subscriber.append(path, ws)
    #asyncio.async(subscriber.run())
    while True:
        name = yield from ws.recv()
        if name is None:
            break
        #print("< {}".format(name))
        # greeting = "Hello {}!".format(name)
#        yield from consumer(greeting)

# another way is listening to pattern channel through psubscribe
class Subscriber:
    redis_connection = None
    subscriber = None
    is_running = False
    connections = {}
    subscriptions = ['umk']
    def __init__(self, channel, ws):
        self.channel = channel
        #self.chanel_params = {channel: self}
        self.ws = ws
        self.state = 'ready'
        self.subs = None
        #redis_subscriber.subscribe(**self.chanel_params)

    @asyncio.coroutine
    def send(self, msg):
        # self.state = 'working'
        # self.subs = yield from self.get_redis_subscriber()
        if not self.ws.open:
            print('Socket closed')
            self.state = 'done'
            self.connections[self.channel].remove(self)
            return
        yield from self.ws.send(msg.value)

    @classmethod
    @asyncio.coroutine
    def run_redis_listener(cls):
        if cls.is_running:
            return
        cls.is_running=True
        subs = yield from cls.get_redis_subscriber()
        while True:
            msg = yield from subs.next_published()
            # TODO: if len(cls.connections[msg.channel]) - than unsubscribe from it
            print('Send msg "{}" to {} subscribers'.format(msg, len(cls.connections[msg.channel])))
            for c in cls.connections[msg.channel]:
                if c.state != 'done':
                    yield from c.send(msg)

    @classmethod
    @asyncio.coroutine
    def append(cls, path, ws):
        subscriber = Subscriber(path, ws)
        if path not in cls.connections:
            cls.connections[path] = []
            yield from cls.subscriber.unsubscribe(cls.subscriptions)
            cls.subscriptions = list(cls.connections.keys())
            yield from cls.subscriber.subscribe(cls.subscriptions)
        cls.connections[path].append(subscriber)

    @classmethod
    @asyncio.coroutine
    def get_redis_subscriber(cls):
        if cls.redis_connection and cls.subscriber:
            return cls.subscriber
        params = get_params_from_url(redis_url)
        cls.redis_connection = yield from asyncio_redis.Connection.create(**params)
        cls.subscriber = yield from cls.redis_connection.start_subscribe()

        yield from cls.subscriber.subscribe(cls.subscriptions)
        return cls.subscriber



def run_server(host='localhost', port=8765):

    print('Start websockets server on {}:{}'.format(host, port))

    start_server = websockets.serve(dispatcher, host, port)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.async(Subscriber.run_redis_listener())
    asyncio.get_event_loop().run_forever()