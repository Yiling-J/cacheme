import time
import pickle
import datetime
import logging

from functools import wraps
from inspect import _signature_from_function, Signature

from cacheme.utils import CachemeUtils
from cacheme import nodes


logger = logging.getLogger('cacheme')


class CacheMe(object):
    connection_set = False
    settings_set = False
    utils = None
    tags = nodes.tags

    @classmethod
    def set_connection(cls, connection):
        cls.conn = connection
        nodes.NodeManager.connection = connection
        cls.connection_set = True

    @classmethod
    def update_settings(cls, settings):
        cls.CACHEME = cls.merge_settings(settings)
        cls.settings_set = True
        nodes.CACHEME = cls.CACHEME
        nodes.NodeManager._initialized = True

    @classmethod
    def merge_settings(cls, settings):
        CACHEME = {
            'ENABLE_CACHE': True,
            'REDIS_CACHE_PREFIX': 'CM:',  # key prefix for cache
            'REDIS_CACHE_SCAN_COUNT': 10,
            'REDIS_URL': 'redis://localhost:6379/0',
            'THUNDERING_HERD_RETRY_COUNT': 5,
            'THUNDERING_HERD_RETRY_TIME': 20
        }

        CACHEME.update(settings)
        return type('CACHEME', (), CACHEME)

    def __init__(self, key=None, invalid_keys=None, hit=None, miss=None, tag=None, skip=False, timeout=None, invalid_sources=None, node=None, **kwargs):

        if not self.connection_set:
            raise Exception('No connection find, please use set_connection first!')
        if not self.settings_set:
            self.update_settings({})
            logger.warning('No custom settings found, use default.')

        if not self.CACHEME.ENABLE_CACHE:
            return

        self.__class__.utils = CachemeUtils(self.CACHEME, self.conn)

        self.key_prefix = self.CACHEME.REDIS_CACHE_PREFIX
        self.deleted = self.key_prefix + 'delete'

        self.node = node
        self.key = key
        self.invalid_keys = invalid_keys
        self.hit = hit
        self.miss = miss
        self.tag = tag
        self.skip = skip
        self.timeout = timeout
        self.progress_key = self.key_prefix + 'progress'
        self.invalid_sources = invalid_sources
        self.kwargs = kwargs

        self.conn = self.conn
        sources = self.collect_sources()
        if sources:
            for source in sources:
                self.connect(source)

    def __call__(self, func):

        if not self.CACHEME.ENABLE_CACHE:
            return func

        self.function = func

        self.tag = self.tag or func.__name__
        if not self.node:
            self.tags[self.tag] = self

        @wraps(func)
        def wrapper(*args, **kwargs):

            # bind args and kwargs to true function params
            signature = _signature_from_function(Signature, func)
            bind = signature.bind(*args, **kwargs)
            bind.apply_defaults()

            # then apply args and kwargs to a container,
            # in this way, we can have clear lambda with just one
            # argument, and access what we need from this container
            self.container = type('Container', (), bind.arguments)

            if callable(self.skip) and self.skip(self.container):
                return self.function(*args, **kwargs)
            elif self.skip:
                return self.function(*args, **kwargs)

            node = None
            if self.node:
                node = self.node(self.container)
                key = node.key_name
                self.tag = node.__class__.__name__
            else:
                key = self.key_prefix + self.key(self.container)

            if self.timeout:
                result = self.get_key(key)

            if self.conn.srem(self.deleted, key):
                result = self.function(*args, **kwargs)
                self.set_result(key, result)
                self.container.cacheme_result = result
                self.add_to_invalid_list(node, key, args, kwargs)
                return result

            if self.timeout is None:
                result = self.get_key(key)

            if result is None:

                if self.add_to_progress(key) == 0:  # already in progress
                    for i in range(self.CACHEME.THUNDERING_HERD_RETRY_COUNT):
                        time.sleep(self.CACHEME.THUNDERING_HERD_RETRY_TIME/1000)
                        result = self.get_key(key)
                        if result:
                            return result

                result = self.get_result_from_func(args, kwargs, key)
                self.set_result(key, result)
                self.remove_from_progress(key)
                self.container.cacheme_result = result
                self.add_to_invalid_list(node, key, args, kwargs)
            else:
                if self.hit:
                    self.hit(key, result, self.container)
                result = result

            self.container = None
            return result

        return wrapper

    def add_key_to_tag(self, val):
        self.conn.sadd(self.CACHEME.REDIS_CACHE_PREFIX + self.tag, val)

    def invalid_all(self):
        iterator = self.conn.sscan_iter(self.CACHEME.REDIS_CACHE_PREFIX + self.tag)
        return self.utils.invalid_iter(iterator)

    def get_result_from_func(self, args, kwargs, key):
        if self.miss:
            self.miss(key, self.container)

        start = datetime.datetime.now()
        result = self.function(*args, **kwargs)
        end = datetime.datetime.now()
        delta = (end - start).total_seconds() * 1000
        logger.debug(
            '[CACHEME FUNC LOG] key: "%s", time: %s ms' % (key, delta)
        )
        return result

    def set_result(self, key, result):
        self.set_key(key, result)

    def get_key(self, key):
        key, field = self.utils.split_key(key)
        if self.timeout:
            result = self.utils.hget_with_ttl(key, field)
        else:
            result = self.conn.hget(key, field)

        if result:
            result = pickle.loads(result)
        return result

    def set_key(self, key, value):
        self.add_key_to_tag(key)
        value = pickle.dumps(value)
        key, field = self.utils.split_key(key)
        if self.timeout:
            self.utils.hset_with_ttl(key, field, value, self.timeout)
        else:
            self.conn.hset(key, field, value)

    def push_key(self, key, value):
        return self.conn.sadd(key, value)

    def add_to_invalid_list(self, node, key, args, kwargs):
        if node:
            invalid_nodes = node.invalid_nodes()
            if not invalid_nodes:
                return
            invalid_keys = [str(i) for i in self.utils.flat_list(invalid_nodes)]
        else:
            invalid_keys = self.invalid_keys
            if not invalid_keys:
                return
            invalid_keys = self.utils.flat_list(invalid_keys(self.container))

        for invalid_key in set(filter(lambda x: x is not None, invalid_keys)):
            invalid_key += ':invalid'
            invalid_key = self.key_prefix + invalid_key
            self.push_key(invalid_key, key)

    def collect_sources(self):
        return self.invalid_sources

    def connect(self, source):
        pass

    def remove_from_progress(self, key):
        self.conn.srem(self.progress_key, key)

    def add_to_progress(self, key):
        return self.conn.sadd(self.progress_key, key)

    @classmethod
    def create_invalidation(cls, key=None, invalid_key=None, pattern=None):
        invalid_keys = set()

        if isinstance(key, str):
            invalid_keys.add(cls.CACHEME.REDIS_CACHE_PREFIX + key)

        if isinstance(invalid_key, str):
            key = cls.CACHEME.REDIS_CACHE_PREFIX + invalid_key + ':invalid'
            invalid_keys.update(cls.conn.smembers(key))

        if invalid_keys:
            cls.conn.sadd(cls.CACHEME.REDIS_CACHE_PREFIX + 'delete', *invalid_keys)

        if isinstance(pattern, str):
            iterator = cls.conn.scan_iter(pattern, count=cls.CACHEME.REDIS_CACHE_SCAN_COUNT)
            cls.utils.invalid_iter(iterator)
