import time
import pickle
import datetime
import logging

from functools import wraps
from inspect import _signature_from_function, Signature

from cacheme import settings, utils as cacheme_utils
from cacheme import nodes


logger = logging.getLogger('cacheme')


class CacheMe(object):
    connection_set = False
    settings_set = False
    utils = cacheme_utils.CachemeUtils()
    tags = nodes.tags
    meta_keys = cacheme_utils.MetaKeys()

    @classmethod
    def set_connection(cls, connection):
        cls.conn = connection
        nodes.NodeManager.connection = connection
        cls.connection_set = True
        cls.utils.conn = connection

    @classmethod
    def update_settings(cls, new_settings):
        cls.merge_settings(new_settings)
        nodes.NodeManager._initialized = True

    @classmethod
    def merge_settings(cls, new_settings):
        settings.update(new_settings)

    def __init__(self, key=None, invalid_keys=None, hit=None, miss=None, tag=None, skip=False, timeout=None, invalid_sources=None, node=None, stale=None, **kwargs):

        if not self.connection_set:
            raise Exception('No connection find, please use set_connection first!')
        if not self.settings_set:
            logger.warning('No custom settings found, use default.')

        if not settings.ENABLE_CACHE:
            return

        self.key_prefix = settings.REDIS_CACHE_PREFIX

        self.node = node
        self.key = key
        self.invalid_keys = invalid_keys
        self.hit = hit
        self.miss = miss
        self.tag = tag
        self.skip = skip
        self.timeout = timeout
        self.invalid_sources = invalid_sources
        self.kwargs = kwargs
        self.stale = settings.STALE if stale is None else stale

        self.conn = self.conn
        sources = self.collect_sources()
        if sources:
            for source in sources:
                self.connect(source)

    def __call__(self, func):

        if not settings.ENABLE_CACHE:
            return func

        self.function = func

        self.tag = self.tag or func.__name__
        if not self.node:
            self.tags[self.tag] = type(self.tag, (nodes.Node,), {})

        @wraps(func)
        def wrapper(*args, **kwargs):

            # bind args and kwargs to true function params
            signature = _signature_from_function(Signature, func)
            bind = signature.bind(*args, **kwargs)
            bind.apply_defaults()

            # then apply args and kwargs to a container,
            # in this way, we can have clear lambda with just one
            # argument, and access what we need from this container
            container = type('Container', (), bind.arguments)

            if callable(self.skip) and self.skip(container):
                return self.function(*args, **kwargs)
            elif self.skip:
                return self.function(*args, **kwargs)

            stale = self.stale
            node = None
            if self.node:
                node = self.node(container)
                key = node.key_name
                self.tag = node.__class__.__name__
                stale = node.meta.get('stale', self.stale)
            else:
                key = self.key_prefix + self.key(container)

            if self.timeout:
                self.utils.invalid_ttl(key)

            if stale:
                status, result = self._get_key_stale(key)
            else:
                status, result = self._get_key_no_stale(key)

            if status in ('new', 'deleted'):
                if self.add_to_progress(key) == 0:  # already in progress
                    for i in range(settings.THUNDERING_HERD_RETRY_COUNT):
                        time.sleep(settings.THUNDERING_HERD_RETRY_TIME/1000)
                        result = self.get_key(key)
                        if result:
                            return result

                result = self.get_result_from_func(key, container, node, args, kwargs)
                pipe = self.conn.pipeline()
                self.set_result(key, result, pipe)
                self.remove_from_progress(key, pipe)
                container.cacheme_result = result
                self.add_to_invalid_list(node, key, container, pipe, args, kwargs)
                pipe.execute()
            else:
                if self.hit:
                    self.hit(key, result, container)
                elif node:
                    node.hit(key, result)
                result = result

            return result

        return wrapper

    def add_key_to_tag(self, val, pipe):
        pipe.sadd(settings.REDIS_CACHE_PREFIX + self.tag, val)

    def get_result_from_func(self, key, container, node, args, kwargs):
        if self.miss:
            self.miss(key, container)
        elif node:
            node.miss(key)

        start = datetime.datetime.now()
        result = self.function(*args, **kwargs)
        end = datetime.datetime.now()
        delta = (end - start).total_seconds() * 1000
        logger.debug(
            '[CACHEME FUNC LOG] key: "%s", time: %s ms' % (key, delta)
        )
        return result

    def set_result(self, key, result, pipe):
        self.set_key(key, result, pipe)

    def get_key(self, key):
        key, field = self.utils.split_key(key)
        result = self.conn.hget(key, field)

        if result:
            result = pickle.loads(result)
        return result

    def _get_key_stale(self, key):
        redis_call = "if redis.call('srem',KEYS[1], ARGV[1]) == 0 then return {'valid', redis.call('hget', KEYS[2], ARGV[2])} else return {'deleted', 0} end"
        key_base, field = self.utils.split_key(key)
        response = self.conn.eval(
            redis_call, 2,
            self.meta_keys.deleted, key_base,
            key, field
        )
        if response[0] == b'valid':
            return ('valid', pickle.loads(response[1])) if response[1] is not None else ('new', 0)
        return ('deleted', 0)

    def _get_key_no_stale(self, key):
        redis_call = "if redis.call('srem',KEYS[1], ARGV[1]) == 1 then return {'deleted', redis.call('hdel', KEYS[2], ARGV[2])} else return {'valid', redis.call('hget', KEYS[2], ARGV[2])} end"
        key_base, field = self.utils.split_key(key)
        response = self.conn.eval(
            redis_call, 2,
            self.meta_keys.deleted, key_base,
            key, field
        )
        if response[0] == b'valid':
            return ('valid', pickle.loads(response[1])) if response[1] is not None else ('new', 0)
        return ('deleted', 0)

    def set_key(self, key, value, pipe):
        self.add_key_to_tag(key, pipe)
        value = pickle.dumps(value)
        key, field = self.utils.split_key(key)
        if self.timeout:
            self.utils.hset_with_ttl(key, field, value, self.timeout, pipe)
        else:
            pipe.hset(key, field, value)

    def _push_key_pipe(self, key, value, pipe):
        pipe.sadd(key, value)

    def add_to_invalid_list(self, node, key, container, pipe, args, kwargs):
        invalid_suffix = ':invalid'
        if node:
            invalid_nodes = node.invalid_nodes()
            if not invalid_nodes:
                return
            invalid_keys = [str(i) for i in self.utils.flat_list(invalid_nodes)]
        else:
            invalid_keys = self.invalid_keys
            if not invalid_keys:
                return
            invalid_keys = self.utils.flat_list(invalid_keys(container))

        for invalid_key in set(filter(lambda x: x is not None, invalid_keys)):
            invalid_key += invalid_suffix
            invalid_key = self.key_prefix + invalid_key
            self._push_key_pipe(invalid_key, key, pipe)
        pipe.execute()

    def collect_sources(self):
        return self.invalid_sources

    def connect(self, source):
        pass

    def remove_from_progress(self, key, pipe):
        pipe.srem(self.meta_keys.progress, key)

    def add_to_progress(self, key):
        return self.conn.sadd(self.meta_keys.progress, key)

    @classmethod
    def create_invalidation(cls, key=None, invalid_key=None, pattern=None):

        if isinstance(key, str):
            cls.conn.sadd(
                cls.meta_keys.deleted,
                settings.REDIS_CACHE_PREFIX + key
            )

        if isinstance(invalid_key, str):
            invalid_key = settings.REDIS_CACHE_PREFIX + invalid_key
            iterator = cls.conn.sscan_iter(invalid_key + ':invalid', count=settings.REDIS_CACHE_SCAN_COUNT)
            cls.utils.invalid_iter(iterator)

        if isinstance(pattern, str):
            iterator = cls.conn.scan_iter(pattern, count=settings.REDIS_CACHE_SCAN_COUNT)
            cls.utils.unlink_iter(iterator)
