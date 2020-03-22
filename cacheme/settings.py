from collections import UserDict

CACHEME_BASE = {
    'ENABLE_CACHE': True,
    'REDIS_CACHE_PREFIX': 'CM:',  # key prefix for cache
    'REDIS_CACHE_SCAN_COUNT': 10,
    'REDIS_URL': 'redis://localhost:6379/0',
    'THUNDERING_HERD_RETRY_COUNT': 5,
    'THUNDERING_HERD_RETRY_TIME': 20,
    'STALE': True
}


class CACHEME(UserDict):

    def __init__(self):
        super().__init__()
        self.data.update(CACHEME_BASE)

    def __getattr__(self, name):
        try:
            return self.data[name]
        except KeyError:
            raise AttributeError('setting not found')


CACHEME = CACHEME()
