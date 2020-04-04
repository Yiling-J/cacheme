from datetime import datetime, timedelta
from cacheme import settings


class MetaKeys(object):
    data = set()
    prefix = settings.REDIS_CACHE_PREFIX + 'meta:'

    def __getattr__(self, attr):
        if attr not in self.data:
            self.data.add(attr)
        return self.prefix + attr


class CachemeUtils(object):
    meta_keys = MetaKeys()

    def __init__(self, conn=None):
        self.conn = conn

    def split_key(self, string):
        lg = b'>' if type(string) == bytes else '>'
        if lg in string:
            return string.split(lg)[:2]
        return [string, 'base']

    def flat_list(self, li):
        if type(li) not in (list, tuple, set):
            li = [li]

        result = []
        for e in li:
            if type(e) in (list, tuple, set):
                result += self.flat_list(e)
            else:
                result.append(e)
        return result

    def chunk_iter(self, iterator, size, stop):
        while True:
            result = {next(iterator, stop) for i in range(size)}
            if stop in result:
                result.remove(stop)
                yield result
                break
            yield result

    def invalid_iter(self, iterator):
        count = 0
        chunks = self.chunk_iter(iterator, 500, None)
        for keys in chunks:
            if keys:
                count += self.conn.sadd(
                    self.meta_keys.deleted,
                    *list(keys)
                )
        return count

    def unlink_iter(self, iterator):
        count = 0
        chunks = self.chunk_iter(iterator, 500, None)
        for keys in chunks:
            if keys:
                count += self.conn.unlink(*list(keys))
        return count

    def invalid_key(self, key):
        return self.conn.sadd(
            self.meta_keys.deleted,
            key
        )

    def get_epoch(self, seconds=0):
        dt = datetime.utcnow() + timedelta(seconds=seconds)
        return int(dt.timestamp())

    def get_metakey(self, key, field):
        return '%s%s:%s' % (
            settings.REDIS_CACHE_PREFIX,
            'Meta:Expire-Buckets:',
            key
        )

    def hset_with_ttl(self, key, field, value, ttl, pipe):
        if field != 'base':
            raw = '>'.join([key, field])
        else:
            raw = key
        pipe.zadd(self.get_metakey(key, field), {raw: self.get_epoch(ttl)})
        pipe.hset(key, field, value)
        pipe.execute()

    def invalid_ttl(self, key):
        key, field = self.split_key(key)
        pipe = self.conn.pipeline()
        metadataKey = self.get_metakey(key, field)
        now = self.get_epoch()

        pipe.zrangebyscore(metadataKey, 0, now)
        pipe.zremrangebyscore(metadataKey, 0, now)
        results = pipe.execute()
        if results[0]:
            self.conn.sadd(self.meta_keys.deleted, *results[0])

        return results[1]
