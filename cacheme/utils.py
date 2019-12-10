from datetime import datetime, timedelta


class CachemeUtils(object):

    def __init__(self, settings, conn):
        self.CACHEME = settings
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

    def invalid_pattern(self, pattern):
        chunks = self.chunk_iter(self.conn.scan_iter(pattern, count=self.CACHEME.REDIS_CACHE_SCAN_COUNT), 500, None)
        for keys in chunks:
            if keys:
                self.conn.unlink(*list(keys))

    def get_epoch(self, seconds=0):
        dt = datetime.utcnow() + timedelta(seconds=seconds)
        return int(dt.timestamp())

    def get_metakey(self, key, field):
        return '%s%s:%s' % (
            self.CACHEME.REDIS_CACHE_PREFIX,
            'Meta:Expire-Buckets:',
            key
        )

    def hset_with_ttl(self, key, field, value, ttl):
        if field != 'base':
            raw = '>'.join([key, field])
        else:
            raw = key
        pipe = self.conn.pipeline()
        pipe.zadd(self.get_metakey(key, field), {raw: self.get_epoch(ttl)})
        pipe.hset(key, field, value)
        pipe.execute()

    def hget_with_ttl(self, key, field):
        pipe = self.conn.pipeline()
        metadataKey = self.get_metakey(key, field)
        now = self.get_epoch()

        expired = self.conn.zrangebyscore(metadataKey, 0, now)
        if expired:
            self.conn.sadd(self.CACHEME.REDIS_CACHE_PREFIX + 'delete', *expired)
        pipe.zremrangebyscore(metadataKey, 0, now)

        pipe.hget(key, field)
        return pipe.execute()[-1]
