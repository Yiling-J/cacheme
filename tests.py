import pickle
import time
import datetime

import redis
import unittest
from unittest import TestCase
from unittest.mock import MagicMock
from cacheme import cacheme, cacheme_tags

r = redis.Redis()


hit = MagicMock()
miss = MagicMock()


class NoConnectionTestCase(TestCase):

    def test_no_connection(self):
        cacheme.connection_set = False
        with self.assertRaises(Exception):
            cacheme(key=lambda c: 'test')
        cacheme.connection_set = True


cacheme.set_connection(r)


class BaseTestCase(TestCase):
    def tearDown(self):
        connection = redis.Redis()
        connection.flushdb()


class CacheTestCase(BaseTestCase):

    @cacheme(
        key=lambda c: 'test',
        invalid_keys=lambda c: ['test_invalid']
    )
    def basic_cache_func(self, n):
        return n

    def test_basic(self):
        self.assertEqual(self.basic_cache_func(1), 1)
        self.assertEqual(self.basic_cache_func(2), 1)
        cacheme.utils.invalid_keys_in_set('test_invalid')
        self.assertEqual(self.basic_cache_func(2), 2)

    @cacheme(
        key=lambda c: str(c.self.pp + c.a + c.args[0] + c.kwargs['ff']),
    )
    def cache_bind_func(self, a, *args, **kwargs):
        return self.pp + a + args[0] + kwargs['ff']

    def test_cache_arguments_bind(self):
        self.pp = 3
        result = self.cache_bind_func(1, 2, ff=14, qq=5)
        self.assertEqual(result, 20)
        result = r.hget('CM:' + '20', 'base')
        self.assertEqual(pickle.loads(result), 20)

    @cacheme(
        key=lambda c: "Test:123",
        hit=hit,
        miss=miss
    )
    def cache_test_func_hit_miss(self):
        return 'test'

    def test_cache_hit_miss(self):
        self.cache_test_func_hit_miss()
        self.assertEqual(miss.call_count, 1)
        hit.assert_not_called()
        self.cache_test_func_hit_miss()
        self.assertEqual(miss.call_count, 1)
        self.assertEqual(hit.call_count, 1)

    @cacheme(
        key=lambda c: "INST:1"
    )
    def cache_inst_1(self):
        return 'test'

    @cacheme(
        key=lambda c: "INST:2",
        tag='test_instance_sec'
    )
    def cache_inst_2(self):
        return 'test'

    @cacheme(
        key=lambda c: "INST:3",
        tag='three'
    )
    def cache_inst_3(self):
        return 'test'

    def test_tags(self):
        self.cache_inst_1()
        self.cache_inst_2()
        self.cache_inst_3()
        self.assertEqual(cacheme_tags['cache_inst_1'].keys, {b'CM:INST:1'})
        self.assertEqual(cacheme_tags['test_instance_sec'].keys, {b'CM:INST:2'})
        self.assertEqual(cacheme_tags['three'].keys, {b'CM:INST:3'})
        cacheme_tags['three'].invalid_all()
        self.assertEqual(cacheme_tags['three'].keys, set())

    @cacheme(
        key=lambda c: "CACHE:SKIP:1",
        tag='three',
        skip=True
    )
    def cache_skip_bool(self, data):
        return {'result': data['test']}

    @cacheme(
        key=lambda c: "CACHE:SKIP:2",
        tag='three',
        skip=lambda c: 'skip' in c.data
    )
    def cache_skip_callable(self, data):
        return {'result': data['test']}

    def test_skip_cache(self):
        result = self.cache_skip_bool({'test': 1})
        self.assertEqual(result['result'], 1)

        result = self.cache_skip_bool({'test': 2})
        self.assertEqual(result['result'], 2)

        result = self.cache_skip_callable({'test': 3})
        self.assertEqual(result['result'], 3)

        result = self.cache_skip_callable({'test': 4, 'skip': True})
        self.assertEqual(result['result'], 4)

        result = self.cache_skip_callable({'test': 5})
        self.assertEqual(result['result'], 5)

        result = self.cache_skip_callable({'test': 6})
        self.assertEqual(result['result'], 6)

    @cacheme(
        key=lambda c: "CACHE:TO",
        timeout=1
    )
    def cache_timeout(self, n):
        return n

    @cacheme(
        key=lambda c: "CACHE:TO2",
        timeout=1
    )
    def cache_timeout2(self, n):
        return n

    @cacheme(
        key=lambda c: "CACHE:TO3",
        timeout=3
    )
    def cache_timeout3(self, n):
        return n

    def test_time_out(self):
        self.assertEqual(self.cache_timeout(1), 1)
        self.assertEqual(self.cache_timeout(2), 1)
        self.assertEqual(self.cache_timeout2(1), 1)
        self.assertEqual(self.cache_timeout2(2), 1)
        time.sleep(1.02)
        self.assertEqual(self.cache_timeout(2), 2)
        self.assertEqual(self.cache_timeout2(2), 2)
        self.assertEqual(self.cache_timeout3(1), 1)
        self.assertEqual(self.cache_timeout2(3), 2)
        time.sleep(1.02)
        self.assertEqual(self.cache_timeout2(3), 3)
        self.assertEqual(self.cache_timeout3(3), 1)

    @cacheme(
        key=lambda c: "CACHE:TH",
    )
    def cache_th(self, n):
        return n

    def test_key_missing(self):
        r.sadd('CM:progress', 'CM:CACHE:TH')
        start = datetime.datetime.now()
        result = self.cache_th(12)
        end = datetime.datetime.now()
        delta = (end - start).total_seconds() * 1000
        self.assertEqual(result, 12)
        self.assertTrue(delta > 50)

        r.sadd('CM:progress', 'CM:CACHE:TH')
        start = datetime.datetime.now()
        result = self.cache_th(15)
        end = datetime.datetime.now()
        self.assertEqual(result, 12)
        self.assertTrue(delta > 50)


if __name__ == '__main__':
    unittest.main()
