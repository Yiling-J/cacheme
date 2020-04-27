import mock
import pickle
import time
import datetime

import redis
from multiprocessing.dummy import Pool
from unittest import TestCase
from unittest.mock import MagicMock
from cacheme import cacheme, settings

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
cacheme.update_settings({'ENABLE_CACHE': False})


class SettingsTest(TestCase):
    def test_settings(self):
        cacheme.update_settings({'foo': 'bar'})
        self.assertEqual(settings.foo, 'bar')
        with self.assertRaises(AttributeError):
            settings.bar


class BaseTestCase(TestCase):
    def tearDown(self):
        connection = redis.Redis()
        connection.flushdb()
        hit.reset_mock()
        miss.reset_mock()


class CacheTestCase(BaseTestCase):

    @cacheme(
        key=lambda c: 'test>me',
        invalid_keys=lambda c: ['test_invalid']
    )
    def basic_cache_func_disable(self, n):
        return n

    def test_disable(self):
        self.assertEqual(self.basic_cache_func_disable(1), 1)
        self.assertEqual(self.basic_cache_func_disable(2), 2)

    cacheme.update_settings({'ENABLE_CACHE': True})

    @cacheme(
        key=lambda c: 'test>me',
        invalid_keys=lambda c: ['test_invalid']
    )
    def basic_cache_func(self, n):
        return n

    def test_basic(self):
        self.assertEqual(self.basic_cache_func(1), 1)
        self.assertEqual(self.basic_cache_func(2), 1)
        cacheme.create_invalidation(invalid_key='test_invalid')
        self.assertEqual(self.basic_cache_func(2), 2)

    @cacheme(
        key=lambda c: 'test>me',
        invalid_keys=lambda c: 'test_invalid'
    )
    def basic_cache_func2(self, n):
        return n

    def test_basic2(self):
        self.assertEqual(self.basic_cache_func2(1), 1)
        self.assertEqual(self.basic_cache_func2(2), 1)
        cacheme.create_invalidation(invalid_key='test_invalid')
        self.assertEqual(self.basic_cache_func2(2), 2)

    @cacheme(
        key=lambda c: 'test>me',
        invalid_keys=lambda c: [['test_invalid']]
    )
    def basic_cache_func3(self, n):
        return n

    def test_basic3(self):
        self.assertEqual(self.basic_cache_func3(1), 1)
        self.assertEqual(self.basic_cache_func3(2), 1)
        cacheme.create_invalidation(invalid_key='test_invalid')
        self.assertEqual(self.basic_cache_func3(2), 2)

    @cacheme(
        key=lambda c: str(c.self.pp + c.a + c.args[0] + c.kwargs['ff']),
    )
    def cache_bind_func(self, a, *args, **kwargs):
        return self.pp + a + args[0] + kwargs['ff']

    def test_cache_arguments_bind(self):
        self.pp = 3
        result = self.cache_bind_func(1, 2, ff=14, qq=5)
        self.assertEqual(result, 20)
        self.assertEqual(cacheme.get_key(cacheme, 'CM:20'), 20)

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

    @cacheme(
        key=lambda c: "INST:4",
        tag='four'
    )
    def cache_inst_4(self):
        return 'test'

    def test_tags(self):
        self.cache_inst_1()
        self.cache_inst_2()
        self.cache_inst_3()
        self.assertEqual(cacheme.tags['cache_inst_1'].objects.invalid(), 1)
        self.assertEqual(cacheme.tags['test_instance_sec'].objects.invalid(), 1)
        self.assertEqual(cacheme.tags['three'].objects.invalid(), 1)
        self.assertEqual(cacheme.tags['three'].objects.invalid(), 0)

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
        key=lambda c: "CACHE:TO3>test",
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
        r.sadd(cacheme.meta_keys.progress, 'CM:CACHE:TH')
        start = datetime.datetime.now()
        result = self.cache_th(12)
        end = datetime.datetime.now()
        delta = (end - start).total_seconds() * 1000
        self.assertEqual(result, 12)
        self.assertTrue(delta > 50)

        r.sadd(cacheme.meta_keys.progress, 'CM:CACHE:TH')
        start = datetime.datetime.now()
        result = self.cache_th(15)
        end = datetime.datetime.now()
        self.assertEqual(result, 12)
        self.assertTrue(delta > 50)

    class FakeTime(object):
        counter = 0

        def __call__(self):
            return self

        def sleep(self, n):
            self.counter += 1
            if self.counter % 5 == 0:
                r.hset('CM:CACHE:TH', 'base', pickle.dumps('100'))
            return mock.Mock()

    faker = FakeTime()

    @mock.patch('cacheme.cache_model.time', new_callable=faker)
    def test_thunder_herd_wait_success(self, m):
        r.sadd(cacheme.meta_keys.progress, 'CM:CACHE:TH')
        result = self.cache_th(12)
        self.assertEqual(result, '100')

    def test_invalid_key(self):
        self.basic_cache_func(1)
        self.assertEqual(self.basic_cache_func(2), 1)
        cacheme.create_invalidation(key='test>me')
        self.assertEqual(self.basic_cache_func(2), 2)

    def test_invalid_pattern(self):
        for i in range(10000):
            r.set('test:%s' % i, i)

        cacheme.create_invalidation(pattern='test*')
        self.assertFalse(r.get('test:600'))

    @cacheme(
        key=lambda c: 'invalid_source',
        invalid_keys=lambda c: ['test_invalid'],
        invalid_sources=['apple']
    )
    def invalid_source_test_func(self, n):
        return n

    def test_invalid_source(self):
        self.invalid_source_test_func(1)
        self.assertEqual(self.invalid_source_test_func(2), 1)


@cacheme(
    key=lambda c: 'test>stale',
    invalid_keys=lambda c: ['test_stale'],
    tag='test_stale'
)
def stale_test_func(n):
    time.sleep(0.1)
    return n


@cacheme(
    key=lambda c: 'test>no_stale',
    invalid_keys=lambda c: ['test_no_stale'],
    stale=False,
    tag='test_no_stale'
)
def no_stale_test_func(n):
    time.sleep(0.1)
    return n


class StaleTestMixin(object):

    def tearDown(self):
        connection = redis.Redis()
        connection.flushdb()

    def invalid(self, key):
        raise NotImplementedError()

    def test_base(self):
        self.assertEqual(
            stale_test_func(1),
            stale_test_func(2)
        )

        self.assertEqual(
            no_stale_test_func(1),
            no_stale_test_func(2)
        )

    def test_stale(self):
        stale_test_func(1)
        self.invalid('test>stale')

        p = Pool(2)
        result = p.map(stale_test_func, [2, 2])
        self.assertEqual(set(result), {1, 2})

    def test_no_stale(self):
        no_stale_test_func(1)
        self.invalid('test>no_stale')

        p = Pool(2)
        result = p.map(no_stale_test_func, [2, 2])
        self.assertEqual(set(result), {2, 2})


class StaleInvalidationKeyTestCase(StaleTestMixin, TestCase):

    def invalid(self, key):
        cacheme.create_invalidation(key=key)


class StaleInvalidationInvalidKeyTestCase(StaleTestMixin, TestCase):

    def invalid(self, key):
        invalid_key = key.replace('>', '_')
        cacheme.create_invalidation(invalid_key=invalid_key)


class StaleInvalidationTagTestCast(StaleTestMixin, TestCase):

    def invalid(self, key):
        tag = key.replace('>', '_')
        cacheme.tags[tag].objects.invalid()


class StaleInvalidationPatternTestCast(StaleTestMixin, TestCase):

    def invalid(self, key):
        cacheme.create_invalidation(pattern='CM:test*')

    def test_stale(self):
        stale_test_func(1)
        self.invalid('test>stale')

        p = Pool(2)
        result = p.map(stale_test_func, [2, 2])
        self.assertEqual(set(result), {2, 2})


class CompressTestCase(CacheTestCase):

    @classmethod
    def setUpClass(cls):
        cacheme.update_settings({'COMPRESS': True, 'COMPRESSTHRESHOLD': 0})

    @classmethod
    def tearDownClass(cls):
        cacheme.update_settings({'COMPRESS': False, 'COMPRESSTHRESHOLD': 1000})


class StaleInvalidationKeyCompressTestCase(StaleTestMixin, TestCase):

    @classmethod
    def setUpClass(cls):
        cacheme.update_settings({'COMPRESS': True, 'COMPRESSTHRESHOLD': 0})

    @classmethod
    def tearDownClass(cls):
        cacheme.update_settings({'COMPRESS': False, 'COMPRESSTHRESHOLD': 1000})

    def invalid(self, key):
        cacheme.create_invalidation(key=key)
