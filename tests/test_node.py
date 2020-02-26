
import redis
import unittest
from unittest import TestCase
from unittest.mock import MagicMock
from cacheme import cacheme
from tests import nodes, invalid_nodes

r = redis.Redis()


hit = MagicMock()
miss = MagicMock()

cacheme.set_connection(r)
cacheme.update_settings({'ENABLE_CACHE': True})


class BaseTestCase(TestCase):
    def tearDown(self):
        connection = redis.Redis()
        connection.flushdb()


class NodeTestCase(BaseTestCase):

    @cacheme(
        node=lambda c: nodes.TestNodeConstant(id=c.id)
    )
    def node_test_func_constant(self, id):
        return id

    @cacheme(
        node=lambda c: nodes.TestNodeDynamic(id=c.id)
    )
    def node_test_func_dynamic(self, id):
        return id

    def test_node_cache_basic(self):
        self.assertEqual(self.node_test_func_constant(1), 1)
        self.assertEqual(self.node_test_func_constant(2), 1)

        self.assertEqual(self.node_test_func_dynamic(1), 1)
        self.assertEqual(self.node_test_func_dynamic(2), 2)

    def test_node_cache_invalidation(self):
        self.assertEqual(self.node_test_func_constant(1), 1)
        cacheme.create_invalidation(
            invalid_key=str(invalid_nodes.InvalidUserNode(user=1))
        )
        self.assertEqual(self.node_test_func_constant(2), 2)
        self.assertEqual(self.node_test_func_constant(3), 2)

    def test_invalid_kwargs(self):
        with self.assertRaises(Exception) as e:
            cacheme(
                node=lambda c: nodes.TestNodeConstant(name='apple')
            )(lambda: 1)()
        self.assertEqual(
            str(e.exception),
            'id is required for TestNodeConstant'
        )

    def test_tag(self):
        self.node_test_func_constant(1)
        self.assertEqual(cacheme.tags['TestNodeConstant'].keys, {b'CM:test'})

    def test_node_keys(self):
        self.node_test_func_constant(1)
        node = nodes.TestNodeConstant
        self.assertEqual(node.objects.keys, {b'CM:test'})
        self.assertEqual(nodes.InvalidUserNode.objects.keys(user=1), {b'CM:test'})


if __name__ == '__main__':
    unittest.main()
