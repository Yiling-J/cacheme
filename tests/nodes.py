from cacheme import nodes
from tests import invalid_nodes


class NodeNoKey(nodes.Node):
    pass


class BasicNode(nodes.Node):

    def key(self):
        return 'basic'


class TestNodeConstant(nodes.Node):
    id = nodes.Field()

    def key(self):
        return 'test'

    def invalid_nodes(self):
        return invalid_nodes.InvalidUserNode(user=self.id)


class TestNodeDynamic(nodes.Node):
    id = nodes.Field()

    def key(self):
        return 'test:%s' % self.id

    def invalid_nodes(self):
        return invalid_nodes.InvalidUserNode(user=self.id)


class TestNodeStale(nodes.Node):

    def key(self):
        return 'test>stale'

    def invalid_nodes(self):
        return invalid_nodes.StaleInvalidNode()


class TestNodeNoStale(nodes.Node):

    def key(self):
        return 'test>no_stale'

    def invalid_nodes(self):
        return invalid_nodes.NoStaleInvalidNode()

    class Meta:
        stale = False


class TestNodeHitMiss(nodes.Node):
    id = nodes.Field()

    def key(self):
        return 'test:%s' % self.id

    def hit(self, key, result):
        return

    def miss(self, key):
        return
