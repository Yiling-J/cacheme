from cacheme import nodes
from tests.invalid_nodes import InvalidUserNode


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
        return InvalidUserNode(user=self.id)


class TestNodeDynamic(nodes.Node):
    id = nodes.Field()

    def key(self):
        return 'test:%s' % self.id

    def invalid_nodes(self):
        return InvalidUserNode(user=self.id)


class TestNodeStale(nodes.Node):

    def key(self):
        return 'test>stale'


class TestNodeNoStale(nodes.Node):

    def key(self):
        return 'test>no_stale'

    class Meta:
        stale = False
