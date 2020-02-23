from cacheme.node import Node, Field
from tests.invalid_nodes import InvalidUserNode


class TestNodeConstant(Node):
    id = Field()

    def key(self):
        return 'test'

    def invalid_nodes(self):
        return InvalidUserNode(user=self.id)


class TestNodeDynamic(Node):
    id = Field()

    def key(self):
        return 'test:%s' % self.id

    def invalid_nodes(self):
        return InvalidUserNode(user=self.id)
