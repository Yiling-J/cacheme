from cacheme import nodes


class InvalidNodeNoKey(nodes.InvalidNode):
    pass


class InvalidUserNode(nodes.InvalidNode):
    user = nodes.Field()

    def key(self):
        return 'user:%s' % self.user


class StaleInvalidNode(nodes.InvalidNode):
    def key(self):
        return 'test_stale'


class NoStaleInvalidNode(nodes.InvalidNode):
    def key(self):
        return 'test_np_stale'
