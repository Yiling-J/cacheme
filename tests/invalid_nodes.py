from cacheme.node import InvalidNode


class InvalidUserNode(InvalidNode):
    def key(self, user):
        return 'user:%s' % user
