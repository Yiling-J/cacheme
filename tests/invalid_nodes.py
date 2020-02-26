from cacheme.nodes import InvalidNode


class InvalidUserNode(InvalidNode):
    def key(self, user):
        return 'user:%s' % user
