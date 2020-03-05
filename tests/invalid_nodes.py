from cacheme import nodes


class InvalidUserNode(nodes.InvalidNode):
    user = nodes.Field()

    def key(self):
        return 'user:%s' % self.user
