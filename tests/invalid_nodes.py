from cacheme import nodes


class InvalidNodeNoKey(nodes.InvalidNode):
    pass


class InvalidUserNode(nodes.InvalidNode):
    user = nodes.Field()

    def key(self):
        return 'user:%s' % self.user
