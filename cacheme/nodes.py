from cacheme import utils

tags = dict()


class NodeManager(object):
    connection = None

    def __init__(self, key):
        self.key = key
        self.utils = utils.CachemeUtils(self.CACHEME, self.connection)

    def invalid(self):
        iterator = self.connection.sscan_iter(self.CACHEME.REDIS_CACHE_PREFIX + self.key)
        return self.utils.invalid_iter(iterator)


class InvalidNodeManager(NodeManager):

    def __init__(self, node_cls):
        self.node_cls = node_cls
        self.utils = utils.CachemeUtils(self.CACHEME, self.connection)

    def invalid(self, **kwargs):
        node = self.node_cls(**kwargs)
        key = node.key_name
        invalid_key = key + ':invalid'
        iterator = self.connection.sscan_iter(self.CACHEME.REDIS_CACHE_PREFIX + invalid_key)
        return self.utils.invalid_iter(iterator)


class Field(object):
    pass


class NodeMetaClass(type):
    def __new__(cls, name, bases, attrs):
        attrs['required_fields'] = {
            field_name: attrs.pop(field_name)
            for field_name, obj in list(attrs.items())
            if isinstance(obj, Field)
        }

        node_class = super().__new__(cls, name, bases, attrs)
        if name and name != 'Node':
            tags[name] = node_class
            node_class.objects = NodeManager(node_class.__name__)
        return node_class

    @property
    def keys(cls):
        return cls.objects.keys


class Node(object, metaclass=NodeMetaClass):

    def __init__(self, **kwargs):
        for field in self.required_fields.keys():
            if field not in kwargs:
                raise Exception('{field} is required for {name}'.format(
                    field=field, name=self.__class__.__name__
                ))
            setattr(self, field, kwargs[field])

    def key(self):
        raise NotImplementedError()

    def invalid_nodes(self):
        return None


class InvalidNodeMetaClass(type):
    def __new__(cls, name, bases, attrs):

        node_class = super().__new__(cls, name, bases, attrs)
        if name and name != 'InvalidNode':
            node_class.objects = InvalidNodeManager(node_class)
        return node_class


class InvalidNode(object, metaclass=InvalidNodeMetaClass):

    def __init__(self, **kwargs):
        self.key_name = self.key(**kwargs)

    def __str__(self):
        return self.key_name

    def key(self, **kwargs):
        raise NotImplementedError()
