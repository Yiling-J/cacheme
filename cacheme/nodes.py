from cacheme import utils

tags = dict()


class NodeManager(object):
    connection = None
    _initialized = False

    def __init__(self, node_class):
        self.key = node_class.__name__
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
        if node_class.manager._initialized:
            node_class.objects = node_class.manager(node_class)
        node_class._update_class()
        return node_class

    def _update_class(cls):
        pass


class Node(object, metaclass=NodeMetaClass):
    manager = NodeManager

    @classmethod
    def _update_class(cls):
        name = cls.__name__
        if name != 'Node':
            tags[name] = cls

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


class InvalidNode(object, metaclass=NodeMetaClass):
    manager = InvalidNodeManager

    def __init__(self, **kwargs):

        for field in self.required_fields.keys():
            if field not in kwargs:
                raise Exception('{field} is required for {name}'.format(
                    field=field, name=self.__class__.__name__
                ))
            setattr(self, field, kwargs[field])

        self.key_name = self.key()

    def __str__(self):
        return self.key_name

    def key(self):
        raise NotImplementedError()
