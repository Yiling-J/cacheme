import pickle
from cacheme import utils

tags = dict()
CACHEME = None


class NodeManager(object):
    connection = None
    _initialized = False

    def __init__(self, node_class):
        self.node_class = node_class
        self.utils = utils.CachemeUtils(CACHEME, self.connection)

    def invalid(self, **kwargs):
        if not kwargs:
            iterator = self.connection.sscan_iter(
                CACHEME.REDIS_CACHE_PREFIX + self.node_class.__name__,
                count=CACHEME.REDIS_CACHE_SCAN_COUNT
            )
            return self.utils.invalid_iter(iterator)

        node = self.node_class(**kwargs)
        result = self.utils.invalid_key(
            node.key_name
        )
        return result

    def get(self, **kwargs):
        node = self.node_class(**kwargs)
        key, field = self.utils.split_key(node.key_name)
        result = self.connection.hget(key, field)
        if not result:
            raise Exception(
                'Node {node} does not exist'.format(node=node)
            )
        return pickle.loads(result)


class InvalidNodeManager(NodeManager):

    def __init__(self, node_cls):
        self.node_cls = node_cls
        self.utils = utils.CachemeUtils(CACHEME, self.connection)

    def invalid(self, **kwargs):
        node = self.node_cls(**kwargs)
        invalid_key = node.key_name
        iterator = self.connection.sscan_iter(invalid_key)
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

        meta = dict()
        if 'Meta' in attrs and '_meta_fields' in attrs:
            for k, v in attrs.pop('Meta').__dict__.items():
                if k in attrs['_meta_fields']:
                    meta[k] = v

        attrs['meta'] = meta

        node_class = super().__new__(cls, name, bases, attrs)
        if node_class.manager._initialized:
            node_class.objects = node_class.manager(node_class)
        node_class._update_class()
        return node_class

    def _update_class(cls):
        pass


class Node(object, metaclass=NodeMetaClass):
    manager = NodeManager
    _meta_fields = ('timeout', 'stale')

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
        self.key_name = CACHEME.REDIS_CACHE_PREFIX + self.key()

    def key(self):
        raise NotImplementedError()

    def __str__(self):
        return self.key_name

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

        self.raw_key_name = self.key()

        self.key_name = '{prefix}{key}{suffix}'.format(
            prefix=CACHEME.REDIS_CACHE_PREFIX,
            key=self.raw_key_name,
            suffix=':invalid')

    def __str__(self):
        return self.raw_key_name

    def key(self):
        raise NotImplementedError()
