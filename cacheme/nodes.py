import pickle
from cacheme import utils, settings

tags = dict()


class NodeManager(object):
    connection = None
    _initialized = False

    def __init__(self, node_class):
        self.node_class = node_class
        self.utils = utils.CachemeUtils(self.connection)

    def invalid(self, **kwargs):
        if not kwargs:
            iterator = self.connection.sscan_iter(
                settings.REDIS_CACHE_PREFIX + self.node_class.__name__,
                count=settings.REDIS_CACHE_SCAN_COUNT
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
        self.utils = utils.CachemeUtils(self.connection)

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

        if 'Meta' in attrs:
            attrs['_raw_meta'] = attrs.pop('Meta')

        node_class = super().__new__(cls, name, bases, attrs)
        node_class.meta = dict()
        if getattr(node_class, '_raw_meta', False) and getattr(node_class, '_meta_fields', False):
            for k, v in node_class._raw_meta.__dict__.items():
                if k in node_class._meta_fields:
                    node_class.meta[k] = v

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
        self.key_name = settings.REDIS_CACHE_PREFIX + self.key()

    def key(self):
        raise NotImplementedError()

    def __str__(self):
        return self.key_name

    def invalid_nodes(self):
        return None

    def hit(self, key, result):
        pass

    def miss(self, key):
        pass


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
            prefix=settings.REDIS_CACHE_PREFIX,
            key=self.raw_key_name,
            suffix=':invalid')

    def __str__(self):
        return self.raw_key_name

    def key(self):
        raise NotImplementedError()
