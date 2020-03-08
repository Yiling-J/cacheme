[![Build Status](https://travis-ci.com/Yiling-J/cacheme.svg?branch=master)](https://travis-ci.com/Yiling-J/cacheme)
[![Build Status](https://codecov.io/gh/Yiling-J/cacheme/branch/master/graph/badge.svg)](https://codecov.io/gh/Yiling-J/cacheme)
# Cacheme

A memoized/cache decorator for Python using redis.

If you use Django, try [Django-Cacheme](https://github.com/Yiling-J/django-cacheme)


## Features

* **[Dynamic key based on args/kwargs](#dynamic-key-based-on-argskwargs)**

* **[Node for better management](#node-for-better-management)**

* **[Avoid thundering herd using stale data](#avoid-thundering-herd-using-stale-data)**

* **[Skip cache based on args/kwargs](#skip-cache-based-on-argskwargs)**

* **[Invalid all keys for tag](#invalid-all-keys-for-tag)**

* **[Hit/miss function support](#hitmiss-function-support)**

* **[Timeout(ttl) support](#timeoutttl-support)**

## Why Cacheme

For complicated page or API, you may need to fetch data from a variety of sources such as MySQL databases,
HDFS installations, some machine learning engines or your backend services.
This heterogeneity requires a flexible caching strategy able to store data from disparate sources.
And cacheme or memoize can help you.

## Getting started

`pip install cacheme`

Find a good place to init cacheme globally, for example `foobar_cache.py`

Or if you use node, create a package for better organization:
```
\your_cache_package_name
    __init__.py
    cache.py
    nodes.py
    invalid_nodes.py
```

Initialize cacheme in your `foobar_cache.py` (or `cache.py` in package)

```
import redis
from cacheme import cacheme

r = redis.Redis()

settings = {
    'ENABLE_CACHE': True,
    'REDIS_CACHE_ALIAS': 'cacheme',  # your CACHES alias name in settings, optional, 'default' as default
    'REDIS_CACHE_PREFIX': 'MYCACHE:',  # cacheme key prefix, optional, 'CM:' as default
    'THUNDERING_HERD_RETRY_COUNT': 5,  # thundering herd retry count, if key missing, default 5
    'THUNDERING_HERD_RETRY_TIME': 20  # thundering herd wait time(millisecond) between each retry, default 20
}

cacheme.set_connection(r)
cacheme.update_settings(settings)
```
Then in your project, when you need cacheme, just:

```
from foobar_cache import cacheme

# using node
# from your_cache_package_name.cache import cacheme
```

## Feature detail

#### Dynamic key based on args/kwargs

```
@cacheme(key=lambda c: 'cat:{name}'.format(name=c.cat.name))
def get_cat(self, cat):
    return some_function(cat)
```
This is how cacheme create key using lambda, the `c` in the lambda contains all args/kwargs of
decorated function.

#### Node for better management

```
@cacheme(node=lambda c: CatNode(cat=c.cat))
def get_cat(self, cat):
    return some_function(cat)
```
Node give you a generic way to manage you cache. Different from key, node use a predefined
node class. In this way, you can make cache reusable. [Detail](#--declaring-node-and-invalidnode)

#### Avoid thundering herd using stale data
How cacheme avoid thundering herds: if there is stale data, use stale data until new data fill in, if there is no stale data, just wait a short time and retry.

#### Skip cache based on args/kwargs
```
@cacheme(
    node=lambda c: CatNode(cat=c.cat),
    skip=lambda c: c.cat is None
)
def get_cat(self, cat):
    if not cat:
        return None
    return some_function(cat)
```
If skip is true, will skip the whole cache part, and get result dierctly from function.

#### Invalid all keys for tag
```
@cacheme(
    key=lambda c: 'cat:{name}'.format(name=c.cat.name),
    tag='cats'
)
def get_cat(self, cat):
    return some_function(cat)
```
After define tags, you can use tag like this:
```
instance = cacheme.tags['cats']
  
# invalid all keys
instance.invalid_all()
```
If you use node mode, tag will be node class name. Invalid will delete keys directly, no stale data.

#### Hit/miss function support
```
@cacheme(
    key=lambda c: 'cat:{name}'.format(name=c.cat.name),
    hit=lambda key, result, c: do_something,
    miss=lambda key, c: do_something
)
```
Just hit/miss callback

#### Timeout(ttl) support
```
@cacheme(
    key=lambda c: 'cat:{name}'.format(name=c.cat.name),
    timeout=300
)
def get_cat(self, cat):
    return some_function(cat)
```
set ttl for your cache, in seconds.


## Example


serializers.py

```
from foobar_cache import cacheme


class BookSerializer(object):

    @cacheme(
        key=lambda c: c.obj.cache_key + ">" + "owner",
        invalid_keys=lambda c: [c.obj.owner.cache_key]
    )
    def get_owner(self, obj):
        return BookOwnerSerializer(obj.owner).data	
```

We have a book, id is 100, and a user, id is 200. And we want to cache
book owner data in serializer. So the cache key will be `Book:100>owner`, "Book:100" as key, and
"owner" as field in redis.

Invalid key will be `User:200:invalid`, the ":invalid" suffix is auto added. And the redis data type
of this key is set. The `Book:100>owner` key will be stored under this invalid key.

## How to use

#### - Cacheme Decorator

Cacheme need following params when init the decorator.

* `key`: Callable. Function to generate the cache key.

* `node`: Callable, return a cahche node. `key` and `invalid_keys` will be ignored.

* `invalid_keys`: Callable or None, default None. an invalid key that will store this key, use redis set,
and the key func before will be stored in this invalid key.

* `hit`: callback when cache hit, need 3 arguments `(key, result, container)`

* `miss`: callback when cache miss, need 2 arguments `(key, container)`

* `tag`: string, default func name(node class name if using node). 
Using tag to get cache instance, then get all keys under that tag.

* `skip`: boolean or callable, default False. If value or callable value return true, will skip cache. For example,
you can cache result if request param has user, but return None directly, if no user.

* `timeout`: set ttl for this key, default `None`

* `invalid_sources`: something cause invalidation (for example Django/Flask signals). To use this, 
You need to override `connect(self, source)` in your cache class.

#### - Invalidation

You can create invalidation using following method:

```
cacheme.create_invalidation(key=None, invalid_key=None, pattern=None)
```

`create_invalidation` support 3 types of invalidation:

* `key`: invalid one key

* `invalid_key`: same as decorator, will invalid all keys saved in this key

* `pattern`: invalid a redis pattern, for example `test*`

Default for all 3 types are `None`, and you can use them together

#### - Declaring Node and InvalidNode

Declaring a node is very simlpe:

```
from cacheme.nodes import Node, Field
from my_cache_package import invalid_nodes


class TestNode(Node):
    id = Field()

    def key(self):
        return 'test:{id}'.format(id=self.id)

    def invalid_nodes(self):
        return invalid_nodes.InvalidNode(id=self.id)
```

You need to add all fields needed in `key()` and `invalid_nodes()` as attributes, and implement `key()`
method. `invalid_nodes()` method is optional.

All fields you add in Node class will be **required** kwargs when using node in cacheme decorator.
For example if your node has 3 fields:

```
class TestNode(Node):
    id = Field()
    user = Field()
    address = Field()
```
Then in cacheme all 3 are required
```
@cacheme(
    node=lambda c: TestNode(id=c.id, user=c.user, address=c.extra['address'])
)
def function(id, user, extra):
...
```
We still use lambda for node, this is the only way to let node get parameters from function args/kwargs.

Invalid node is similar:
```
from cacheme.nodes import InvalidNode, Field


class TestInvalidNode(Node):
    id = Field()

    def key(self):
        return 'test:{id}'.format(id=self.id)

```

#### - Invalid from Node
```
from my_cache import nodes


# invalid keys create by *Node Class*
nodes.TestNode.objects.invalid()

# invalid keys store in *a single invalid node*
nodes.TestInvalidNode.objects.invalid(id=123)
```
Invalid will delete keys directly, no stale data.

#### - Get a single node value
```
from my_cache import nodes


value = nodes.TestNode.objects.get(id=123)

```

## Tips:

* key and invalid_keys callable: the first argument in the callable is the container, this container
contains the args and kwargs for you function. For example, if your function is `def func(a, b, **kwargs)`,
then you can access `a` and `b` in your callable by `container.a`, `container.b`, also `container.kwargs`.

* For invalid_keys callable, you can aslo get your function result through `container.cacheme_result`, so you can invalid based on this result.

* if code is changed, developer should check if cache should invalid or not, for example you add some
fields to json, then cache for that json should be invalid, there is no signal for this, so do it manually

* For keys with timeout set, because cacheme store k/v using hash, we also store timeout in another redis sorted set

* There is another thing you can do to avoid thundering herds, if you use cacheme in a class, for example a `Serializer`,
and cache many methods in this class, and, order of these methods does not matter. Then you can make the order of call to theses methods randomly.
For example, if your class has 10 cached methods, and 100 clients call this method same time, then some clients will call method1 first, some will call method2 first..., so they can run in parallel.
