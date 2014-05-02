from __future__ import absolute_import

from contextlib import contextmanager

from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel
from google.appengine.datastore.datastore_query import Cursor

from neodb import keygen
from neodb import mutex
from neodb.plurals import singularize


def _list_or_args(items):
    if len(items) == 1 and hasattr(items[0], '__iter__'):
        return items[0]
    return items


def as_key(item):
    if item is None:
        return None
    return item if isinstance(item, ndb.Key) else item.key


def as_keys(*items):
    return [ as_key(item) for item in _list_or_args(items) ]


def as_id(item):
    if item is None:
        return None
    if isinstance(item, (basestring, int, long)):
        return item
    return as_key(item).id()


def as_ids(*items):
    return [as_id(item) for item in _list_or_args(items)]


def as_model(item, **options):
    if item is None:
        return None
    if options.pop('async', False):
        return item.get_async() if isinstance(item, ndb.Key) else item
    else:
        return item.get() if isinstance(item, ndb.Key) else item


def as_models(*items):
    results = [ as_model(item, async=True) for item in _list_or_args(items) ]
    for index, result in enumerate(results):
        if isinstance(result, ndb.Future):
            results[index] = result.get_result()
    return results


def as_path(item, separator=':'):
    if item is None:
        return None
    return separator.join([ str(it) for it in as_key(item).flat() ])


@contextmanager
def lock(item, **kwargs):
    with mutex.lock(as_path(item), **kwargs):
        yield


def put(*entities, **options):
    entities = _list_or_args(entities)
    async = options.pop('async', False)
    delegate = ndb.put_multi_async if async else ndb.put_multi
    return delegate(entities, **options)


def get(*items, **options):
    keys = as_keys(_list_or_args(items))
    async = options.pop('async', False)
    delegate = ndb.get_multi_async if async else ndb.get_multi
    return delegate(keys, **options)


def delete(*items, **options):
    keys = as_keys(_list_or_args(items))
    async = options.pop('async', False)
    delegate = ndb.delete_multi_async if async else ndb.delete_multi
    return delegate(keys, **options)


def batch_delete(query, batch_size=1000):
    while True:
        keys = query.fetch(batch_size, keys_only=True)
        if not len(keys):
            break
        delete(keys)


def cursor_for(websafe_string):
    if websafe_string is None:
        return None
    return Cursor.from_websafe_string(websafe_string)


def memoized(func):
    memo_prop = '_memo_for_' + func.__name__
    from functools import wraps
    @wraps(func)
    def decorate(target):
        if not hasattr(target, memo_prop):
            setattr(target, memo_prop, func(target))
        return getattr(target, memo_prop)
    return decorate


class ModelMixin(object):

    created = ndb.DateTimeProperty(auto_now_add=True, indexed=True)

    updated = ndb.DateTimeProperty(auto_now=True, indexed=True)

    def update_attributes(self, **kwargs):
        self.populate(**kwargs)
        self.put()
        return True

    def __getattr__(self, name):
        if name.endswith('_key') or name.endswith('_keys'):
            raise AttributeError("Unable to resolve property '%s'." % name)
        try:
            key_prop = name + '_key'
            key = getattr(self, key_prop)
            value = key and key.get()
            setattr(self, name, value)
            return value
        except AttributeError:
            try:
                keys_prop = singularize(name) + '_keys'
                keys = getattr(self, keys_prop)
                values = keys and get(keys)
                setattr(self, name, values)
                return values
            except AttributeError:
                raise AttributeError("Unable to resolve property '%s' of '%s' by delegating to '%s' or '%s'." % (name, self, key_prop, keys_prop))

    @property
    def id(self):
        return self.key.id()

    def kind(self):
        return self.key.kind()

    def delete(self):
        return self.key.delete()

    def delete_async(self):
        return self.key.delete_async()

    @classmethod
    def properties(cls):
        return { key: cls._properties[key] for key in cls._properties if key != 'class' }

    @classmethod
    def fields(cls):
        return cls.properties()

    @classmethod
    def for_id(cls, id, parent=None, key_only=False):
        if id is None:
            return None
        key = Key(pairs=[(cls, id)], parent=as_key(parent))
        if key_only:
            return key
        return key.get()

    @classmethod
    def empty_query(cls):
        return cls.query().filter(cls.key == Key(cls, 'this_id_should_not_be_found'))

    @classmethod
    def new(cls, *args, **kwargs):
        kwds = dict(id=gen_medium_key())
        kwds.update(kwargs)
        return cls(**kwds)

    @classmethod
    def create(cls, *args, **kwargs):
        it = cls.new(*args, **kwargs)
        it.put()
        return it

    @classmethod
    def for_id(cls, id, parent=None, use_cache=True):
        try:
            return ndb.Key(cls, id, parent=as_key(parent)).get(use_cache=use_cache) or \
                ndb.Key(cls, int(id), parent=as_key(parent)).get(use_cache=use_cache)
        except ValueError as e:
            return None


class Model(ndb.Model, ModelMixin):

    def _prepare_for_put(self):
        self._is_new = self.created is None
        super(Model, self)._prepare_for_put()

    def _post_put_hook(self, future):
        if getattr(self, '_is_new', False):
            self._is_new = False
            self._post_create_hook(future)
        else:
            self._post_update_hook(future)
        super(Model, self)._post_put_hook(future)

    def _post_create_hook(self, future):
        pass

    def _post_update_hook(self, future):
        pass


class PolyModel(polymodel.PolyModel, ModelMixin):

    def _prepare_for_put(self):
        self._is_new = self.created is None
        super(Model, self)._prepare_for_put()

    def _post_put_hook(self, future):
        if getattr(self, '_is_new', False):
            self._is_new = False
            self._post_create_hook(future)
        else:
            self._post_update_hook(future)
        super(Model, self)._post_put_hook(future)

    def _post_create_hook(self, future):
        pass

    def _post_update_hook(self, future):
        pass

    @classmethod
    def _get_hierarchy(cls):
        return polymodel.PolyModel._get_hierarchy.im_func(cls)[1:]


class Expando(ndb.Expando, ModelMixin):

    pass


class Migration(ndb.Expando, ModelMixin):

    @classmethod
    def traverse(cls, callable, query, batch_size=100):
        import sys
        def write(it):
            sys.stdout.write(it)
            sys.stdout.flush()
        write('|')
        cursor = None;
        while True:
            batch, cursor, more = query.fetch_page(batch_size, start_cursor=cursor)
            if not len(batch): break
            write('-')
            for it in batch:
                callable(it)
            write('-')
            put(batch)
            write('>')
        write('|\n')

    @classmethod
    def drop(cls, field):
        def _drop(it):
            if hasattr(it, field): delattr(it, field)
        cls.traverse(_drop, cls.query(GenericProperty(field) > None))

    @classmethod
    def rename(cls, field, to):
        def _rename(it):
            if hasattr(it, field):
                setattr(it, to, getattr(it, field))
                delattr(it, field)
        cls.traverse(_rename, cls.query(GenericProperty(field) > None))

    @classmethod
    def add(cls, field, default):
        def _add(it):
            if hasattr(it, field): return
            setattr(it, field, default(it) if callable(default) else default)
        cls.traverse(_add, cls.query())



########################################################
# ndb aliases
########################################################

IntegerProperty = ndb.IntegerProperty

FloatProperty = ndb.FloatProperty

BooleanProperty = ndb.BooleanProperty

StringProperty = ndb.StringProperty

TextProperty = ndb.TextProperty

BlobProperty = ndb.BlobProperty

DateTimeProperty = ndb.DateTimeProperty

DateProperty = ndb.DateProperty

TimeProperty = ndb.TimeProperty

GeoPt = ndb.GeoPt

GeoPtProperty = ndb.GeoPtProperty

KeyProperty = ndb.KeyProperty

BlobKeyProperty = ndb.BlobKeyProperty

UserProperty = ndb.UserProperty

StructuredProperty = ndb.StructuredProperty

LocalStructuredProperty = ndb.LocalStructuredProperty

JsonProperty = ndb.JsonProperty

PickleProperty = ndb.PickleProperty

GenericProperty = ndb.GenericProperty

ComputedProperty = ndb.ComputedProperty

Future = ndb.Future

Key = ndb.Key

Query = ndb.Query

AND = ndb.AND

OR = ndb.OR

transactional = ndb.transactional

transaction = ndb.transaction

in_transaction = ndb.in_transaction

tasklet = ndb.tasklet

Return = ndb.Return

########################################################
# keygen aliases
########################################################

gen_key = keygen.gen_key

gen_short_key = keygen.gen_short_key

gen_medium_key = keygen.gen_medium_key

gen_long_key = keygen.gen_long_key
