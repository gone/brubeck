from request_handling import FourOhFourException
from itertools import imap
import zlib
import ujson as json
try:
    import redis
except ImportError:
    pass

class AbstractQueryset(object):
    """The design of the `AbstractQueryset` attempts to map RESTful calls
    directly to CRUD calls. It also attempts to be compatible with a single
    item of a list of items, handling multiple statuses gracefully if
    necessary.

    The querysets then must allow for calls to perform typical CRUD operations
    on individual items or a list of items.

    By nature of being dependent on complete data models or ids, the system
    suggests users follow a key-value methodology. Brubeck believes this is
    what we scale into over time and should just build to this model from the
    start.

    Implementing the details of particular databases is then to implement the
    `create_one`, `create_many`, ..., for all the CRUD operations. MySQL,
    Mongo, Redis, etc should be easy to implement while providing everything
    necessary for a proper REST API.
    """

    MSG_OK = 'OK'
    MSG_UPDATED = 'Updated'
    MSG_CREATED = 'Created'
    MSG_NOTFOUND = 'Not Found'
    MSG_FAILED = 'Failed'

    def __init__(self, db_conn=None, api_id='id'):
        self.db_conn = db_conn
        self.api_id = api_id

    ###
    ### CRUD Operations
    ###

    ### Section TODO:
    ### * Pagination
    ### * Hook in authentication
    ### * Key filtering (owner / public)
    ### * Make model instantiation an option

    def create(self, shields):
        """Commits a list of new shields to the database
        """
        if isinstance(shields, list):
            return self.create_many(shields)
        else:
            return self.create_one(shields)

    def read(self, ids):
        """Returns a list of items that match ids
        """
        if not ids:
            return self.read_all()
        elif isinstance(ids, list):
            return self.read_many(ids)
        else:
            return self.read_one(ids)

    def update(self, shields):
        if isinstance(shields, list):
            return self.update_many(shields)
        else:
            return self.update_one(shields)

    def destroy(self, item_ids):
        """ Removes items from the datastore
        """
        if isinstance(item_ids, list):
            return self.destroy_many(item_ids)
        else:
            return self.destroy_one(item_ids)

    ###
    ### CRUD Implementations
    ###

    ### Create Functions

    def create_one(self, shield):
        raise NotImplementedError

    def create_many(self, shields):
        raise NotImplementedError

    ### Read Functions

    def read_all(self):
        """Returns a list of objects in the db
        """
        raise NotImplementedError

    def read_one(self, iid):
        """Returns a single item from the db
        """
        raise NotImplementedError

    def read_many(self, ids):
        """Returns a list of objects matching ids from the db
        """
        raise NotImplementedError

    ### Update Functions

    def update_one(self, shield):
        raise NotImplementedError

    def update_many(self, shields):
        raise NotImplementedError

    ### Destroy Functions

    def destroy_one(self, iid):
        raise NotImplementedError

    def destroy_many(self, ids):
        raise NotImplementedError


class DictQueryset(AbstractQueryset):
    """This class exists as an example of how one could implement a Queryset.
    This model is an in-memory dictionary and uses the model's id as the key.

    The data stored is the result of calling model's `to_python()` function.
    """
    def __init__(self, **kw):
        """Set the db_conn to a dictionary.
        """
        super(DictQueryset, self).__init__(db_conn=dict(), **kw)

    ### Create Functions

    def create_one(self, shield):
        if shield.id in self.db_conn:
            status = self.MSG_UPDATED
        else:
            status = self.MSG_CREATED

        shield_key = str(getattr(shield, self.api_id))
        self.db_conn[shield_key] = shield.to_python()
        return (status, shield)

    def create_many(self, shields):
        statuses = [self.create_one(shield) for shield in shields]
        return statuses

    ### Read Functions

    def read_all(self):
        return [(self.MSG_OK, datum) for datum in self.db_conn.values()]


    def read_one(self, iid):
        iid = str(iid)  # TODO Should be cleaner
        try:
            return (self.MSG_UPDATED, self.db_conn[iid])
        except KeyError:
            raise FourOhFourException

    def read_many(self, ids):
        return [self.read_one(iid) for iid in ids]

    ### Update Functions
    def update_one(self, shield):
        shield_key = str(getattr(shield, self.api_id))
        self.db_conn[shield_key] = shield.to_python()
        return (self.MSG_UPDATED, shield)

    def update_many(self, shields):
        statuses = [self.update_one(shield) for shield in shields]
        return statuses

    ### Destroy Functions

    def destroy_one(self, item_id):
        try:
            datum = self.db_conn[item_id]
            del self.db_conn[item_id]
        except KeyError:
            raise FourOhFourException
<<<<<<< HEAD

        return (self.MSG_UPDATED, shield)
=======
        return (self.MSG_UPDATED, datum)
>>>>>>> bb1c0fe8d9387d59e7ecdb4ceaa6cea4110fd904

    def destroy_many(self, ids):
        statuses = [self.destroy_one(iid) for iid in ids]
        return statuses

<<<<<<< HEAD
    
class MongoQueryset(AbstractQueryset):
    """ A Queryset that deals with talking to mongodb"""

    def __init__(self, db_conn=None, api_id='id'):
        """ A mongoqueryset takes a collection as it's fundamental unit."""
        self.collection = db_conn
        self.api_id=api_id
        
    def _get_id(self, shield):
        getattr(shield, shield._meta['id_field'])

    
    def read_all(self):
        return self.collection.find()

    def read_one(self, i):
        match = self.collection.find_one({i._meta['id_field']:self._get_id(i)})
        if not match:
            raise FourOhFourException
        return match

    def read_many(self, ids):
        matches = list(self.collection.find({self.id_field:{"$in":ids}}))
        if len(matches) != len(ids):
            raise FourOhFourException
        return matches
                
    def create_one(self, shield):
        return self.collection.insert(shield.to_python())
    
    def create_many(self, shields):
        created, updated = [], []
        for shield in shields:
            old = self.collection.find_and_modify({self.id_field:shield.get(self.id_field)}, shield.to_python(), upsert=True)
            if not old:
                created.append(shield)
        self.collection.insert(shield.to_python() for shield in shields)

        return created, updated, []
                
    def update_one(self, shield):
        return self.update_many([shield])

    def update_many(self, shields):
        for shield in shields:
            self.db_conn[str(getattr(shield, self.api_id))] = shield.to_python()
        return shields, []
    
    def destroy_one(self, item_id):
        return self.destroy_many([item_id])

    def destroy_many(self, item_ids):
        try:
            for i in item_ids:
                del self.db_conn[i]
        except KeyError:
            raise FourOhFourException
        return item_ids, []
=======
class RedisQueryset(AbstractQueryset):
    """This class uses redis to store the DictShield after 
    calling it's `to_json()` method. Upon reading from the Redis
    store, the object is deserialized using json.loads().

    Redis connection uses the redis-py api located here:
    https://github.com/andymccurdy/redis-py
    """
    # TODO: - catch connection exceptions?
    #       - set Redis EXPIRE and self.expires
    #       - confirm that the correct status is being returned in 
    #         each circumstance
    def __init__(self, compress=False, compress_level=1, **kw):
        """The Redis connection wiil be passed in **kw and is used below
        as self.db_conn.
        """
        super(RedisQueryset, self).__init__(**kw)
        self.compress = compress
        self.compress_level = compress_level
        
    def _setvalue(self, shield):
        if self.compress:
            return zlib.compress(shield.to_json(), self.compress_level)
        return shield.to_json()

    def _readvalue(self, value):
        if self.compress:
            try:
                compressed_value = zlib.decompress(value)
                return json.loads(zlib.decompress(value))
            except Exception as e:
                # value is 0 or None from a Redis return value
                return value
        if value:
            return json.loads(value)
        return None

    def _message_factory(self, fail_status, success_status):
        """A Redis command often returns some value or 0 after the
        operation has returned.
        """
        return lambda x: success_status if x else fail_status

    ### Create Functions

    def create_one(self, shield):
        shield_value = self._setvalue(shield)
        shield_key = str(getattr(shield, self.api_id))        
        result = self.db_conn.hset(self.api_id, shield_key, shield_value)
        if result:
            return (self.MSG_CREATED, shield)
        return (self.MSG_UPDATED, shield)

    def create_many(self, shields):
        message_handler = self._message_factory(self.MSG_UPDATED, self.MSG_CREATED)
        pipe = self.db_conn.pipeline()
        for shield in shields:
            pipe.hset(self.api_id, str(getattr(shield, self.api_id)), self._setvalue(shield))
        results = zip(imap(message_handler, pipe.execute()), shields)
        pipe.reset()
        return results
        
    ### Read Functions

    def read_all(self):
        return [(self.MSG_OK, self._readvalue(datum)) for datum in self.db_conn.hvals(self.api_id)]

    def read_one(self, shield_id):
        result = self.db_conn.hget(self.api_id, shield_id)
        if result:
            return (self.MSG_OK, self._readvalue(result))
        return (self.MSG_FAILED, shield_id)

    def read_many(self, shield_ids):
        message_handler = self._message_factory(self.MSG_FAILED, self.MSG_OK)
        pipe = self.db_conn.pipeline()
        for shield_id in shield_ids:
            pipe.hget(self.api_id, str(shield_id))
        results = pipe.execute()
        pipe.reset()
        return zip(imap(message_handler, results), map(self._readvalue, results))

    ### Update Functions

    def update_one(self, shield):
        shield_key = str(getattr(shield, self.api_id))
        message_handler = self._message_factory(self.MSG_UPDATED, self.MSG_CREATED)
        status = message_handler(self.db_conn.hset(self.api_id, shield_key, self._setvalue(shield)))
        return (status, shield)

    def update_many(self, shields):
        message_handler = self._message_factory(self.MSG_UPDATED, self.MSG_CREATED)
        pipe = self.db_conn.pipeline()
        for shield in shields:
            pipe.hset(self.api_id, str(getattr(shield, self.api_id)), self._setvalue(shield))
        results = pipe.execute()
        pipe.reset()
        return zip(imap(message_handler, results), shields)

    ### Destroy Functions

    def destroy_one(self, shield_id):
        pipe = self.db_conn.pipeline()
        pipe.hget(self.api_id, shield_id)
        pipe.hdel(self.api_id, shield_id)
        result = pipe.execute()
        if result[1]:
            return (self.MSG_UPDATED, self._readvalue(result[0]))
        return self.MSG_NOTFOUND

    def destroy_many(self, ids):
        # TODO: how to handle missing fields, currently returning self.MSG_FAILED
        message_handler = self._message_factory(self.MSG_FAILED, self.MSG_UPDATED)
        pipe = self.db_conn.pipeline()
        for _id in ids:
            pipe.hget(self.api_id, _id)
        values_results = pipe.execute()
        for _id in ids:
            pipe.hdel(self.api_id, _id)
        delete_results = pipe.execute()
        pipe.reset()
        return zip(imap(message_handler, delete_results), map(self._readvalue, values_results))
>>>>>>> bb1c0fe8d9387d59e7ecdb4ceaa6cea4110fd904

