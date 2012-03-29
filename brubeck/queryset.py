from request_handling import FourOhFourException

    
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
            shield = self.db_conn[item_id]
            del self.db_conn[item_id]
        except KeyError:
            raise FourOhFourException

        return (self.MSG_UPDATED, {'_id':item_id})

    def destroy_many(self, ids):
        statuses = [self.destroy_one(iid) for iid in ids]
        return statuses

from pymongo.errors import OperationFailure
    
class MongoQueryset(AbstractQueryset):
    """ A Queryset that deals with talking to mongodb"""

    def __init__(self, db_conn=None, api_id='_id'):
        """ A mongoqueryset takes a collection as it's fundamental unit."""
        self.collection = db_conn
        self.api_id=api_id
        
    def _get_id(self, shield):
        getattr(shield, self.api_id)

    def read_all(self):
        return [(self.MSG_OK, datum) for datum in self.collection.find({})]

    def read_one(self, idd):
        match = self.collection.find_one({self.api_id:idd})
        if not match:
            raise FourOhFourException
        return (self.MSG_OK, match)

    def read_many(self, ids):
        import pdb
        pdb.set_trace()
        cursor = self.collection.find({self.api_id:{"$in":ids}})
        matches = list(cursor)
        if len(matches) != len(ids):
            raise FourOhFourException
        return [(self.MSG_OK, datum) for datum in matches]
                
    def create_one(self, shield):
        try:
             self.collection.insert(shield.to_python(),
                                    safe=True,
                                    check_keys=True)
             return (self.MSG_CREATED, shield)
        except OperationFailure as e:
            return (self.MSG_FAILED, e) ## should we return the error here?
    
    def create_many(self, shields):
        import pdb
        pdb.set_trace()
        try:
            self.collection.insert(shield.to_python() for shield in shields)
            return [(self.MSG_CREATED, shield) for shield in shields]
        except OperationFailure as e:
            return (self.MSG_FAILED, e)

    def update_one(self, shield):
        try:
            self.collection.update(shield.to_python())
            return (self.MSG_UPDATED, shield)
        except OperationFailure as e:
            return (self.MSG_FAILED, e)
        
    def update_many(self, shields):
        try:
            ret = []
            for shield in shields:
                idd = self._get_id(shield)
                self.collection.update({self.api_id:idd}, shield.to_python())
                ret.append((self.MSG_UPDATED, shield))
            return ret
        except OperationFailure as e:
            return (self.MSG_FAILED, e)
    
    def destroy_one(self, item_id):
        self.read_one(item_id) #to check for 404
        try: 
            self.collection.remove({self.api_id:item_id}, safe=True)
            return (self.MSG_UPDATED, {'_id':item_id})
        except OperationFailure as e:
            return (self.MSG_FAILED, e)
        
    def destroy_many(self, item_ids):
        self.read_many(item_ids) #to check for 404
        try:
            self.collection.remove({self.api_id : {"$in" :item_ids}})
            return [(self.MSG_UPDATED, {'_id':item_id}) for item_id in item_ids]
        except OperationFailure as e:
            return (self.MSG_FAILED, e)


