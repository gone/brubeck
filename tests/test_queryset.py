#!/usr/bin/env python

import unittest

from brubeck.queryset import DictQueryset, AbstractQueryset, MongoQueryset

from dictshield.document import Document
from dictshield.fields import StringField

import pymongo


##TestDocument
class TestDoc(Document):
    id = StringField(required=True, id_field=True)
    data = StringField()

###
### Tests for ensuring that the autoapi returns good data
###
class TestQuerySetPrimitives(unittest.TestCase):
    """
    a test class for brubeck's queryset objects' core operations.
    """

    def setUp(self):
        self.queryset = AbstractQueryset()


    def create(self):
        pass

    def read(self):
        pass

    def update(self):
        pass

    def destroy(self):
       pass


class TestQueryset(object):
    """
    a test class for brubeck's dictqueryset's operations.
    """

    def seed_reads(self):
        shields = [TestDoc(id="foo"), TestDoc(id="bar"), TestDoc(id="baz")]
        self.queryset.create_many(shields)
        return shields

    def test__create_one(self):
        shield = TestDoc(id="foo")
        status, return_shield = self.queryset.create_one(shield)
        self.assertEqual(self.queryset.MSG_CREATED, status)
        self.assertEqual(shield, return_shield)

        status, return_shield = self.queryset.create_one(shield)
        self.assertEqual(self.queryset.MSG_UPDATED, status)


    def test__create_many(self):
        shield0 = TestDoc(id="foo")
        shield1 = TestDoc(id="bar")
        shield2 = TestDoc(id="baz")
        statuses = self.queryset.create_many([shield0, shield1, shield2])
        for status, datum in statuses:
            self.assertEqual(self.queryset.MSG_CREATED, status)

        shield3 = TestDoc(id="bloop")
        statuses = self.queryset.create_many([shield0, shield3, shield2])
        status, datum = statuses[1]
        self.assertEqual(self.queryset.MSG_CREATED, status)
        status, datum = statuses[0]
        self.assertEqual(self.queryset.MSG_UPDATED, status)

    def test__read_all(self):
        shields = self.seed_reads()
        statuses = self.queryset.read_all()

        for status, datum in statuses:
            self.assertEqual(self.queryset.MSG_OK, status)

        actual = sorted([datum for trash, datum in statuses])
        expected = sorted([shield.to_python() for shield in shields])
        self.assertEqual(expected, actual)

    def test__read_one(self):
        shields = self.seed_reads()
        for shield in shields:
            status, datum = self.queryset.read_one(shield.id)
            self.assertEqual(self.queryset.MSG_OK, status)
            self.assertEqual(datum, shield.to_python())
        bad_key = 'DOESNTEXISIT'
        status, datum = self.queryset.read(bad_key)
        self.assertEqual(bad_key, datum)
        self.assertEqual(self.queryset.MSG_FAILED, status)

    def test__read_many(self):
        shields = self.seed_reads()
        expected = [shield.to_python() for shield in shields]
        responses = self.queryset.read_many([s.id for s in shields])
        for status, datum in responses:
            self.assertEqual(self.queryset.MSG_OK, status)
            self.assertTrue(datum in expected)

        bad_ids = [s.id for s in shields]
        bad_ids.append('DOESNTEXISIT')
        status, iid = self.queryset.read_many(bad_ids)[-1]
        self.assertEqual(self.queryset.MSG_FAILED, status)


    def test_update_one(self):
        shields = self.seed_reads()
        test_shield = shields[0]
        test_shield.data = "foob"
        status, datum = self.queryset.update_one(test_shield)

        self.assertEqual(self.queryset.MSG_UPDATED, status)
        self.assertEqual('foob', datum['data'])

        status, datum =  self.queryset.read_one(test_shield.id)
        self.assertEqual('foob', datum['data'])


    def test_update_many(self):
        shields = self.seed_reads()
        for shield in shields:
            shield.data = "foob"
        responses = self.queryset.update_many(shields)
        for status, datum in responses:
            self.assertEqual(self.queryset.MSG_UPDATED, status)
            self.assertEqual('foob', datum['data'])
        for status, datum in self.queryset.read_all():
            self.assertEqual('foob', datum['data'])


    def test_destroy_one(self):
        shields = self.seed_reads()
        test_shield = shields[0]
        status, datum = self.queryset.destroy_one(test_shield.id)
        self.assertEqual(self.queryset.MSG_UPDATED, status)

        status, datum = self.queryset.read_one(test_shield.id)
        self.assertEqual(test_shield.id, datum)
        self.assertEqual(self.queryset.MSG_FAILED, status)


    def test_destroy_many(self):
        shields = self.seed_reads()
        shield_to_keep = shields.pop()
        responses = self.queryset.destroy_many([shield.id for shield in shields])
        for status, datum in responses:
            self.assertEqual(self.queryset.MSG_UPDATED, status)

        responses = self.queryset.read_many([shield.id for shield in shields])
        for status, datum in responses:
            self.assertEqual(self.queryset.MSG_FAILED, status)

        status, datum = self.queryset.read_one(shield_to_keep.id)
        self.assertEqual(self.queryset.MSG_OK, status)
        self.assertEqual(shield_to_keep.to_python(), datum)


class TestDictQueryset(TestQueryset, unittest.TestCase):
    def setUp(self):
        self.queryset = DictQueryset()


class TestMongoQueryset(TestQueryset, unittest.TestCase):
    db_name = "brubecktest"
    collection_name = 'test_db'
    #TODO: alert if this already exisits. dropping someone's data would be a bummer!

    def setUp(self):
        self.connection = pymongo.connection.Connection()
        database = getattr(self.connection, self.db_name)
        collection = getattr(database, self.collection_name)
        self.queryset = MongoQueryset(collection, api_id='id')

    def tearDown(self):
        self.connection.drop_database(self.db_name)


##
## This will run our tests
##
if __name__ == '__main__':
    unittest.main()
