from bson.dbref import DBRef

from openspending.model import (Changeset, ChangeObject, Classifier, Dataset,
                                Entry, Entity, mongo)

from openspending.etl import loader
from openspending.etl import util
from openspending.etl.test import LoaderTestCase, helpers as h

test_data = {
    (u'red', u'flowery'): 30.0,
    (u'red', u'pungent'): 30.0,
    (u'red', None): 30.0,
    (u'green', u'flowery'): 30.0,
    (u'green', u'pungent'): 30.0,
}


class TestLoader(LoaderTestCase):

    def _get_index_num(self, cls):
        return len(cls.c.index_information())

    def test_create_loader(self):
        loader = self._make_loader()
        h.assert_true(isinstance(loader.dataset, Dataset))
        h.assert_equal(loader.dataset.name, u'test_dataset')
        h.assert_equal(loader.num_entries, 0)

    @h.raises(loader.LoaderSetupError)
    def test_loader_raises_if_multiple_datasets(self):
        Dataset(name="foo").save()
        Dataset(name="foo").save()
        self._make_loader(dataset_name="foo")

    @h.raises(loader.LoaderSetupError)
    def test_loader_raises_if_no_unique_keys(self):
        self._make_loader(unique_keys=[])

    def test_loader_sorts_unique_keys(self):
        ldr = self._make_loader(unique_keys=['c', 'a', 'b', 'z'])
        h.assert_equal(ldr.unique_keys, ['a', 'b', 'c', 'z'])

    def test_loader_creates_indexes(self):
        db = mongo.db()
        db.create_collection('entry')
        db.create_collection('entity')
        h.assert_equal(self._get_index_num(Entry), 1)
        h.assert_equal(self._get_index_num(Entity), 1)

        self._make_loader()
        h.assert_equal(self._get_index_num(Entry), 9)
        h.assert_equal(self._get_index_num(Entity), 2)

    def test_loader_checks_duplicate_entries(self):
        loader = self._make_loader(unique_keys=['name'])
        self._make_entry(loader, name=u'Test Entry')

        second_entry = Entry(name=u'Test Entry',
                             dataset=loader.dataset.to_ref_dict())
        second_entry.save()
        h.assert_raises(ValueError, self._make_loader, unique_keys=['name'])

    def test_loader_checks_duplicate_entities(self):
        Entity(name=u'Test Entity').save()
        Entity(name=u'Test Entity').save()
        h.assert_raises(ValueError, self._make_loader, unique_keys=['name'])

    # Create Entries

    def test_create_entry(self):
        loader = self._make_loader()
        entry = self._make_entry(loader)
        h.assert_true(isinstance(entry, Entry))

    def test_create_entry_id_from_unique_keys(self):
        loader = self._make_loader(dataset_name="monkeys",
                                   unique_keys=['a', 'b', 'c'])
        entry = self._make_entry(loader, a="foo", b="bar", c="baz")

        h.assert_equal(entry.id, util.hash_values(['monkeys', 'foo', 'bar', 'baz']))

    def test_create_entry_returns_query_spec(self):
        loader = self._make_loader()
        entry = {'name': 'one',
                 'amount': 1000.00,
                 'from': loader.create_entity(u'From Entity'),
                 'to': loader.create_entity(u'To Entity'),
                 'first': u'first',
                 'second': u'second',
                 'extra': u'extra'}
        query_spec = loader.create_entry(**entry)
        fetched_entry = Entry.find_one(query_spec)
        h.assert_equal(fetched_entry['name'], 'one')

    def test_create_entry_does_not_delete_attributes_in_existing(self):
        loader = self._make_loader()
        entry = self._make_entry(loader, extra=u'extra')
        h.assert_true('extra' in entry)
        new_loader = self._make_loader()
        upserted_entry = self._make_entry(new_loader, extra=u'extra')
        h.assert_true('extra' in upserted_entry)

    def test_create_entry_creates_entities(self):
        loader = self._make_loader()
        special_entity = loader.create_entity(name='special')
        testentry = {'name': 'testentry'}
        loader.entitify_entry(testentry, special_entity, 'special')
        h.assert_true('special' in testentry)
        h.assert_equal(len(testentry['entities']), 1)
        h.assert_equal(special_entity['_id'], testentry['entities'][0])
        created = self._make_entry(loader, **testentry)
        h.assert_equal(len(created['entities']), 3)

    def test_currency_when_create_entry(self):
        loader = self._make_loader(currency=u'Default')
        entry = self._make_entry(loader)
        h.assert_equal(entry['currency'], u'DEFAULT')

        entry = self._make_entry(loader, name='Other Entry', currency=u'other')
        h.assert_equal(entry['currency'], u'OTHER')

    def test_entry_assertions(self):
        loader = self._make_loader(unique_keys=['first', 'second'])

        def entry(miss=None, **kwargs):
            entry = {
                'amount': 1000.00,
                'from': loader.create_entity(u'From Entity'),
                'to': loader.create_entity(u'To Entity'),
                'first': u'first',
                'second': u'second',
                'extra': u'extra'
            }

            entry.update(kwargs)
            if miss is not None:
                del entry[miss]

            return entry

        h.assert_raises(AssertionError, loader.create_entry,
                          **entry(miss='amount'))
        h.assert_raises(AssertionError, loader.create_entry,
                          **entry(miss='to'))
        h.assert_raises(AssertionError, loader.create_entry,
                          **entry(miss='from'))
        h.assert_raises(AssertionError, loader.create_entry,
                          **entry(to=u'no_entity'))
        h.assert_raises(AssertionError, loader.create_entry,
                          **entry(**{'from': u'no_entity'}))
        h.assert_raises(KeyError, loader.create_entry,
                          **entry(miss='first'))
        h.assert_raises(KeyError, loader.create_entry,
                          **entry(miss='second'))

    # Create Entities

    def test_create_entity(self):
        loader = self._make_loader()
        entity = loader.create_entity(name=u'Test Entity')
        h.assert_true(isinstance(entity, Entity))

    def test_create_entry_with_different_match_keys(self):
        loader = self._make_loader()
        loader.create_entity(name=u'Test', company_id=1000,
                             match_keys=('company_id',))
        h.assert_equal(len(loader.entity_cache[('company_id',)]), 1)
        h.assert_true(Entity.find_one({'company_id': 1000}) is not None)

    def test_create_finds_existing_entity_in_db(self):
        Entity.c.save({'name': 'existing', 'company_id': 1000})
        existing = Entity.find_one({'company_id': 1000})

        loader = self._make_loader()
        loader.create_entity(name=u'Test', company_id=1000,
                             match_keys=('company_id',))
        cached = loader.entity_cache[('company_id',)][(1000,)]

        h.assert_equal(existing['_id'], cached['_id'])
        h.assert_equal(Entity.find({'company_id': 1000}).count(), 1)

    def test_create_entity_does_not_delete_attributes_in_existing(self):
        loader = self._make_loader()
        entity = loader.create_entity(u'Test Entity', extra=u'extra')
        h.assert_true('extra' in entity)
        new_loader = self._make_loader()
        upserted_entity = new_loader.create_entity(u'Test Entity')
        h.assert_true('extra' in upserted_entity)

    def test_entities_are_cached(self):
        loader = self._make_loader()
        entity = loader.create_entity(name=u'Test Entity')
        h.assert_true(entity is loader.entity_cache[('name',)].values()[0])

    def test_entities_cached_with_passed_in_cached(self):
        loader = self._make_loader()
        cache = {('name',): {}}
        entity = loader.create_entity(name=u'Test Entity', _cache=cache)
        h.assert_true(entity is cache[('name', )].values()[0])
        h.assert_equal(len(loader.entity_cache), 0)

    # Create Classifiers and classify Entries

    def test_create_classifier(self):
        loader = self._make_loader()
        classifier = loader.create_classifier(name=u'Test Classifier',
                                              taxonomy=u'taxonomy')
        h.assert_true(isinstance(classifier, Classifier))

    def test_create_classifier_does_not_delete_attributes_in_existing(self):
        loader = self._make_loader()
        classifier = loader.create_classifier(u'Test Classifier',
                                              taxonomy=u'taxonomy',
                                              extra=u'extra')
        h.assert_true('extra' in classifier)
        new_loader = self._make_loader()
        upserted_classifier = new_loader.create_classifier(
            u'Test Classifier', taxonomy=u'taxonomy')
        h.assert_true('extra' in upserted_classifier)

    def test_classifiers_are_cached(self):
        loader = self._make_loader()
        classifier = loader.create_classifier(name=u'Test Classifier',
                                              taxonomy=u'taxonomy')
        h.assert_equal(loader.classifier_cache.values()[0], classifier)

    def test_classifiers_cached_with_passed_in_cache(self):
        loader = self._make_loader()
        cache = {}
        classifier = loader.create_classifier(name=u'Test Classifier',
                                              taxonomy=u'taxonomy',
                                              _cache=cache)
        h.assert_true(classifier is cache.values()[0])
        h.assert_equal(len(loader.classifier_cache), 0)

    def test_classify_entry(self):
        loader = self._make_loader()
        entry = {'name': u'Test Entry',
                 'amount': 1000.00}
        c_name = u'support-transparency'
        c_taxonomy = u'Good Reasons'
        c_label = u'Support Transparency Initiatives'
        classifier = loader.create_classifier(name=c_name,
                                              label=c_label,
                                              taxonomy=c_taxonomy)
        loader.classify_entry(entry, classifier, name=u'reason')
        h.assert_equal(entry.keys(), [u'reason', 'amount', 'name',
                                        'classifiers'])
        h.assert_equal(entry['classifiers'], [classifier['_id']])
        h.assert_equal(entry['reason']['label'], c_label)
        h.assert_equal(entry['reason']['name'], c_name)
        h.assert_equal(entry['reason']['taxonomy'], c_taxonomy)
        h.assert_true(isinstance(entry['reason']['ref'], DBRef))

    # Changeset / Revisioning support

    def test_loader_creates_changeset(self):
        self._make_loader()
        h.assert_equal(Changeset.c.find().count(), 1)

    def _find_changeobject(self, collection, _id):
        return ChangeObject.find_one({'object_id': [collection, _id]})

    def test_loader_creates_changeobject_for_entry(self):
        loader = self._make_loader()
        entry = self._make_entry(loader, name=u'revisioned_testentry')
        changeobj = self._find_changeobject('entry', entry.id)
        h.assert_equal(changeobj['changeset']['_id'], loader.changeset.id)
        h.assert_equal(changeobj['data']['name'], entry['name'])

    def test_loader_creates_changeobject_for_entities(self):
        loader = self._make_loader()
        entity = loader.create_entity(u'Test Entity')
        changeobj = self._find_changeobject('entity', entity.id)
        h.assert_equal(changeobj['changeset']['_id'],
                         loader.changeset.id)
        h.assert_equal(changeobj['data']['name'], entity['name'])

    def test_loader_creates_changeobject_for_classifiers(self):
        loader = self._make_loader()
        classifier = loader.create_classifier(u'testclassifier',
                                              u'testtaxonomy')
        changeobj = self._find_changeobject('classifier', classifier.id)
        h.assert_equal(changeobj['changeset']['_id'],
                         loader.changeset.id)
        h.assert_equal(changeobj['data']['name'], classifier['name'])

    def test_loader_creates_changeobject_for_dataset(self):
        loader = self._make_loader()
        changeobj = self._find_changeobject('dataset', loader.dataset.id)
        h.assert_equal(changeobj['changeset']['_id'],
                         loader.changeset.id)
        h.assert_equal(changeobj['data']['name'], loader.dataset['name'])

    # Other stuff

    # Fixme: create_view tests.