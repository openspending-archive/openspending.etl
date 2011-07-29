from bson.dbref import DBRef

from openspending import mongo
from openspending import model
from openspending.model import Classifier, Dataset, Entity

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
    def test_loader_raises_if_no_unique_keys(self):
        self._make_loader(unique_keys=[])

    def test_loader_sorts_unique_keys(self):
        ldr = self._make_loader(unique_keys=['c', 'a', 'b', 'z'])
        h.assert_equal(ldr.unique_keys, ['a', 'b', 'c', 'z'])

    def test_loader_creates_indexes(self):
        mongo.db.create_collection('entry')
        mongo.db.create_collection('entity')
        h.assert_equal(len(mongo.db[model.entry.collection].index_information()), 1)
        h.assert_equal(self._get_index_num(Entity), 1)

        self._make_loader()
        h.assert_equal(len(mongo.db[model.entry.collection].index_information()), 8)
        h.assert_equal(self._get_index_num(Entity), 2)

    @h.raises(mongo.errors.DuplicateKeyError)
    def test_loader_checks_duplicate_entries(self):
        h.skip("FIXME: skip until bunkered datasets")
        d = Dataset(name='foo').save()
        model.entry.create({'name': 'Test Entry'}, d)
        model.entry.create({'name': 'Test Entry'}, d)

        self._make_loader(unique_keys=['name'])

    @h.raises(mongo.errors.DuplicateKeyError)
    def test_loader_checks_duplicate_entities(self):
        Entity(name=u'Test Entity').save()
        Entity(name=u'Test Entity').save()

        self._make_loader(unique_keys=['name'])

    def test_loader_creates_dataset(self):
        self._make_loader(dataset_name="foo")

        h.assert_true(
            Dataset.find_one({'name': 'foo'}),
            "Loader didn't create new dataset!"
        )

    def test_loader_uses_extant_dataset(self):
        Dataset(name="foo", id=123).save()
        ldr = self._make_loader(dataset_name="foo")
        h.assert_equal(ldr.dataset.id, 123)

    # Create Entries

    def test_create_entry(self):
        loader = self._make_loader()
        entry = self._make_entry(loader)
        h.assert_equal(entry['name'], 'Test Entry')

    def test_create_entry_id_from_unique_keys(self):
        loader = self._make_loader(dataset_name="monkeys",
                                   unique_keys=['ka', 'kb', 'kc'])
        entry = self._make_entry(loader, ka="foo", kb="bar", kc="baz")

        h.assert_equal(entry['_id'], util.hash_values(['monkeys', 'foo', 'bar', 'baz']))

    def test_create_entry_returns_query_spec(self):
        loader = self._make_loader()
        entry = {'name': 'one',
                 'amount': 1000.00,
                 'from': loader.create_entity(u'From Entity'),
                 'to': loader.create_entity(u'To Entity'),
                 'first': u'first',
                 'second': u'second',
                 'extra': u'extra'}
        _id = loader.create_entry(**entry)
        fetched_entry = model.entry.get(_id)
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

    def test_entries_different_unique_keys(self):
        loader = self._make_loader(dataset_name='test', unique_keys=['id'])
        e1 = self._make_entry(loader, id='123', name='foo')
        e2 = self._make_entry(loader, id='456', name='bar')

        res = model.entry.find({'dataset.name': 'test'})
        h.assert_equal(res.count(), 2)

    def test_entries_same_unique_keys(self):
        loader = self._make_loader(dataset_name='test', unique_keys=['id'])
        self._make_entry(loader, id='123', name='foo')
        self._make_entry(loader, id='123', name='bar')

        res = model.entry.find({'dataset.name': 'test'})
        h.assert_equal(res.count(), 1)
        h.assert_equal(res.next()['name'], 'bar')

    def test_loads_are_idempotent(self):
        loader1 = self._make_loader(dataset_name='test', unique_keys=['id'])
        self._make_entry(loader1, id='123', name='foo')
        self._make_entry(loader1, id='456', name='bar')
        loader2 = self._make_loader(dataset_name='test', unique_keys=['id'])
        self._make_entry(loader2, id='123', name='foo')
        self._make_entry(loader2, id='456', name='bar')
        self._make_entry(loader2, id='789', name='baz')

        res = model.entry.find({'dataset.name': 'test'})
        h.assert_equal(res.count(), 3)

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

    # Other stuff

    # Fixme: create_view tests.