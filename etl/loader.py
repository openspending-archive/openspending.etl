import logging
import time

from bson import DBRef
from pymongo import ASCENDING

from openspending.lib.aggregator import update_distincts
from openspending.lib.cubes import Cube
from openspending.lib.util import check_rest_suffix, deep_get
from openspending.logic.classifier import create_classifier, get_classifier
from openspending.logic.entry import classify_entry, entitify_entry
from openspending.logic.dimension import create_dimension
from openspending.model import Changeset, ChangeObject, Classifier
from openspending.model import Dataset, Dimension, Entry, Entity
from openspending.ui.lib.views import View

log = logging.getLogger(__name__)

CREATE = ChangeObject.OperationType.CREATE
UPDATE = ChangeObject.OperationType.UPDATE


class Loader(object):
    '''\
    A helper class that provides all methods required to save the data from a
    dataset into Open Spending in an efficient way.

    Read the :doc:`loaders.rst <../loaders>` in the /doc folder.
    '''

    def __init__(self, dataset_name, unique_keys, label, description=u'',
                 metadata=None, currency=u'gbp', time_axis='time.from.year',
                 changeset=None):
        '''\
        Constructs a Loader for the :class:`openspending.model.Dataset`
        `dataset_name`. Calling the constructor creates or updates the
        `Dataset` object with `dataset_name`, `label`, `description`,
        `metadata` and `currency`. The Loader instance can only be used
        to create :class:`openspending.model.Entry` objects with the same set
        of `unique_keys`. If you need to create another type of
        ``Entry`` objects instantiate another ``Loader``.

        ``dataset_name``
            The unique name for the dataset.
        ``unique_keys``
            The keys for which all entries in the dataset are unique.
            For example if you have a entries with payments that have
            are identifiable by a *department* and a *consecutive number*
            that is unique within the *department*, you would pass in
            a list with the keys ``['department', 'consecutive_number']``.
        ``label``
            A label for the dataset that can be presented to the user
        ``description``
            A description for the dataset taht can be presented
            to the user.
        ``metadata``
            A ``dict`` with metadata that will be saved on the dataset.
        ``currency``
            The default currency for the entries in the dataset. An
            individual currency can be set in :meth:`create_entry`.
            The currenty is stored in upper case.
        ``time_axis``
            The time axis of the dataset. This is the time range for which
            all entries in the dataset can be analized. The default is
            'time.from.year' and should not be changed.
            fixme: add details and move possible values into constants in
            model.dataset.
         ``changeset``
            A :class:`openspending.model.Changeset` object. This is only required
            if you use load a dataset with more than one loader. If you
            want to add manual changes to the changeset of your loader
            you can retrive the changeset with *.changeset*.

        Raises:
            ``AssertionError`` if more than one dataset with the name
                ``dataset_name`` exists already.
            ``ValueError``
                If and duplicated :class:`openspending.model.Entry` object
                is found (The entry has the same values for the
                ``unique_keys``) or two :class:`model.class.Entity`
                objects are found with the same name.
        '''
        assert isinstance(dataset_name, unicode)
        assert isinstance(unique_keys, list)
        check_rest_suffix(dataset_name)

        # create a changeset:
        if changeset is None:
            name = dataset_name
            if label:
                name = "%s (%s)" % (name, label)
            message = ('Load dataset %s. currency: %s, time axis: %s' %
                       (name, currency, time_axis))
            changeset = Changeset()
            changeset.author = 'system'
            changeset.message = message
            changeset.save()
        self.changeset = changeset

        # get the dataset
        q = {'name': dataset_name}
        dataset_count = Dataset.find(q).count()
        if dataset_count == 0:
            operation = CREATE
        elif dataset_count == 1:
            operation = UPDATE
        else:
            raise AssertionError("Ambiguous dataset name: %s" % dataset_name)
        data = {"label": label,
                "currency": currency.upper(),
                "description": description,
                "time_axis": time_axis}
        if metadata is not None:
            data.update(metadata)
        Dataset.c.update(q, {"$set": data}, upsert=True)
        self.dataset = Dataset.find_one(q)
        self._add_changeobj(Dataset.c.name, self.dataset.id, self.dataset,
                            operation)
        self.base_query = {"dataset._id": self.dataset.id}

        # caches
        self.entity_cache = {}
        self.classifier_cache = {}
        self.unique_keys = unique_keys

        # We need indexes to speed up lookups and updates
        self.ensure_index(Entry, ['dataset._id'])
        self.ensure_index(Entry, ['dataset.name'])
        self.ensure_index(Entry, ['classifiers'])
        self.ensure_index(Entry, ['entities'])
        self.ensure_index(Entry, ['from._id'])
        self.ensure_index(Entry, ['to._id'])
        self.ensure_index(Entry, ['to._id', 'from._id', 'amount'])
        self.ensure_index(Classifier, ['taxonomy', 'name'])
        self.ensure_index(Dimension, ['dataset', 'key'])
        self.ensure_index(Entity, ['name'])
        # fixme: The entry.name index might be dropped when Base.by_id()
        #        changes. The 'name' field for entries is not interesting.
        self.ensure_index(Entry, ['name'])

        # Make sure entries and entities are unique
        self.existing_entries = self._ensure_unique(Entry, self.unique_keys,
                                                    self.base_query)
        self._ensure_unique(Entity, ['name'])

        # info's needed to print statistics during the run
        self.num_entries = 0
        self.start_time = None

    def ensure_index(self, modelcls, keys):
        '''\
        Ensure an index is created for the collection of
        ``modelcls`` exists for the combination of ``keys``.
        The index will be ascending for all keys.

        ``modelcls``
            A model class inheriting from :class:`openspending.model.Base`.
        ``keys``
            A list of strings.
        '''
        log.debug('ensure index for "%s": "%s"...' %
                 (modelcls.collection_name, '/'.join(keys)))
        start = time.time()
        keys = [(key, ASCENDING) for key in keys]
        modelcls.c.ensure_index(keys)
        log.debug('done in %0.2fs' % (time.time() - start))

    def _extract_key_values(self, dict_, unique_keys):
        '''\
        Extract the values for the keys in ``unique_keys``
        from the ``dict_``.

        ``dict_``
             A dict like object
        ``unique_keys``
             A list of strings

        Returns: A ``tuple`` of values of the unique_keys.

        Raises: :exc:`KeyError` if a unique column has no value.
        '''
        res = []

        for k in unique_keys:
            v = deep_get(dict_, k)
            # deep_get doesn't raise KeyErrors, so do it here.
            if v is None:
                raise KeyError("Unique key %s missing from dict: %s" % (k, dict_))
            else:
                res.append(v)

        return tuple(res)

    def _ensure_unique(self, modelcls, keys, query={}):
        '''\
        Ensure that in the collection for the model class
        ``modelcls`` there are not two items with the same values
        for the ``keys``.

        Not part of the ``Loader`` API!

        ``modelcls``
            a ``class`` that inherits from :class:`openspending.model.Base`.
        ``keys``
            A list of keys for which the entries have to be unique.
        ``query``
            A mongodb query spec to limit the elements in the collection
            that will be checked.
        ``cache``
            Use a cache (*dict*) to store the objectid of all entries that
            are in this dataset.

        Raises: ``ValueError`` if two identical elements are found.
        '''
        start = time.time()
        index = {}
        log.debug('ensure unique items for %s (%s) ...' %
                  (modelcls, '/'.join(keys)))

        items = modelcls.find(query, fields=keys)

        for item in items:
            values = self._extract_key_values(item, keys)
            if values in index:
                value_dict = dict(zip(keys, values))
                msg = ('%s with unique keys is present twice. '
                       'keys/values: %s, query: %s') % (modelcls.__name__,
                                                        str(value_dict),
                                                        self.base_query)
                raise ValueError(msg)
            index[values] = item['_id']

        log.debug("... done. %s instances checked in %0.2fs" %
                  (len(index), time.time() - start))
        return index

    def _add_changeobj(self, collection_name, _id, data, operation_type):
        change = ChangeObject()
        object_id = [collection_name, _id]
        change.object_id = object_id
        change.data = data
        change.operation_type = operation_type
        change.changeset = self.changeset
        change.save()

    def create_entry(self, **entry):
        '''\
        Create or update an :class:`openspending.model.Entry` object in the
        database.

        ``entry``
            A ``dict`` containing at least these information:

            ``amount``
                amount spend
            ``from``
                :class:`openspending.model.Entity` object
            ``to``
                :class:`openspending.model.Entity` object
            ``data for unique keys``
                Data for all ``unique_keys`` of the ``Loader``

            If ``entry`` does not contain a ``currency``, the currency
            of the loader object is used.

        Returns: A mongodb query spec that can be used with
        :meth:`openspending.model.Entry.find_one()` to get the object.

        Raises: ``AssertionError`` in some cases if the Entry violates
        the datamodel (fixme: full assertions and description!)
        '''
        assert 'amount' in entry, "No amount!"
        assert isinstance(entry['amount'], float), entry['amount']
        assert 'to' in entry, "No recipient!"
        assert 'from' in entry, "No spender!"
        for key in ('to', 'from'):
            entity_ = entry[key]
            if isinstance(entity_, Entity):
                continue
            if isinstance(entity_, dict):
                ref = entity_.get('ref')
                if (isinstance(ref, DBRef) and
                    ref.collection == Entity.collection_name):
                    continue
            raise AssertionError('Wrong value for "%s": "%s"' %
                                 (key, entity_))

        if self.start_time is None:
            self.start_time = time.time()

        # Create the Entry
        if (not 'currency' in entry) or (entry['currency'].upper() == "(EMPTY)"):
            entry['currency'] = self.dataset.currency
        else:
            entry['currency'] = entry['currency'].upper()

        entry['dataset'] = self.dataset.to_ref_dict()

        for key in ('to', 'from'):
            obj = entry[key]
            if isinstance(obj, Entity):
                self.entitify_entry(entry, obj, key)

        entry_cache_key = self._extract_key_values(entry, self.unique_keys)
        existing_entry_id = self.existing_entries.get(entry_cache_key, None)
        if existing_entry_id is not None:
            upsert_query = {'_id': existing_entry_id}
            Entry.c.update(upsert_query, {"$set": entry}, upsert=True)
            entry['_id'] = existing_entry_id
        else:
            # with maniulate=True the new '_id' will be added to the document.
            Entry.c.insert(entry, manipulate=True)

        # Add a ChangeObject for this change
        operation_type = (existing_entry_id is None) and CREATE or UPDATE
        self._add_changeobj(Entry.c.name, entry['_id'], entry, operation_type)

        # Print progress
        self.num_entries += 1
        if self.num_entries % 1000 == 0:
            now = time.time()
            timediff = now - self.start_time
            self.start_time = now
            log.debug("%s loaded %s in %0.2fs" % (self.dataset.name,
                                                  self.num_entries,
                                                  timediff))
        return {'_id': entry['_id']}

    def create_entity(self, name=None, label=u'', description=u'',
                      _cache=None, match_keys=('name', ), **entity):
        '''\
        Create or update an :class:`openspending.model.Entity` object in the
        database when this is called for the entity the first time.
        An existing entity is looked up with the entitie's data for
        *match_keys*. By default we look up by name, but other data
        like company or tax ids may be more appropriate.

        The entry will only be created/updated if it is not in the
        ``_cache``.

        ``name``
            Name of the entity.
        ``label, description``
            label an description of the entity (unicodes)
        ``match_keys``
            The keys with which we try to find an existing entity
            in the database. default: ('name',). type: ``list`` or
            ``tuple``.
        ``**entity``
            Keyword arguments that are saved in the entity.
        ``_cache``
          Use the given ``dict`` like object for caching.
          Normally not used by callers. It can be used to force an
          update of an entity that was created/updated by an earlier
          call. With ``None`` (default), the ``Loader`` uses internal
          caching.

        Returns: The created ``Entity`` object.

        Raises:

        :exc:`AssertionError`
            If the name ends with a suffix used for REST, e.g. .json.
            If match_keys is not list or tuple.
        :exc:`KeyError`
            If a given match_key is not present in the entity.
        '''
        # assertions
        check_rest_suffix(name)
        if not isinstance(match_keys, (list, tuple)):
            raise AssertionError('match_keys has to be list or tuple')

        entity.update({'name': name,
                       'label': label,
                       'description': description})

        # prepare a cache for the match_keys combination
        if _cache is None:
            _cache = self.entity_cache
        cache = _cache.setdefault(match_keys, {})
        cache_key = tuple([entity[key] for key in match_keys])

        if not cache_key in cache:
            query = {}
            for key in match_keys:
                query[key] = entity[key]
            entity_obj = Entity.find_one(query)

            if entity_obj is None:
                operation = CREATE
            else:
                operation = UPDATE

            Entity.c.update(query, {"$set": entity}, upsert=True)
            new_entity = Entity.find_one(query)
            self._add_changeobj(Entity.c.name, new_entity['_id'],
                                new_entity, operation)
            cache[cache_key] = new_entity

        return cache[cache_key]

    def get_default_society(self):
        '''\
        Get a default :class:`openspending.model.Entity` object that
        represents the society (general public) to use across all
        datasets. This can be called only one time.
        '''
        return self.create_entity(name=u'society',
            label=u'Society (the General Public)',
            description=(u'A dummy entity to be the recipient of '
                         u'final government spending'))

    def get_classifier(self, name, taxonomy, _cache=None):
        '''\
        Get the classifier object with the name ``name`` for the taxonomy
        ``taxonomy``. This will be cached to speed up the loader

        ``name``
          name of the classifier. (``unicode``)
        ``taxonomy``
          The taxonomy to which the classifier ``name`` belongs.
          (``unicode``)
        ``_cache``
          Use the given ``dict`` like object for caching.
          Normally not used by callers. It can be used to ensure the
          classifier is fetched from the database

        Returns: An :class:`openspending.model.Classifier` object if found or
        ``None``.
        '''
        if _cache is None:
            _cache = self.classifier_cache
        if not (name, taxonomy) in _cache:
            _cache[(name, taxonomy)] = get_classifier(name, taxonomy)
        return _cache[(name, taxonomy)]

    def create_classifier(self, name, taxonomy, label=u'', description=u'',
                          _cache=None, **classifier):
        '''\
        Create a :class:openspending.model.`Classifier`. The ``name`` has to
        be unique for the ``taxonomy``. The ``classifier`` will be updated
        with the values for ``label``, ``description`` and
        ``**classifier``
        Note that the classifier for ``(name, taxonomy)`` is only created or
        updated if it is not already in the ``_cache``.

        Arguments:

        ``name``
          name of the classifier. (``unicode``)
        ``taxonomy``
          The taxonomy to which the classifier ``name`` belongs.
          (``unicode``)
        ``label``, ``descripiton``, ``**classifiers``
          used to update the classifier for the first time
        ``_cache``
          Use the given ``dict`` like object for caching.
          Normally not used by callers. It can be used to force an
          update of a classifier that was created/updated by an earlier
          call. With ``None`` (default), the ``Loader`` uses internal
          caching.

        Returns: An :class:`openspending.model.Classifier` object

        Raises:
           AssertionError if more than one ``Classifer`` object with the
           Name existes in the ``taxonomy``
        '''
        if _cache is None:
            _cache = self.classifier_cache
        if not (name, taxonomy) in _cache:
            existing = self.get_classifier(name, taxonomy, _cache=_cache)
            if existing is None:
                operation = UPDATE
            else:
                operation = CREATE
            classifier = create_classifier(name, taxonomy, label,
                                           description, **classifier)
            _cache[(name, taxonomy)] = classifier
            self._add_changeobj(Classifier.c.name, classifier.id,
                                classifier, operation)
        return _cache[(name, taxonomy)]

    def create_dimension(self, key, label, description, **kwargs):
        '''\
        Describe the data you save on *entries* with the key *key*.
        Describe it with a *label* and a *description*.

        ``key``
            The key on the model where the data to describe is saved.
            (type: ``unicode``)
        ``label``
            A label (title) to display to the user (type: ``unicode``)
        ``description``
            A description to display to the user. (type: ``unicode``)
        ``kwargs``
            You can add more information as keyword arguments. All
            values have to be ``unicode``.

        Returns: ``None``

        Raises: ``TypeError`` if one of the arguments is of the wrong
        type.
        '''
        create_dimension(self.dataset.name, key, label,
                         description=description, **kwargs)

    def classify_entry(self, entry, classifier, name):
        '''\
        Update the *entry* to be classified with *classifier*.
        *entry* is mutated, but not returned.

        ``entry``
            A ``dict`` like object, e.g. an instance of
            :class:`openspending.model.Base`.
        ``classifier``
            A :class:`openspending.model.Classifier` object
        ``name``
            This is the key where the value of the classifier
            will be saved. This my be the same as classifier['name'].

        return:``None``
        '''
        classify_entry(entry, classifier, name)

    def entitify_entry(self, entry, entity, name):
        '''\
        Update the *entry* to use the *entity* for the
        dimension *name*.

        ``entry``
            A ``dict`` like object, e.g. an instance of
            :class:`openspending.model.Base`.
        ``entity``
            A :class:`openspending.model.entity` object
        ``name``
            This is the key where the value of the entity
            will be saved.

        return:``None``
        '''
        entitify_entry(entry, entity, name)

    def create_view(self, cls, add_filters, name, label, dimension,
                    breakdown=None, view_filters={}):
        '''\
        Create a view. The view will be computed when you call
        :meth:`finalize`.

        ``cls``
            A model class (inheriting from :class:`openspending.model.Base`)
        ``add_filters``
            A :term:`mongodb query spec` used as a query to select the
            instances of *cls* that will be used for the view.
        ``name``
            A name for the view. This name must be unique for all views
            for *cls* in an Open Spending site.
        ``label``
            A label that can be displayed to the user.
        ``dimensions``
            The dimensions that will be used to compute the view
        ``breakdown``
            ...
        ``view_filters``
            ...

        Returns: A :class:`openspending.lib.views.View` object.
        '''
        log.debug("pre-aggregating view %s on %r where %r",
                  name, cls, view_filters)
        view = View(self.dataset, name, label, dimension,
                    breakdown, cuts=view_filters)
        view.apply_to(cls, add_filters)
        view.compute()
        Dataset.c.update({'name': self.dataset.name},
                         {'$set': {'cubes': self.dataset.get('cubes', {})}})
        self.dataset = Dataset.by_id(self.dataset.name)
        return view

    def compute_aggregates(self):
        '''\
        This method has to be called as the last method when
        using the loader. It will add additional, required data
        to the database.
        '''
        log.debug("updating distinct values...")
        update_distincts(self.dataset.name)
        log.debug("updating all cubes...")
        Cube.update_all_cubes(self.dataset)

    def flush_aggregates(self):
        pass
