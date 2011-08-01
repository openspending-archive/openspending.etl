import logging
import time

from bson import DBRef
from pymongo import ASCENDING

from openspending.lib.aggregator import update_distincts
from openspending.lib.cubes import Cube
from openspending.lib.util import check_rest_suffix, deep_get
from openspending.logic.dimension import create_dimension
from openspending import model
from openspending import mongo
from openspending.model import Dimension, Entity
from openspending.ui.lib.views import View

from openspending.etl import util

log = logging.getLogger(__name__)

class LoaderError(Exception):
    pass

class LoaderSetupError(LoaderError):
    pass

class Loader(object):
    """\
    A helper class that provides all methods required to save the data from a
    dataset into Open Spending in an efficient way.

    Read the :doc:`loaders.rst <../loaders>` in the /doc folder.
    """

    def __init__(self, dataset_name, unique_keys, label, description=u'',
                 metadata=None, currency=u'gbp', time_axis='time.from.year',
                 changeset=None):
        """\
        Constructs a Loader for the ``dataset`` `dataset_name`. Calling the
        constructor creates or updates the `dataset` object with
        `dataset_name`, `label`, `description`, `metadata` and `currency`.
        The Loader instance can only be used to create ``entry`` objects with
        the same set of `unique_keys`. If you need to create another type of
        ``entry`` objects instantiate another ``Loader``.

        ``dataset_name``
            The unique name for the dataset.
        ``unique_keys``
            The keys for which all entries in the dataset are unique.
            For example if you have a entries with payments that have
            are identifiable by a *department* and a *consecutive number*
            that is unique within the *department*, you would pass in
            a list with the keys ``['department', 'consecutive_number']``. All
            datasets must have a non-empty list of ``unique_keys``.
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
            ``LoaderSetupError`` if more than one dataset with the name
                ``dataset_name`` exists already, or if no ``unique_keys`` are
                provided.
        """
        check_rest_suffix(dataset_name)

        if not len(unique_keys) > 0:
            raise LoaderSetupError("Must provide a non-empty set of unique keys!")

        self.dataset = model.dataset.find_one_by('name', dataset_name)
        self.new_dataset = not self.dataset

        if self.new_dataset:
            _id = model.dataset.create({'_id': util.hash_values([dataset_name]),
                                        'name': dataset_name})
            self.dataset = model.dataset.get(_id)

        metadata = metadata or {}
        metadata.update({
            "label": label,
            "currency": currency.upper(),
            "description": description,
            "time_axis": time_axis
        })
        model.dataset.update(self.dataset, {'$set': metadata})

        # Update local dataset from database
        self.dataset = model.dataset.get(self.dataset['_id'])
        self.base_query = {"dataset._id": self.dataset['_id']}

        # caches
        self.entity_cache = {}
        self.classifier_cache = {}
        self.unique_keys = sorted(unique_keys)

        # We need indexes to speed up lookups and updates
        self.ensure_index(model.entry, ['dataset._id'])
        self.ensure_index(model.entry, ['dataset.name'])
        self.ensure_index(model.entry, ['classifiers'])
        self.ensure_index(model.entry, ['entities'])
        self.ensure_index(model.entry, ['from._id'])
        self.ensure_index(model.entry, ['to._id'])
        self.ensure_index(model.entry, ['to._id', 'from._id', 'amount'])
        self.ensure_index(model.classifier, ['taxonomy', 'name'])
        self.ensure_index(Dimension, ['dataset', 'key'])

        # Ensure existing entities are uniquely identified by name
        self.ensure_index(Entity, ['name'], unique=True, drop_dups=False)

        # Ensure the unique_keys is compatible with existing data.
        # FIXME: once datasets have their own namespaces, reenable this uniqueness
        # constraint
        #uniques = ['dataset._id'] + self.unique_keys
        #self.ensure_index(model.entry, uniques, unique=True, drop_dups=False)

        # info's needed to print statistics during the run
        self.num_entries = 0
        self.start_time = None

    def ensure_index(self, modelcls, keys, **kwargs):
        """\
        Ensure an index is created for the collection of
        ``modelcls`` exists for the combination of ``keys``.
        The index will be ascending for all keys.

        ``modelcls``
            A model class inheriting from :class:`openspending.model.Base`.
        ``keys``
            A list of strings.
        """
        # FIXME: remove when all models have same interface
        try:
            coll = modelcls.c
        except AttributeError:
            coll = mongo.db[modelcls.collection]

        coll.ensure_index([(key, ASCENDING) for key in keys], **kwargs)

    def _entry_unique_values(self, entry):
        """\
        Extract the values for the keys in ``unique_keys``
        from the given``entry``.

        ``entry``
             A dict like object
        ``unique_keys``
             A list of strings

        Returns: A ``tuple`` of values of the unique_keys.

        Raises: :exc:`KeyError` if a unique column has no value.
        """
        res = []

        for k in self.unique_keys:
            v = deep_get(entry, k)
            # deep_get doesn't raise KeyErrors, so do it here.
            if v is None:
                raise KeyError("Unique key %s missing from entry: %s" % (k, entry))
            else:
                res.append(v)

        return res

    def create_entry(self, **entry):
        """\
        Create or update an ``entry`` object in the database.

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

        Returns: A mongodb query spec that can be used to get the object.

        Raises: ``AssertionError`` in some cases if the entry violates
        the datamodel (fixme: full assertions and description!)
        """
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

        # Create the entry
        if not 'currency' in entry or not entry['currency']:
            entry['currency'] = self.dataset['currency'].upper()
        else:
            entry['currency'] = entry['currency'].upper()

        for key in ('to', 'from'):
            obj = entry[key]
            if isinstance(obj, Entity):
                self.entitify_entry(entry, obj, key)

        entry_uniques = [self.dataset['name']]
        entry_uniques.extend(self._entry_unique_values(entry))
        entry_id = util.hash_values(entry_uniques)

        extant_entry = model.entry.get(entry_id)

        if extant_entry:
            if self.new_dataset:
                log.warn("Duplicate entry found for new dataset '%s'. This is "
                         "almost certainly not what you wanted. Are you sure "
                         "that your unique_keys are truly unique across the "
                         "dataset? Unique keys: %s", self.dataset['name'],
                         self._entry_unique_values(entry))
            else:
                log.debug("Updating extant entry '%s' with new data", entry_id)
            model.entry.update(extant_entry, {'$set': entry})
        else:
            entry['_id'] = entry_id
            model.entry.create(entry, self.dataset)

        # Print progress
        self.num_entries += 1
        if self.num_entries % 1000 == 0:
            now = time.time()
            timediff = now - self.start_time
            self.start_time = now
            log.debug("%s loaded %s in %0.2fs", self.dataset['name'],
                      self.num_entries, timediff)

        return entry_id

    def create_entity(self, name=None, label=u'', description=u'',
                      match_keys=('name', ), **entity):
        """\
        Create or update an :class:`openspending.model.Entity` object in the
        database when this is called for the entity the first time.
        An existing entity is looked up with the entitie's data for
        *match_keys*. By default we look up by name, but other data
        like company or tax ids may be more appropriate.

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

        Returns: The created ``Entity`` object.

        Raises:

        :exc:`AssertionError`
            If the name ends with a suffix used for REST, e.g. .json.
            If match_keys is not list or tuple.
        :exc:`KeyError`
            If a given match_key is not present in the entity.
        """
        # assertions
        check_rest_suffix(name)
        if not isinstance(match_keys, (list, tuple)):
            raise AssertionError('match_keys has to be list or tuple')

        entity.update({'name': name,
                       'label': label,
                       'description': description})

        # prepare a cache for the match_keys combination

        cache = self.entity_cache.setdefault(match_keys, {})
        cache_key = tuple([entity[key] for key in match_keys])

        if not cache_key in cache:
            query = {}
            for key in match_keys:
                query[key] = entity[key]

            Entity.c.update(query, {"$set": entity}, upsert=True)
            new_entity = Entity.find_one(query)
            cache[cache_key] = new_entity

        return cache[cache_key]

    def create_classifier(self, classifier):
        """\
        Create a classifier from the dict-like object ``classifier``.
        See :func:`openspending.model.classifier.create` for full details.
        """
        name = classifier['name']
        taxonomy = classifier['taxonomy']

        if not (name, taxonomy) in self.classifier_cache:
            existing = model.classifier.find_one({'name': name,
                                                  'taxonomy': taxonomy})
            if existing:
                classifier = existing
            else:
                _id = model.classifier.create(classifier)
                classifier = model.classifier.get(_id)

            self.classifier_cache[(name, taxonomy)] = classifier

        return self.classifier_cache[(name, taxonomy)]

    def create_dimension(self, key, label, description, **kwargs):
        """\
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
        """
        create_dimension(self.dataset['name'], key, label,
                         description=description, **kwargs)

    def classify_entry(self, entry, classifier, name):
        """\
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
        """
        model.entry.classify_entry(entry, classifier, name)

    def entitify_entry(self, entry, entity, name):
        """\
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
        """
        model.entry.entitify_entry(entry, entity, name)

    def create_view(self, cls, add_filters, name, label, dimension,
                    breakdown=None, view_filters={}):
        """\
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
        """
        log.debug("pre-aggregating view %s on %r where %r",
                  name, cls, view_filters)
        view = View(self.dataset, name, label, dimension,
                    breakdown, cuts=view_filters)
        view.apply_to(cls, add_filters)
        view.compute()
        model.dataset.update(self.dataset,
                             {'$set': {'cubes': self.dataset.get('cubes', {})}})
        self.dataset = model.dataset.get(self.dataset['_id'])
        return view

    def compute_aggregates(self):
        """\
        This method has to be called as the last method when
        using the loader. It will add additional, required data
        to the database.
        """
        log.debug("updating distinct values...")
        update_distincts(self.dataset['name'])
        log.debug("updating all cubes...")
        Cube.update_all_cubes(self.dataset)

    def flush_aggregates(self):
        pass
