"""The application's model objects"""
import os.path

from pkg_resources import resource_listdir, resource_stream

import mongo
from openspending.etl.ui.model.mongo import Base
from openspending.etl.ui.model.dimension import Dimension
from openspending.etl.ui.model.dataset import Dataset
from openspending.etl.ui.model.classifier import Classifier
from openspending.etl.ui.model.entry import Entry
from openspending.etl.ui.model.entity import Entity
from openspending.etl.ui.model.account import Account
from openspending.etl.ui.model.changeset import Changeset, ChangeObject
from openspending.etl.ui.model.model import Model, default_mapping


def init_mongo(config):
    host = config.get('openspending.mongodb.host', 'localhost')
    port = config.get('mongodb.port', 27017)
    mongo.make_connection(host, port)
    mongo.db_name = config.get('openspending.mongodb.database', 'openspending')
    init_serverside_js()


def init_serverside_js():
    '''
    store (and update) all server side javascript functions
    that are stored in openspending:serverside_js
    '''
    for filename in resource_listdir('openspending.etl.ui', 'serverside_js'):
        if filename.endswith('.js'):
            function_name = filename.rsplit('.js')[0]
            function_file = resource_stream(
                'openspending.etl.ui',
                os.path.join('serverside_js', filename)
            )

            function_string = function_file.read()
            function_file.close()

            setattr(mongo.db().system_js, function_name, function_string)
