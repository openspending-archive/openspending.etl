"""Pylons environment configuration"""
import logging
import os
from gettext import translation

from genshi.template import TemplateLoader
from genshi.filters.i18n import Translator
from pylons import config
import pylons
from webhelpers import markdown

from sqlalchemy import engine_from_config
from migrate.versioning.util import construct_engine

from openspending.model import init_model
from openspending.ui.lib import helpers

from openspending.etl.ui.config.routing import make_map
from openspending.etl.ui.lib import app_globals

class MultiDomainTranslator(object):
    """ This is used by Genshi to allow using multiple domains within
    a single template. The usage in a plugin would be about the following::

        config['openspending.etl.ui.translations'].add_domain(__name__, locale_dir)

    """
    # TODO: This needs to be reconfigured to enable live language changes.

    def __init__(self, languages):
        self._translations = {}
        self._languages = languages

    def add_domain(self, domain, localedir):
        t = translation(domain, localedir, languages=self._languages)
        self._translations[domain] = t

    def ugettext(self, *a):
        return pylons.translator.ugettext(*a)

    def ungettext(self, *a):
        return pylons.translator.ungettext(*a)

    def dungettext(self, domain, *a):
        if domain in self._translations:
            return self._translations[domain].ungettext(*a)
        return self.ungettext(*a)

    def dugettext(self, domain, *a):
        if domain in self._translations:
            return self._translations[domain].ugettext(*a)
        return self.ugettext(*a)


def load_environment(global_conf, app_conf):
    """Configure the Pylons environment via the ``pylons.config``
    object
    """

    # Pylons paths
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paths = dict(root=root,
                 controllers=os.path.join(root, 'controllers'),
                 static_files=os.path.join(root, 'public'),
                 templates=[os.path.join(root, 'templates')])

    # Initialize config with the basic options
    config.init_app(global_conf, app_conf, package='openspending.etl.ui', paths=paths)

    config['routes.map'] = make_map()
    config['pylons.app_globals'] = app_globals.Globals()
    config['pylons.h'] = helpers

    # set log level in markdown
    markdown.logger.setLevel(logging.WARN)

    ## redo template setup to use genshi.search_path (so remove std template setup)
    template_paths = [paths['templates'][0]]

    # Translator (i18n)
    config['openspending.etl.ui.translations'] = MultiDomainTranslator([config.get('lang', 'en')])
    translator = Translator(config['openspending.etl.ui.translations'])

    def template_loaded(template):
        translator.setup(template)

    config['pylons.app_globals'].genshi_loader = TemplateLoader(
        template_paths, auto_reload=True, callback=template_loaded)

    # SQLAlchemy
    engine = engine_from_config(config, 'sqlalchemy.')
    engine = construct_engine(engine)
    init_model(engine)

    # Configure ckan
    import openspending.etl.importer.ckan as ckan
    ckan.configure(config)

    # Configure Solr
    import openspending.lib.solr_util as solr
    solr.configure(config)

