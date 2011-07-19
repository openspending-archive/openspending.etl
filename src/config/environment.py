"""Pylons environment configuration"""
import logging
import os
from gettext import translation

from genshi.template import TemplateLoader
from genshi.filters.i18n import Translator
from pylons import config
import pylons
from webhelpers import markdown

import openspending.etl.ui.lib.app_globals as app_globals
import openspending.etl.ui.lib.helpers
from openspending.etl.ui.config.routing import make_map
from openspending.etl.ui.model import init_mongo
from openspending.etl.ui import plugins

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
    from openspending.etl.ui.plugins import PluginImplementations
    from openspending.etl.ui.plugins.interfaces import IConfigurable, IConfigurer

    # Pylons paths
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paths = dict(root=root,
                 controllers=os.path.join(root, 'controllers'),
                 static_files=os.path.join(root, 'public'),
                 templates=[os.path.join(root, 'templates')])

    # Initialize config with the basic options
    config.init_app(global_conf, app_conf, package='openspending.etl.ui', paths=paths)
    plugins.load_all(config)
    for plugin in PluginImplementations(IConfigurer):
        plugin.configure(config)
    
    config['routes.map'] = make_map()
    config['pylons.app_globals'] = app_globals.Globals()
    config['pylons.h'] = openspending.etl.ui.lib.helpers

    # set log level in markdown
    markdown.logger.setLevel(logging.WARN)

    ## redo template setup to use genshi.search_path (so remove std template setup)
    template_paths = [paths['templates'][0]]
    extra_template_paths = config.get('extra_template_paths')
    if extra_template_paths:
        # must be first for them to override defaults
        template_paths = extra_template_paths.split(' ') + template_paths
    
    # Translator (i18n)
    config['openspending.etl.ui.translations'] = MultiDomainTranslator([config.get('lang', 'en')])
    translator = Translator(config['openspending.etl.ui.translations'])
    def template_loaded(template):
        translator.setup(template)

    # Create the Genshi TemplateLoader
    # config['pylons.app_globals'].genshi_loader = TemplateLoader(
    #    paths['templates'], auto_reload=True)
    # tmpl_options["genshi.loader_callback"] = template_loaded
    config['pylons.app_globals'].genshi_loader = TemplateLoader(
        template_paths, auto_reload=True, callback=template_loaded)

    init_mongo(config)
    
    import celerypylons # side effect: set os.environ["CELERY_LOADER"] to Pylons Celery Loader
    
    # CONFIGURATION OPTIONS HERE (note: all config options will override
    # any Pylons config options)
    for plugin in PluginImplementations(IConfigurable):
        plugin.configure(config)
        
    
