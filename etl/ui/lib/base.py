"""The base Controller API

Provides the BaseController class for subclassing.
"""
from time import time

from pylons.controllers import WSGIController
from pylons.templating import literal, cached_template, pylons_globals
from pylons import tmpl_context as c, request, config, app_globals, session
from pylons.controllers.util import abort, redirect
from genshi.filters import HTMLFormFiller

from openspending import model
from openspending.etl.ui import i18n

import logging
log = logging.getLogger(__name__)

def render(template_name, form_fill=None, form_errors={}, extra_vars=None,
           cache_key=None, cache_type=None, cache_expire=None,
           method='xhtml'):
    # Create a render callable for the cache function
    def render_template():
        # Pull in extra vars if needed
        globs = extra_vars or {}

        # Second, get the globals
        globs.update(pylons_globals())
        globs['g'] = app_globals
        globs['_form_errors'] = form_errors

        # Grab a template reference
        template = globs['app_globals'].genshi_loader.load(template_name)
        stream = template.generate(**globs)
        if form_fill is not None:
            filler = HTMLFormFiller(data=form_fill)
            stream = stream | filler

        return literal(stream.render(method=method, encoding=None))

    return cached_template(template_name, render_template, cache_key=cache_key,
                           cache_type=cache_type, cache_expire=cache_expire,
                           ns_options=('method'), method=method)


class BaseController(WSGIController):

    def __call__(self, environ, start_response):
        """Invoke the Controller"""
        # WSGIController.__call__ dispatches to the Controller method
        # the request is routed to. This routing information is
        # available in environ['pylons.routes_dict']
        begin = time()
        try:
            return WSGIController.__call__(self, environ, start_response)
        finally:
            log.debug("Request to %s took %sms" % (request.path,
               int((time() - begin) * 1000)))

    def __before__(self, action, **params):
        account_name = request.environ.get('REMOTE_USER', None)
        if account_name:
            c.account = model.account.find_one_by('name', account_name)
        else:
            c.account = None

        i18n.handle_request(request, c)

        c.q = ''
        c.items_per_page = int(request.params.get('items_per_page', 20))
        c.state = session.get('state', {})

        c.datasets = model.db.session.query(model.Dataset).all()
        c.dataset = None

    def __after__(self):
        if session.get('state', {}) != c.state:
            session['state'] = c.state
            session.save()


