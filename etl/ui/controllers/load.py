from pylons.decorators.cache import beaker_cache

from pylons import app_globals as g, tmpl_context as c
from pylons.i18n import _

from openspending.lib import ckan
from openspending.ui.lib import helpers as h

from openspending.etl.ui.lib.base import BaseController, render

import logging
log = logging.getLogger(__name__)

class LoadController(BaseController):

    def index(self):
        c.group = ckan.openspending_group
        return render('load/index.html')

    @beaker_cache(expire=600, type='memory')
    def packages(self):
        try:
            c.packages = ckan.openspending_packages()
        except ckan.CkanApiError as e:
            log.error(e)

        return render('load/_packages.html')

    def diagnose(self, package):
        c.pkg = ckan.Package(package)

        c.pkg_diagnostics = {}
        for hint in ('model', 'model:mapping', 'data'):
            c.pkg_diagnostics[hint] = _url_or_error_for_package(c.pkg, hint)

        return render('load/diagnose.html')

    # @requires("admin")
    # def load(self, package, resource, model):
    #     max_errors = 25 # TODO: make this tunable
    #
    #     from openspending.etl.ui.lib.tasks import load_dataset
    #     self._load_ckan(package, resource)
    #     model = Model.by_id(model)
    #
    #     if model is None:
    #         abort(404)
    #
    #     if g.use_celery:
    #         result = load_dataset.delay(c.resource.get('url'), model,
    #                                     max_errors=max_errors)
    #
    #         return redirect(url_for(controller="load",
    #                                 action="task",
    #                                 operation="load",
    #                                 task_id=result.task_id))
    #     else:
    #         c.result = load_dataset(c.resource.get('url'), model,
    #                                 max_errors=max_errors)
    #         return render('load/started.html')
    #
    # def task(self, operation, task_id):
    #     from openspending.etl.ui.lib.tasks import load_dataset
    #     result = load_dataset.AsyncResult(task_id)
    #
    #     c.result = result
    #     assert operation in ("store", "load")
    #     c.operation = operation
    #     return render('load/task.html')

def _url_or_error_for_package(pkg, hint):
    try:
        r = pkg.openspending_resource(hint)
        if r:
            return "<a href='%(url)s'>%(url)s</a>" % {"url": r["url"]}
        else:
            return _("None set")
    except ckan.ResourceError as e:
        return "<span class='error-message'>%s</span>" % h.escape(e)