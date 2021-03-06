import subprocess

from pylons.decorators.cache import beaker_cache

from pylons import url, config, request, tmpl_context as c
from pylons.i18n import _

from openspending.etl.importer import ckan
from openspending.ui.lib import helpers as h
#from openspending.ui.lib.authz import requires

from openspending.etl.command import daemon
from openspending.etl.ui.lib.base import BaseController, render, redirect

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

    def preflight(self, package):
        c.pkg = ckan.Package(package)

        c.pkg_diagnostics = {}
        for hint in ('model', 'data'):
            c.pkg_diagnostics[hint] = _resource_or_error_for_package(c.pkg, hint)

        return render('load/preflight.html')

    #@requires('admin')
    def start(self, package):
        job_id = _job_id_for_package(package)

        if daemon.job_running(job_id):
            c.pkg_name = package
            c.job_id = job_id
            return render('load/start.html')
        else:
            daemon.dispatch_job(
                job_id=job_id,
                config=config['__file__'],
                task='ckan_import',
                args=(package,)
            )
            return redirect(url(
                controller='job',
                action='status',
                job_id=job_id
            ))

def _job_id_for_package(name):
    return "import_%s" % name

def _resource_or_error_for_package(pkg, hint):
    try:
        return pkg.openspending_resource(hint)
    except ckan.ResourceError as e:
        return {'error': h.escape(e)}


