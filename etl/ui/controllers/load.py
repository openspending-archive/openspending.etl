import subprocess

from pylons.decorators.cache import beaker_cache

from pylons import url, config, request, tmpl_context as c
from pylons.i18n import _

from openspending.lib import ckan
from openspending.ui.lib import helpers as h
from openspending.ui.lib.authz import requires

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

    def diagnose(self, package):
        c.pkg = ckan.Package(package)

        c.pkg_diagnostics = {}
        for hint in ('model', 'model:mapping', 'data'):
            c.pkg_diagnostics[hint] = _url_or_error_for_package(c.pkg, hint)

        return render('load/diagnose.html')

    @requires('admin')
    def start(self, package):
        jobid = _jobid_for_package(package)

        if daemon.job_running(jobid):
            c.pkg_name = package
            return render('load/start.html')
        else:
            daemon.dispatch_job(
                jobid=jobid,
                config=config['__file__'],
                task='ckan_import',
                args=(package,)
            )
            return redirect(url(
                controller='load',
                action='status',
                package=package
            ))

    def status(self, package):
        jobid = _jobid_for_package(package)

        c.load_running = daemon.job_running(jobid)
        c.load_log = daemon.job_log(jobid)

        c.pkg_name = package

        if request.is_xhr:
            return render('load/_status.html')
        else:
            return render('load/status.html')

def _jobid_for_package(name):
    return "import_%s" % name

def _url_or_error_for_package(pkg, hint):
    try:
        r = pkg.openspending_resource(hint)
        if r:
            return "<a href='%(url)s'>%(url)s</a>" % {"url": r["url"]}
        else:
            return _("None set")
    except ckan.ResourceError as e:
        return "<span class='error-message'>%s</span>" % h.escape(e)