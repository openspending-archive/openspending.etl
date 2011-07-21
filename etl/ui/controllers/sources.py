from pylons import config
from pylons.decorators.cache import beaker_cache

from urllib import unquote_plus as unquote

from pylons import request, tmpl_context as c, url
from pylons.controllers.util import abort, redirect
from pylons.i18n import _
from routes import url_for

from openspending.lib import ckan
from openspending.lib import json
from openspending.logic.model import save_model, available_models
from openspending.model import Dataset, Model
from openspending.ui.lib import jsonp
from openspending.ui.lib import helpers as h
from openspending.ui.lib.authz import requires

from openspending.etl.ui.lib.base import BaseController, render
from openspending.etl.csvimport import DatasetImporter
from openspending.etl.csvimport import validate_model, resource_lines, load_dataset
from openspending.etl.resourceimport import package_and_resource, load_from_ckan

import logging
log = logging.getLogger(__name__)

class SourcesController(BaseController):

    def index(self):
        c.group = ckan.openspending_group
        return render('sources/index.html')

    @beaker_cache(expire=600, type='memory')
    def ckan_packages(self):
        try:
            c.packages = ckan.openspending_packages()
        except ckan.CkanApiError as e:
            log.error(e)

        return render('sources/_ckan_packages.html')

    def diagnose(self, package):
        c.pkg = ckan.Package(package)

        c.pkg_diagnostics = {}
        for hint in ('model', 'model:mapping', 'data'):
            c.pkg_diagnostics[hint] = _url_or_error_for_package(c.pkg, hint)

        return render('sources/diagnose.html')

    # this really does not belong here
    def _load_ckan(self, package, resource):
        c.package, c.resource = package_and_resource(package, resource)
        if c.package is None:
            abort(404, "There was an error loading this resource")
        c.dataset, c.importer = load_from_ckan(c.package, c.resource)
        c.lines = c.importer.sanity_check()
        if not c.lines:
            error = c.importer.errors[0].message
            h.flash_error(_("This resource cannot be read as a CSV file. "
                            "Please check the format: %s." % error))
            redirect(url(controller='sources', action='index'))

    @requires("user")
    def validate(self, package, resource):
        self._load_ckan(package, resource)
        c.csv_errors = c.importer.errors
        c.csv_warnings = c.importer.warnings
        return render("sources/validation.html")

    @requires("user")
    def mapping_form(self, package, resource):
        from openspending.etl.ui.lib.mappingimporter import MappingImporter
        from openspending.etl.ui.lib.mapping import validate_mapping
        self._load_ckan(package, resource)
        c.error = ''
        c.columns = sorted(c.lines[0].keys())
        c.samples = dict([(k, [c.lines[i][k] for i in range(1, 4)]) for
                          k in c.columns])
        c.models = available_models(c.package.get('name'))
        if c.models:
            c.mapping = c.models[-1]['model'].get('mapping', {})
        else:
            c.mapping = {}
        invalid_json = False

        mapping_csv_url = request.params.get('mapping_csv', '')
        if mapping_csv_url:
            importer = MappingImporter()
            try:
                c.mapping = importer.import_from_url(mapping_csv_url)
            except Exception, inst:
                c.error = 'Failed to load from spreadsheet: %s' % inst
        if request.method == 'POST':
            params = request.params
            c.mapping = params['mapping'].strip()
            try:
                c.mapping = json.loads(c.mapping)
            except Exception, inst:
                invalid_json = True
                c.error = 'Invalid JSON: %s' % inst

        if request.method == 'POST' and not invalid_json:
            if 'validate' in params:
                try:
                    validate_model({
                        'dataset': c.dataset,
                        'mapping': c.mapping
                        })
                except Exception, inst:
                    c.error = 'Validation failed: %s' % inst
                if not c.error:
                    h.flash_success('Mapping validated OK!')
            else:
                model_id = save_model(c.account, dataset=c.dataset, mapping=c.mapping)
                model = Model.by_id(model_id)
                h.flash_success('Saved model')
                if 'dryrun' in params:
                    c.dry_run_results = ['Starting dry run']
                    def progress_hook(msg):
                        c.dry_run_results.append(msg)
                    result = load_dataset(c.resource.get('url'), model,
                            dry_run=True, progress_callback=progress_hook)
                    h.flash_success('Completed Dry Run')
                elif 'load' in params:
                    load_url = h.url_for(
                                controller='sources',
                                action='load',
                                package=c.package.get('name'),
                                resource=c.resource.get('id'),
                                model=model_id)
                    redirect(load_url)
        if c.error:
            h.flash_error(c.error)

        if not invalid_json: # if invalid json do not want to redump
            c.mapping = json.dumps(c.mapping, indent=2)
        return render("sources/mapping.html")

    @requires("user")
    def mapping_save(self, package, resource):
        self._load_ckan(package, resource)
        mapping = dict(json.loads(unquote(request.body).strip('=')))
        model = save_model(c.account, dataset=c.dataset, mapping=mapping)
        return json.dumps({"url": h.url_for(
                    controller='sources',
                    action='load',
                    package=c.package.get('name'),
                    resource=c.resource.get('id'),
                    model=str(model))})

    @jsonp.jsonpify
    def model(self, id):
        model = Model.by_id(id)
        if model is None:
            abort(404)
        return model

    @requires("admin")
    def load(self, package, resource, model):
        use_celery = config.get('openspending.use_celery', 'true') != 'false'
        max_errors = 25 # TODO: make this tunable

        from openspending.etl.ui.lib.tasks import load_dataset
        self._load_ckan(package, resource)
        model = Model.by_id(model)
        if model is None:
            abort(404)
        if use_celery:
            result = load_dataset.delay(c.resource.get('url'), model,
                                        max_errors=max_errors)
            return redirect(url_for(controller="sources", action="task",
                                    operation="load",
                                    task_id=result.task_id))
        else:
            c.result = load_dataset(c.resource.get('url'), model,
                                    max_errors=max_errors)
            c.template = 'sources/started.html'
            return render(c.template)

    def task(self, operation, task_id):
        from openspending.etl.ui.lib.tasks import load_dataset
        result = load_dataset.AsyncResult(task_id)

        c.result = result
        assert operation in ("store", "load")
        c.operation = operation
        c.template = "sources/task.html"
        return render(c.template)

def _url_or_error_for_package(pkg, hint):
    try:
        r = pkg.openspending_resource(hint)
        if r:
            return "<a href='%(url)s'>%(url)s</a>" % {"url": r["url"]}
        else:
            return "None set"
    except ckan.ResourceError as e:
        return "<span class='error-message'>%s</span>" % h.escape(e)