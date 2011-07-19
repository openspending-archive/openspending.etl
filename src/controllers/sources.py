from pylons import config

from urllib import unquote_plus as unquote

from pylons import request, tmpl_context as c, url
from pylons.controllers.util import abort, redirect
from pylons.i18n import _
from routes import url_for

from openspending.etl.ui.lib import ckan
from openspending.etl.ui.lib import json, jsonp
from openspending.etl.ui.lib import helpers as h
from openspending.etl.ui.lib.authz import requires
from openspending.etl.ui.lib.base import BaseController, render
from openspending.etl.ui.lib.csvimport import DatasetImporter
from openspending.etl.ui.lib.resourceimport import package_and_resource, load_from_ckan
from openspending.etl.ui.lib.csvimport import validate_model, resource_lines, load_dataset
from openspending.etl.ui.logic.model import save_model, available_models
from openspending.etl.ui.model import Dataset, Model

import logging
log = logging.getLogger(__name__)

class SourcesController(BaseController):

    def index(self, error=False, form_fields=None):
        ckan_client = ckan.get_client()
        c.packages = []
        c.group = ckan_client.group_entity_get(ckan.openspending_group)
        for pkg_name in c.group.get('packages'):
            try:
                c.packages.append(ckan_client.package_entity_get(pkg_name))
            except ckan.CkanApiError, cae:
                log.error(cae)
        c.template = 'sources/index.html'
        return render(c.template)

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
        use_celery = config.get('use_celery', 'true') != 'false'
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
