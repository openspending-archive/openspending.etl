from pylons import config, url, request, tmpl_context as c

from openspending.model import Dataset, meta as db
#from openspending.ui.lib import authz

from openspending.etl.command import daemon
from openspending.etl.importer import ckan
from openspending.etl.ui.lib.base import BaseController, render, redirect

class TaskController(BaseController):

    #@authz.requires('admin')
    def drop_database(self):
        c.job_id = 'drop_database'

        if daemon.job_running(c.job_id):
            return render('task/drop_database.html')

        daemon.dispatch_job(
            job_id=c.job_id,
            config=config['__file__'],
            task='drop_datasets'
        )
        return redirect(url(controller='job', action='status', job_id=c.job_id))


    #@authz.requires('admin')
    def remove_dataset(self, dataset=None):
        if dataset is None:
            c.datasets = db.session.query(Dataset).all()
            return render('task/remove_dataset.html')

        c.job_id = 'remove_%s' % dataset
        c.job_running = daemon.job_running(c.job_id)

        if c.job_running:
            return render('task/remove_dataset.html')

        daemon.dispatch_job(
            job_id=c.job_id,
            config=config['__file__'],
            task='remove_dataset',
            args=(dataset,)
        )
        return redirect(url(controller='job', action='status', job_id=c.job_id))

    def add_hint(self):
        pkg = request.params['pkg']
        resource_id = request.params['resource_id']
        prev_resource_id = request.params['prev_resource_id']
        hint = request.params['hint']

        p = ckan.Package(pkg)

        if prev_resource_id != '' and prev_resource_id != resource_id:
            p.remove_hint(prev_resource_id)

        p.add_hint(resource_id, hint)

        redirect(url(request.referer))
