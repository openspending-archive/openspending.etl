from pylons import config, url, tmpl_context as c

from openspending import model
from openspending.ui.lib import authz

from openspending.etl.command import daemon
from openspending.etl.ui.lib.base import BaseController, render, redirect

class TaskController(BaseController):

    @authz.requires('admin')
    def drop_database(self):
        c.job_id = 'drop_database'

        if daemon.job_running(c.job_id):
            return render('task/drop_database.html')

        daemon.dispatch_job(
            job_id=c.job_id,
            config=config['__file__'],
            task='drop_collections'
        )
        return redirect(url(controller='job', action='status', job_id=c.job_id))


    @authz.requires('admin')
    def remove_dataset(self, dataset=None):
        if dataset is None:
            c.datasets = model.Dataset.find()
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
