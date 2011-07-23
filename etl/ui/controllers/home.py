from pylons import config, url, tmpl_context as c

from openspending.ui.lib import authz

from openspending.etl.command import daemon
from openspending.etl.ui.lib.base import BaseController, render, redirect

class HomeController(BaseController):
    def index(self):
        return render('home/index.html')

    @authz.requires('admin')
    def drop_database(self):
        c.job_id = 'drop_database'

        if daemon.job_running(c.job_id):
            return render('home/drop_database.html')
        else:
            daemon.dispatch_job(
                job_id=c.job_id,
                config=config['__file__'],
                task='drop_collections'
            )
            return redirect(url(controller='job', action='status', job_id=c.job_id))
