from pylons import request, tmpl_context as c

from openspending.etl.command import daemon
from openspending.etl.ui.lib.base import BaseController, render

class JobController(BaseController):
    def status(self, job_id):
        c.job_id = job_id
        c.job_running = daemon.job_running(job_id)
        c.job_log = daemon.job_log(job_id)

        if request.is_xhr:
            return render('job/_status.html')
        else:
            return render('job/status.html')