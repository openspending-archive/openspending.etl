#
# daemon.py, or openspendingetld (see entry point in setup.py)

"""
This file is a replacement for the Celery background job system, used to run
imports in the background. It runs jobs from `openspending.etl.tasks` as
daemonized processes, and pipes their output to a log file which can be read by
the web front end.

Before starting the job, the pylons app environment is loaded from a specified
config file.

At it's simplest, it is passed a unique job id (uniqueness is enforced using
a unix pidfile in $PREFIX/var/run/openspendingetld_<jobid>.pid), a config file
path, and a task to run. The task name is assumed to be a top-level function
in the `openspending.etl.tasks` module.

    $ openspendingetld myjob123 development.ini heavy_processing_task

Arguments can also be passed to the task:

    $ openspendingetld myjob123 development.ini heavy_processing_task foo bar

This process will return as soon as the job has daemonized.

The daemonized job will write both STDOUT and STDERR to
$PREFIX/var/log/openspendingetld_<jobid>.log
"""

from __future__ import absolute_import, print_function

import os
import sys
import logging

from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile, AlreadyLocked
from paste.deploy import appconfig

from openspending.etl import tasks
from openspending.etl.ui.config.environment import load_environment


class TaskNotFoundError(Exception):
    pass


class PIDLockFileZeroTimeout(PIDLockFile):
    def acquire(self, *args, **kwargs):
        kwargs.update({'timeout': 0})
        super(PIDLockFileZeroTimeout, self).acquire(*args, **kwargs)


def main():
    args = sys.argv[1:]

    if not len(args) >= 3:
        print("Usage: openspendingetld <jobid> <config_file> <task> [args, ...]" % args,
              file=sys.stderr)
        sys.exit(1)

    # No two jobs with the same jobid can run at the same time
    jobid = args.pop(0)
    configfile_path = os.path.abspath(args.pop(0))
    task = args.pop(0)

    _create_directories()

    logfile_path = os.path.join(sys.prefix, 'var',
                                'log', 'openspendingetld_%s.log' % jobid)
    pidfile_path = os.path.join(sys.prefix, 'var',
                                'run', 'openspendingetld_%s.pid' % jobid)

    pidfile = PIDLockFile(pidfile_path)

    context = DaemonContext(
        stdout=open(logfile_path, 'w+'),
        stderr=open(logfile_path, 'w+', buffering=0),
        pidfile=pidfile
    )

    # NB: There *is* a possible race condition here, if a job with the same
    # name is able to start between this job calling is_locked() above, and
    # acquiring the lock when daemonizing.
    #
    # The problem is that we want to provide immediate feedback to the
    # web front end, which calls this file as "openspendingetld", that it
    # is trying to start a job with an already used jobid, without having
    # to open another log file.
    #
    # This is unlikely to crop up in practice, but ideally we should FIXME.
    if pidfile.is_locked():
        raise AlreadyLocked("Can't start two jobs with id '%s'!" % jobid)

    with context:
        try:
            # Configure logger
            log = logging.getLogger('openspending.etl')
            log.addHandler(logging.StreamHandler(sys.stderr))
            log.setLevel(logging.INFO)

            # Load pylons environment from specified config file
            load_environment(configfile_path)

            # Run task, passing leftover arguments
            tasks.__dict__[task](*args)
        except KeyError:
            raise TaskNotFoundError("No task called '%s' exists in openspending.tasks!" % task)


def _load_environment(configfile_path):
    conf = appconfig('config:' + configfile_path)
    load_environment(conf.global_conf, conf.local_conf)

def _create_directories():
    var = os.path.join(sys.prefix, 'var')
    run = os.path.join(var, 'run')
    log = os.path.join(var, 'log')

    for d in (var, run, log):
        if not os.path.isdir(d):
            os.mkdir(d)

if __name__ == '__main__':
    main()