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
a unix pidfile in $PREFIX/var/run/openspendingetld_<job_id>.pid), a config file
path, and a task to run. The task name is assumed to be a top-level function
in the `openspending.etl.tasks` module.

    $ openspendingetld myjob123 development.ini heavy_processing_task

Arguments can also be passed to the task:

    $ openspendingetld myjob123 development.ini heavy_processing_task foo bar

This process will return as soon as the job has daemonized.

The daemonized job will write both STDOUT and STDERR to
$PREFIX/var/log/openspendingetld_<job_id>.log
"""

from __future__ import absolute_import, print_function

import os
import sys
import logging
import subprocess

from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile, AlreadyLocked
from paste.deploy import appconfig

from openspending.etl import tasks
from openspending.etl.ui.config.environment import load_environment


class TaskNotFoundError(Exception):
    pass


class PIDLockFileZeroTimeout(PIDLockFile):
    """A simple subclass of PIDLockFile to curry the timeout to zero."""

    def acquire(self, *args, **kwargs):
        kwargs.update({'timeout': 0})
        super(PIDLockFileZeroTimeout, self).acquire(*args, **kwargs)


def logfile_path(job_id):
    """Return path to log file for job <job_id>"""
    return os.path.join(sys.prefix, 'var', 'log', 'openspendingetld_%s.log' % job_id)

def pidfile_path(job_id):
    """Return path to pid file for job <job_id>"""
    return os.path.join(sys.prefix, 'var', 'run', 'openspendingetld_%s.pid' % job_id)

def job_running(job_id):
    """\
    Return True if job <job_id> is considered to be running by presence of pid
    file.
    """
    return PIDLockFile(pidfile_path(job_id)).is_locked()

def dispatch_job(job_id, config, task, args=None):
    """\
    Helper function to dispatch a job that will then daemonize.
    """
    if args is None:
        args = ()

    cmd = ['openspendingetld', job_id, config, task]
    cmd.extend(args)

    try: # python >= 2.7
        return subprocess.check_output(cmd)
    except AttributeError:
        return _check_output(cmd)

def job_log(job_id):
    """Return contents of log for job <job_id>"""
    with open(logfile_path(job_id)) as f:
        return f.read()

def main():
    args = sys.argv[1:]

    if not len(args) >= 3:
        print("Usage: openspendingetld <job_id> <config_file> <task> [args, ...]" % args,
              file=sys.stderr)
        sys.exit(1)

    # No two jobs with the same job_id can run at the same time
    job_id = args.pop(0)
    configfile_path = os.path.abspath(args.pop(0))
    task = args.pop(0)

    _create_directories()

    pidfile = PIDLockFile(pidfile_path(job_id))

    context = DaemonContext(
        stdout=open(logfile_path(job_id), 'w+'),
        stderr=open(logfile_path(job_id), 'w+', buffering=0),
        pidfile=pidfile
    )

    # NB: There *is* a possible race condition here, if a job with the same
    # name is able to start between this job calling is_locked() below, and
    # acquiring the lock when daemonizing.
    #
    # The problem is that we want to provide immediate feedback to the
    # web front end, which calls this file as "openspendingetld", that it
    # is trying to start a job with an already used job_id, without having
    # to open another log file.
    #
    # This is unlikely to crop up in practice, but ideally we should FIXME.
    if pidfile.is_locked():
        raise AlreadyLocked("Can't start two jobs with id '%s'!" % job_id)

    with context:
        try:
            # Configure logger
            log = logging.getLogger('openspending.etl')
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s',
                '%Y-%m-%d %H:%M:%S'
            ))
            log.addHandler(handler)
            log.setLevel(logging.INFO)

            # Load pylons environment from specified config file
            _load_environment(configfile_path)

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

# Copied from python 2.7's subprocess.py
def _check_output(*popenargs, **kwargs):
    r"""Run command with arguments and return its output as a byte string.

    If the exit code was non-zero it raises a CalledProcessError.  The
    CalledProcessError object will have the return code in the returncode
    attribute and output in the output attribute.

    The arguments are the same as for the Popen constructor.  Example:

    >>> check_output(["ls", "-l", "/dev/null"])
    'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'

    The stdout argument is not allowed as it is used internally.
    To capture standard error in the result, use stderr=STDOUT.

    >>> check_output(["/bin/sh", "-c",
    ...               "ls -l non_existent_file ; exit 0"],
    ...              stderr=STDOUT)
    'ls: non_existent_file: No such file or directory\n'
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        err = subprocess.CalledProcessError(retcode, cmd)
        err.output = output
        raise err
    return output

if __name__ == '__main__':
    main()
