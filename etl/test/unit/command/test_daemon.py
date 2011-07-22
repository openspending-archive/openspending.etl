import sys
import time

from pylons import config

from openspending.etl.test import TestCase, helpers as h
from openspending.etl.command import daemon


class TestDaemon(TestCase):
    def setup(self):
        super(TestDaemon, self).setup()

    # We need to find a better way to test this. My attempts to monkeypatch
    # mocks into openspending.etl.tasks haven't worked, perhaps because of
    # the daemonization process?
    def test_dispatch_job(self):
        daemon.dispatch_job('test', config['__file__'], 'test_noop')

    def test_job_log_stdout(self):
        daemon.dispatch_job('test', config['__file__'], 'test_stdout')

        while daemon.job_running('test'):
            time.sleep(0.1)

        h.assert_equal(daemon.job_log('test'), 'Text to standard out\n')

    def test_job_log_stderr(self):
        daemon.dispatch_job('test', config['__file__'], 'test_stderr')

        while daemon.job_running('test'):
            time.sleep(0.1)

        h.assert_equal(daemon.job_log('test'), 'Text to standard error\n')

    def test_args(self):
        args = ('one', '123', 'abc')
        daemon.dispatch_job('test', config['__file__'], 'test_args', args)

        while daemon.job_running('test'):
            time.sleep(0.1)

        h.assert_equal(daemon.job_log('test'), "('one', '123', 'abc')\n")

    def test_dispatch_job_nonexistent_task(self):
        daemon.dispatch_job('test', config['__file__'], 'test_nonexistent')

        while daemon.job_running('test'):
            time.sleep(0.1)

        assert 'TaskNotFoundError' in daemon.job_log('test'), \
               "TaskNotFoundError not in job log for nonexistent task!"

    def test_logfile_path(self):
        h.assert_equal(daemon.logfile_path('test'),
                       sys.prefix + '/var/log/openspendingetld_test.log')

    def test_pidfile_path(self):
        h.assert_equal(daemon.pidfile_path('test'),
                       sys.prefix + '/var/run/openspendingetld_test.pid')

    def test_dispatch_twice(self):
        h.skip("FIXME: work out how to test that AlreadyLocked gets thrown.")

    def teardown(self):
        super(TestDaemon, self).teardown()

