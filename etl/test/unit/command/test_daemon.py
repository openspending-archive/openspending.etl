import sys
import time
from StringIO import StringIO

# import daemon as daemon_lib
from pylons import config

from openspending.etl.test import TestCase, helpers as h
from openspending.etl.command import daemon

class TestDaemon(TestCase):
    def setup(self):
        super(TestDaemon, self).setup()

        self.daemoncontext_obj = MockDaemonContext()
        self.pidlockfile_obj = MockPIDLockFile()

        self.patcher_dc = h.patch('openspending.etl.command.daemon.DaemonContext')
        self.mock_dc = self.patcher_dc.start()
        self.mock_dc.return_value = self.daemoncontext_obj

        self.patcher_pf = h.patch('openspending.etl.command.daemon.PIDLockFileZeroTimeout')
        self.mock_pf = self.patcher_pf.start()
        self.mock_pf.return_value =  self.pidlockfile_obj

        self.patcher_dj = h.patch('openspending.etl.command.daemon.dispatch_job')
        self.mock_dj = self.patcher_dj.start()
        self.mock_dj.side_effect = mock_dispatch_job

    def teardown(self):
        self.patcher_dc.stop()
        self.patcher_pf.stop()
        self.patcher_dj.stop()

        super(TestDaemon, self).teardown()

    def test_dispatch_job(self):
        daemon.dispatch_job('test', config['__file__'], 'test_noop')

    def test_job_log_stdout(self):
        daemon.dispatch_job('test', config['__file__'], 'test_stdout')
        h.assert_equal(self.daemoncontext_obj.stdout.getvalue(),
                       'Text to standard out\n')

    def test_job_log_stderr(self):
        daemon.dispatch_job('test', config['__file__'], 'test_stderr')
        h.assert_equal(self.daemoncontext_obj.stderr.getvalue(),
                       'Text to standard error\n')

    def test_args(self):
        args = ('one', '123', 'abc')
        daemon.dispatch_job('test', config['__file__'], 'test_args', args)

        h.assert_equal(self.daemoncontext_obj.stdout.getvalue(),
                       "('one', '123', 'abc')\n")

    @h.raises(daemon.TaskNotFoundError)
    def test_dispatch_job_nonexistent_task(self):
        daemon.dispatch_job('test', config['__file__'], 'test_nonexistent')

    def test_logfile_path(self):
        h.assert_equal(daemon.logfile_path('test'),
                       sys.prefix + '/var/log/openspendingetld_test.log')

    def test_pidfile_path(self):
        h.assert_equal(daemon.pidfile_path('test'),
                       sys.prefix + '/var/run/openspendingetld_test.pid')

    @h.raises(daemon.AlreadyLocked)
    def test_dispatch_when_locked(self):
        self.pidlockfile_obj.locked = True
        daemon.dispatch_job('test', config['__file__'], 'test_noop')

class MockDaemonContext(object):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        self.stdout = StringIO()
        self.stderr = StringIO()
        sys.stdout = self.stdout
        sys.stderr = self.stderr

    def __exit__(self, exc_type, exc_value, tb):
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

class MockPIDLockFile(object):
    def __init__(self, *args, **kwargs):
        self.locked = False

    def is_locked(self):
        return self.locked

# dispatch_job would usually shell out to start a new daemon, but for
# testing we want to run the job in the current thread.
def mock_dispatch_job(job_id, config, task, args=None):
    if args is None:
        args = ()

    daemon.run_job(job_id, config, task, *args)
