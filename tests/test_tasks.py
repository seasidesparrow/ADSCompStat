import os
import unittest
from mock import Mock, patch

from adscompstat import app, tasks
from adscompstat.models import Base, CompStatMaster


class TestTasks(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), "..")
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")
        self._app = tasks.app
        self.app = app.ADSCompStatCelery(
            'test',
            local_config={
                'SQLALCHEMY_URL': 'sqlite:///',
                'SQLALCHEMY_ECHO': False,
                'RECORDS_PER_BATCH': 100,
            }
        )
        tasks.app = self.app # monkey-patch the app object

        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app

    def test__process_logfile(self):
        
        # input file has 20 records, hence exactly one batch
        logfile = os.path.join(self.inputdir, "10.1186:300627.out.2023-12-10")
        with patch.object(tasks.task_process_meta, "delay") as next_task:
            self.assertEqual(next_task.call_count, 0)
            tasks.task_process_logfile(logfile)
            self.assertEqual(next_task.call_count, 1)

        # input file has 101 records, hence two batches
        logfile = os.path.join(self.inputdir, "10.1186:300627.long-fake.2023-12-10")
        with patch.object(tasks.task_process_meta, "delay") as next_task:
            self.assertEqual(next_task.call_count, 0)
            tasks.task_process_logfile(logfile)
            self.assertEqual(next_task.call_count, 2)

    @patch('adscompstat.tasks.app.session_scope')
    def test__retry_records(self, mock_session):
    #def test__retry_records(self):
        fake_files = ["file."+str(t) for t in range(0,205)]
        # with patch(mock_adscompstat_tasks.app.session_scope.return_value.query.return_value.filter.return_value.all.return_value, return_value=fake_files), patch.object(tasks.task_process_meta, "delay") as next_task:
        with patch(mock_session.return_value.query.return_value.filter.return_value.all.return_value, return_value=fake_files), patch.object(tasks.task_process_meta, "delay") as next_task:
            tasks.task_retry_records("failed")
            self.assertEqual(next_task.call_count, 3)
















