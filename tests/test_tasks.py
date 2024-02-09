import json
import os
import unittest
from mock import patch, Mock

from adscompstat import app, database as db, tasks, utils
from adscompstat.models import Base


class TestTasks(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")
        self.maxDiff = None
        self.proj_home = os.path.join(os.path.dirname(__file__), "../..")
        self._app = tasks.app
        self.app = app.ADSCompStatCelery(
            "test", 
            local_config={"SQLALCHEMY_URL": "sqlite:///", "SQLALCHEMY_ECHO": False},
        )   
        tasks.app = self.app  # monkey-path the app object
            
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def test_task_clear_classic_data(self):
        with patch.object(db, "clear_classic_data") as next_task:
            tasks.task_clear_classic_data()
            self.assertEqual(next_task.call_count, 1)

        with patch.object(db, "clear_classic_data", side_effect=Exception("Existing classic data tables not cleared: ''")) as next_task, patch.object(tasks.logger, "warning") as tasks.logger.warning:
            tasks.task_clear_classic_data()
            self.assertEqual(tasks.logger.warning.call_count, 1)


    def test_task_write_block(self):
        with patch.object(db, "write_block") as next_task:
            tasks.task_write_block(Base, [])
            self.assertEqual(next_task.call_count, 1)




    def test_task_process_logfile(self):
        infile = os.path.join(self.inputdir, "test_log.txt")
        with patch.object(tasks.task_process_meta, "delay") as next_task:
            tasks.task_process_logfile(infile)
            self.assertEqual(next_task.call_count, 2)


    def test_task_retry_records(self):
        record_meta_file = os.path.join(self.inputdir, "test_metadata.xml")
        # if you had 147 records, you process one batch of 100, and one of 47
        retries = [record_meta_file for i in range(0,147)]
        with patch.object(db, "query_retry_files", return_value=retries) as db.query_retry_files, patch.object(tasks.task_process_meta, "delay") as next_task:
            tasks.task_retry_records("test")
            db.query_retry_files.assert_called_once()
            self.assertEqual(next_task.call_count, 2)


"""
    def test_task_write_matched_record_to_db(self):
        record_meta_file = os.path.join(self.inputdir, "test_metadata.xml")
        processedRecord = utils.process_one_meta_xml(record_meta_file)
        match = {"match": "canonical",
                 "bibcode": "2016ApJ...816...36X",
                 "errs": {}}
        query_classic_bibs_return = ([match], [match])
        with patch.object(db, "query_classic_bibcodes", return_value=query_classic_bibs_return) as db.query_classic_bibcodes,:


    def test_task_process_meta(self):
        record_meta_file = os.path.join(self.inputdir, "test_metadata.xml")
        with patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task, patch.object(tasks, "db_query_bibstem", return_value="ApJ") as tasks.db_query_bibstem:
            tasks.task_process_meta([record_meta_file])
            next_task.assert_called_with
"""


