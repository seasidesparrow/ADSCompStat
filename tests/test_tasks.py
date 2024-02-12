import json
import os
import unittest

from mock import patch

from adscompstat import app
from adscompstat import database as db
from adscompstat import tasks, utils
from adscompstat.models import Base, CompStatMaster as master


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

        with patch.object(
            db, "clear_classic_data", side_effect=Exception()
        ) as next_task, patch.object(tasks.logger, "warning") as tasks.logger.warning:
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

    def test_task_process_meta(self):
        # test1: empty xml document
        infile = os.path.join(self.inputdir, "test_null.xml")
        expected_call = (infile, "", "{}", "{}", "{}", "Failed", "failed", "", "", "error: 'NoneType' object has no attribute 'extract'")
        with patch.object(db, "query_master_by_doi", return_value=[]) as db.query_master_by_doi, patch.object(db, "write_matched_record") as db.write_matched_record, patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task:
            tasks.task_process_meta([infile])
            next_task.assert_called_with(expected_call)

        # test2: valid xml document, but you can't supply a bibstem to bibcode generator, nothing returned from doi query
        infile = os.path.join(self.inputdir, "test_metadata.xml")
        expected_call=(infile, "10.3847/0004-637X/816/1/36", '{"electronic": "1538-4357"}', '{"publication": {"pubName": "The Astrophysical Journal", "issueNum": "1", "volumeNum": "816", "pubYear": "2016", "ISSN": [{"pubtype": "electronic", "issnString": "1538-4357"}]}, "pagination": {"firstPage": "36"}, "persistentIDs": [{"DOI": "10.3847/0004-637X/816/1/36"}], "first_author": {"name": {"surname": "Xiong", "given_name": "Gang"}}, "title": {"textEnglish": "OPACITY MEASUREMENT AND THEORETICAL INVESTIGATION OF HOT SILICON PLASMA"}}', "{}", "Failed", "failed", "", "", "You're missing year and or bibstem -- no bibcode can be made!")
        with patch.object(db, "query_bibstem", return_value=None) as db.query_bibstem, patch.object(db, "query_master_by_doi", return_value=[]) as db.query_master_by_doi, patch.object(db, "write_matched_record") as db.write_matched_record, patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task:
            tasks.task_process_meta([infile])
            next_task.assert_called_with(expected_call)

        # test3: valid xml document, successfully made a bibcode, nothing returned from doi query
        infile = os.path.join(self.inputdir, "test_metadata.xml")
        expected_call=(infile, "10.3847/0004-637X/816/1/36", '{"electronic": "1538-4357"}', '{"publication": {"pubName": "The Astrophysical Journal", "issueNum": "1", "volumeNum": "816", "pubYear": "2016", "ISSN": [{"pubtype": "electronic", "issnString": "1538-4357"}]}, "pagination": {"firstPage": "36"}, "persistentIDs": [{"DOI": "10.3847/0004-637X/816/1/36"}], "first_author": {"name": {"surname": "Xiong", "given_name": "Gang"}}, "title": {"textEnglish": "OPACITY MEASUREMENT AND THEORETICAL INVESTIGATION OF HOT SILICON PLASMA"}}', '{"DOI": "DOI not in classic"}', "Unmatched", "unmatched", "2016ApJ...816...36X", None, "")
        with patch.object(db, "query_bibstem", return_value='ApJ') as db.query_bibstem, patch.object(db, "query_master_by_doi", return_value=[]) as db.query_master_by_doi, patch.object(db, "write_matched_record") as db.write_matched_record, patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task:
            tasks.task_process_meta([infile])
            next_task.assert_called_with(expected_call)

        # test4: valid xml document, successfully made a bibcode, bibcode is in classic but doi is not
        infile = os.path.join(self.inputdir, "test_metadata.xml")
        expected_call=(infile, "10.3847/0004-637X/816/1/36", '{"electronic": "1538-4357"}', '{"publication": {"pubName": "The Astrophysical Journal", "issueNum": "1", "volumeNum": "816", "pubYear": "2016", "ISSN": [{"pubtype": "electronic", "issnString": "1538-4357"}]}, "pagination": {"firstPage": "36"}, "persistentIDs": [{"DOI": "10.3847/0004-637X/816/1/36"}], "first_author": {"name": {"surname": "Xiong", "given_name": "Gang"}}, "title": {"textEnglish": "OPACITY MEASUREMENT AND THEORETICAL INVESTIGATION OF HOT SILICON PLASMA"}}', '{"DOI": "DOI not in classic"}', "Matched", "canonical", "2016ApJ...816...36X", "2016ApJ...816...36X", "")
        with patch.object(db, "query_bibstem", return_value='ApJ') as db.query_bibstem, patch.object(db, "query_master_by_doi", return_value="") as db.query_master_by_doi, patch.object(db, "query_classic_bibcodes", return_value=([],[("2016ApJ...816...36X", "2016ApJ...816...36X", "canonical")])) as db.query_classic_bibcodes, patch.object(db, "write_matched_record") as db.write_matched_record, patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task:
            tasks.task_process_meta([infile])
            next_task.assert_called_with(expected_call)

        # test5: valid xml document, successfully made a bibcode, matching bibcode from doi query
        infile = os.path.join(self.inputdir, "test_metadata.xml")
        expected_call=(infile, "10.3847/0004-637X/816/1/36", '{"electronic": "1538-4357"}', '{"publication": {"pubName": "The Astrophysical Journal", "issueNum": "1", "volumeNum": "816", "pubYear": "2016", "ISSN": [{"pubtype": "electronic", "issnString": "1538-4357"}]}, "pagination": {"firstPage": "36"}, "persistentIDs": [{"DOI": "10.3847/0004-637X/816/1/36"}], "first_author": {"name": {"surname": "Xiong", "given_name": "Gang"}}, "title": {"textEnglish": "OPACITY MEASUREMENT AND THEORETICAL INVESTIGATION OF HOT SILICON PLASMA"}}', "{}", "Matched", "canonical", "2016ApJ...816...36X", "2016ApJ...816...36X", "")
        with patch.object(db, "query_bibstem", return_value='ApJ') as db.query_bibstem, patch.object(db, "query_master_by_doi", return_value="2016ApJ...816...36X") as db.query_master_by_doi, patch.object(db, "query_classic_bibcodes", return_value=([("2016ApJ...816...36X", "2016ApJ...816...36X", "canonical")],[("2016ApJ...816...36X", "2016ApJ...816...36X", "canonical")])) as db.query_classic_bibcodes, patch.object(db, "write_matched_record") as db.write_matched_record, patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task:
            tasks.task_process_meta([infile])
            next_task.assert_called_with(expected_call)


        # test6: valid xml document, successfully made a bibcode, doi is wrong in classic
        infile = os.path.join(self.inputdir, "test_metadata.xml")
        expected_call=(infile, "10.3847/0004-637X/816/1/36", '{"electronic": "1538-4357"}', '{"publication": {"pubName": "The Astrophysical Journal", "issueNum": "1", "volumeNum": "816", "pubYear": "2016", "ISSN": [{"pubtype": "electronic", "issnString": "1538-4357"}]}, "pagination": {"firstPage": "36"}, "persistentIDs": [{"DOI": "10.3847/0004-637X/816/1/36"}], "first_author": {"name": {"surname": "Xiong", "given_name": "Gang"}}, "title": {"textEnglish": "OPACITY MEASUREMENT AND THEORETICAL INVESTIGATION OF HOT SILICON PLASMA"}}', '{"DOI": "DOI mismatched", "bibcode": "2016ApJ...816...36X"}', "Matched", "mismatch", "2016ApJ...816...36X", "2016FAKEY.123..456Z", "")
        with patch.object(db, "query_bibstem", return_value='ApJ') as db.query_bibstem, patch.object(db, "query_master_by_doi", return_value="2016FAKEY.123..456Z") as db.query_master_by_doi, patch.object(db, "query_classic_bibcodes", return_value=([("2016FAKEY.123..456Z", "2016FAKEY.123..456Z", "canonical")],[("2016ApJ...816...36X", "2016ApJ...816...36X", "canonical")])) as db.query_classic_bibcodes, patch.object(db, "write_matched_record") as db.write_matched_record, patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task:
            tasks.task_process_meta([infile])
            next_task.assert_called_with(expected_call)



    def test_task_retry_records(self):
        record_meta_file = os.path.join(self.inputdir, "test_metadata.xml")
        # default: app.conf.get("RECORDS_PER_BATCH", 100)
        # if you had 147 records, you process one batch of 100, and one of 47
        retries = [record_meta_file for i in range(0, 147)]
        with patch.object(
            db, "query_retry_files", return_value=retries
        ) as db.query_retry_files, patch.object(tasks.task_process_meta, "delay") as next_task:
            tasks.task_retry_records("test")
            db.query_retry_files.assert_called_once()
            self.assertEqual(next_task.call_count, 2)

        # different value of RECORDS_PER_BATCH
        self.app.conf["RECORDS_PER_BATCH"] = 25
        with patch.object(
            db, "query_retry_files", return_value=retries
        ) as db.query_retry_files, patch.object(tasks.task_process_meta, "delay") as next_task:
            tasks.task_retry_records("test")
            db.query_retry_files.assert_called_once()
            self.assertEqual(next_task.call_count, 6)


"""
    def test_task_process_meta(self):
        record_meta_file = os.path.join(self.inputdir, "test_metadata.xml")
        with patch.object(tasks.task_write_matched_record_to_db, "delay") as next_task, patch.object(tasks, "db_query_bibstem", return_value="ApJ") as tasks.db_query_bibstem:
            tasks.task_process_meta([record_meta_file])
            next_task.assert_called_with
"""
