#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `diseasescope_taskrunner` script."""

import os
import json
import unittest
import shutil
import tempfile
import diseasescope_rest_server
from diseasescope_rest_server.dao import FileBasedTask
from diseasescope_rest_server.dao import FileBasedSubmittedTaskFactory
from diseasescope_rest_server.dao import DeletedFileBasedTaskFactory
from diseasescope_rest_server import dao


class TestDAO(unittest.TestCase):
    """Tests for `dao` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        pass

    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def test_filebasedtask_getter_setter_on_basic_obj(self):

        task = FileBasedTask(None, None)
        self.assertEqual(task.get_task_uuid(), None)
        self.assertEqual(task.get_ipaddress(), None)
        self.assertEqual(task.get_diseaseid(), None)
        self.assertEqual(task.get_state(), None)
        self.assertEqual(task.get_taskdict(), None)
        self.assertEqual(task.get_taskdir(), None)

        self.assertEqual(task.get_task_summary_as_str(),
                         "{'basedir': None, 'state': None,"
                         " 'ipaddr': None, 'uuid': None}")

        task.set_taskdir('/foo')
        self.assertEqual(task.get_taskdir(), '/foo')

        task.set_taskdict({ dao.DOID_PARAM: 1234})
        self.assertEqual(task.get_diseaseid(), 1234)

        task.set_taskdict({})
        self.assertEqual(task.get_diseaseid(), None)

        task.set_taskdict({ dao.DOID_PARAM: 2})
        self.assertEqual(task.get_diseaseid(), 2)

    def test_filebasedtask_get_uuid_ip_state_basedir_from_path(self):
        # taskdir is none
        task = FileBasedTask(None, None)
        res = task._get_uuid_ip_state_basedir_from_path()
        self.assertEqual(res[FileBasedTask.BASEDIR], None)
        self.assertEqual(res[FileBasedTask.STATE], None)
        self.assertEqual(res[FileBasedTask.IPADDR], None)
        self.assertEqual(res[FileBasedTask.UUID], None)

        # too basic a path
        task.set_taskdir('/foo')
        res = task._get_uuid_ip_state_basedir_from_path()
        self.assertEqual(res[FileBasedTask.BASEDIR], '/')
        self.assertEqual(res[FileBasedTask.STATE], None)
        self.assertEqual(res[FileBasedTask.IPADDR], None)
        self.assertEqual(res[FileBasedTask.UUID], 'foo')

        # valid path
        task.set_taskdir('/b/submitted/i/myjob')
        res = task._get_uuid_ip_state_basedir_from_path()
        self.assertEqual(res[FileBasedTask.BASEDIR], '/b')
        self.assertEqual(res[FileBasedTask.STATE], 'submitted')
        self.assertEqual(res[FileBasedTask.IPADDR], 'i')
        self.assertEqual(res[FileBasedTask.UUID], 'myjob')

        # big path
        task.set_taskdir('/a/c/b/submitted/i/myjob')
        res = task._get_uuid_ip_state_basedir_from_path()
        self.assertEqual(res[FileBasedTask.BASEDIR], '/a/c/b')
        self.assertEqual(res[FileBasedTask.STATE], 'submitted')
        self.assertEqual(res[FileBasedTask.IPADDR], 'i')
        self.assertEqual(res[FileBasedTask.UUID], 'myjob')

    def test_save_task(self):
        temp_dir = tempfile.mkdtemp()
        try:
            task = FileBasedTask(None, None)
            self.assertEqual(task.save_task(), 'Task dir is None')

            # try with None for dictionary
            task.set_taskdir(temp_dir)
            self.assertEqual(task.save_task(), 'Task dict is None')

            # try with taskdir set to file
            task.set_taskdict('hi')
            somefile = os.path.join(temp_dir, 'somefile')
            open(somefile, 'a').close()
            task.set_taskdir(somefile)
            self.assertEqual(task.save_task(), somefile +
                             ' is not a directory')

            # try with string set as dictionary
            task.set_taskdict('hi')
            task.set_taskdir(temp_dir)
            self.assertEqual(task.save_task(), None)

            task.set_taskdict({'blah': 'value'})
            self.assertEqual(task.save_task(), None)
            tfile = os.path.join(temp_dir, dao.TASK_JSON)
            with open(tfile, 'r') as f:
                self.assertEqual(f.read(), '{"blah": "value"}')

            # test with fs set
            self.assertEqual(task.save_task(), None)
            rfile = os.path.join(temp_dir, dao.TASK_JSON)
            with open(rfile, 'r') as f:
                self.assertEqual(f.read(), '{"blah": "value"}')
        finally:
            shutil.rmtree(temp_dir)

    def test_move_task(self):
        temp_dir = tempfile.mkdtemp()
        try:
            submitdir = os.path.join(temp_dir, dao.SUBMITTED_STATUS)
            os.makedirs(submitdir, mode=0o755)
            processdir = os.path.join(temp_dir, dao.PROCESSING_STATUS)
            os.makedirs(processdir, mode=0o755)
            donedir = os.path.join(temp_dir, dao.DONE_STATUS)
            os.makedirs(donedir, mode=0o755)

            # try a move on unset task
            task = FileBasedTask(None, None)
            self.assertEqual(task.move_task(dao.PROCESSING_STATUS),
                             'Unable to extract state basedir from task path')

            # try a move from submit to process
            ataskdir = os.path.join(submitdir, '192.168.1.1', 'qwerty-qwerty')
            os.makedirs(ataskdir)
            task = FileBasedTask(ataskdir, {'hi': 'bye'})

            self.assertEqual(task.save_task(), None)

            # try a move from submit to submit
            self.assertEqual(task.move_task(dao.SUBMITTED_STATUS),
                             None)
            self.assertEqual(task.get_taskdir(), ataskdir)

            # try a move from submit to process
            self.assertEqual(task.move_task(dao.PROCESSING_STATUS),
                             None)
            self.assertTrue(not os.path.isdir(ataskdir))
            self.assertTrue(os.path.isdir(task.get_taskdir()))
            self.assertTrue(dao.PROCESSING_STATUS in
                            task.get_taskdir())

            # try a move from process to done
            self.assertEqual(task.move_task(dao.DONE_STATUS),
                             None)
            self.assertTrue(dao.DONE_STATUS in
                            task.get_taskdir())

            # try a move from done to submitted
            self.assertEqual(task.move_task(dao.SUBMITTED_STATUS),
                             None)
            self.assertTrue(dao.SUBMITTED_STATUS in
                            task.get_taskdir())

            # try a move from submitted to error
            self.assertEqual(task.move_task(dao.ERROR_STATUS),
                             None)
            self.assertTrue(dao.DONE_STATUS in
                            task.get_taskdir())
            tjson = os.path.join(task.get_taskdir(), dao.TASK_JSON)
            with open(tjson, 'r') as f:
                data = json.load(f)
                self.assertEqual(data['message'],
                                 'Unknown error')

            # try a move from error to submitted then back to error again
            # with message this time
            self.assertEqual(task.move_task(dao.SUBMITTED_STATUS),
                             None)
            self.assertEqual(task.move_task(dao.ERROR_STATUS,
                                            error_message='bad'),
                             None)
            tjson = os.path.join(task.get_taskdir(), dao.TASK_JSON)
            with open(tjson, 'r') as f:
                data = json.load(f)
                self.assertEqual(data['message'],
                                 'bad')
        finally:
            shutil.rmtree(temp_dir)

    def test_filebasedtask_delete_task_files(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # try where taskdir is none
            task = FileBasedTask(None, None)
            self.assertEqual(task.delete_task_files(),
                             'Task directory is None')

            # try where taskdir is not a directory
            notadir = os.path.join(temp_dir, 'notadir')
            task = FileBasedTask(notadir, None)
            self.assertEqual(task.delete_task_files(),
                             'Task directory ' + notadir +
                             ' is not a directory')

            # try on empty directory
            emptydir = os.path.join(temp_dir, 'emptydir')
            os.makedirs(emptydir, mode=0o755)
            task = FileBasedTask(emptydir, None)
            self.assertEqual(task.delete_task_files(), None)
            self.assertFalse(os.path.isdir(emptydir))

            # try with result, snp, and task.json files
            valid_dir = os.path.join(temp_dir, 'yoyo')
            os.makedirs(valid_dir, mode=0o755)
            open(os.path.join(valid_dir, dao.TASK_JSON),
                 'a').close()

            task = FileBasedTask(valid_dir, {})
            self.assertEqual(task.delete_task_files(), None)
            self.assertFalse(os.path.isdir(valid_dir))

            # try where extra file causes os.rmdir to fail
            valid_dir = os.path.join(temp_dir, 'yoyo')
            os.makedirs(valid_dir, mode=0o755)
            open(os.path.join(valid_dir, 'somefile'), 'a').close()

            open(os.path.join(valid_dir, dao.RESULT), 'a').close()
            open(os.path.join(valid_dir, dao.TASK_JSON),
                 'a').close()

            task = FileBasedTask(valid_dir, {})
            self.assertTrue('trying to remove ' in task.delete_task_files())
            self.assertTrue(os.path.isdir(valid_dir))

        finally:
            shutil.rmtree(temp_dir)

    def test_filebasedsubmittedtaskfactory_get_next_task_taskdirnone(self):
        fac = FileBasedSubmittedTaskFactory(None)
        self.assertEqual(fac.get_next_task(), None)

    def test_filebasedsubmittedtaskfactory_get_next_task(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # no submit dir
            fac = FileBasedSubmittedTaskFactory(temp_dir)
            self.assertEqual(fac.get_next_task(), None)

            # empty submit dir
            sdir = os.path.join(temp_dir, dao.SUBMITTED_STATUS)
            os.makedirs(sdir, mode=0o755)
            self.assertEqual(fac.get_next_task(), None)

            # submit dir with file in it
            sdirfile = os.path.join(sdir, 'somefile')
            open(sdirfile, 'a').close()
            self.assertEqual(fac.get_next_task(), None)

            # submit dir with 1 subdir, but that is empty too
            ipsubdir = os.path.join(sdir, '1.2.3.4')
            os.makedirs(ipsubdir, mode=0o755)
            self.assertEqual(fac.get_next_task(), None)

            # submit dir with 1 subdir, with a file in it for some reason
            afile = os.path.join(ipsubdir, 'hithere')
            open(afile, 'a').close()
            self.assertEqual(fac.get_next_task(), None)

            # empty task dir
            taskdir = os.path.join(ipsubdir, 'sometask')
            os.makedirs(taskdir, mode=0o755)
            self.assertEqual(fac.get_next_task(), None)

            # empty json file
            taskjsonfile = os.path.join(taskdir, dao.TASK_JSON)
            open(taskjsonfile, 'a').close()
            self.assertEqual(fac.get_next_task(), None)
            self.assertEqual(fac.get_size_of_problem_list(), 1)
            plist = fac.get_problem_list()
            self.assertEqual(plist[0], taskdir)

            # try invalid json file

            # try with another task this time valid
            fac = FileBasedSubmittedTaskFactory(temp_dir)
            anothertask = os.path.join(sdir, '4.5.6.7', 'goodtask')
            os.makedirs(anothertask, mode=0o755)
            goodjson = os.path.join(anothertask, dao.TASK_JSON)
            with open(goodjson, 'w') as f:
                json.dump({'hi': 'there'}, f)

            res = fac.get_next_task()
            self.assertEqual(res.get_taskdict(), {'hi': 'there'})
            self.assertEqual(fac.get_size_of_problem_list(), 0)

            # try again since we didn't move it
            res = fac.get_next_task()
            self.assertEqual(res.get_taskdict(), {'hi': 'there'})
            self.assertEqual(fac.get_size_of_problem_list(), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_deletefilebasedtaskfactory_get_task_with_id(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # try where taskdir is not set
            tfac = DeletedFileBasedTaskFactory(None)
            res = tfac._get_task_with_id('foo')
            self.assertEqual(res, None)

            # try with valid taskdir
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac._get_task_with_id('foo')
            self.assertEqual(res, None)

            # try where we match in submit dir, but match is not
            # a directory
            submitfile = os.path.join(temp_dir, dao.SUBMITTED_STATUS,
                                      '1.2.3.4', 'foo')
            os.makedirs(os.path.dirname(submitfile), mode=0o755)
            open(submitfile, 'a').close()
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac._get_task_with_id('foo')
            self.assertEqual(res, None)
            os.unlink(submitfile)

            # try where we match in submit dir, but no json file
            submitdir = os.path.join(temp_dir, dao.SUBMITTED_STATUS,
                                     '1.2.3.4', 'foo')
            os.makedirs(submitdir, mode=0o755)
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac._get_task_with_id('foo')
            self.assertEqual(res.get_taskdir(), submitdir)

            # try where we match in submit dir and there is a json file
            taskfile = os.path.join(submitdir,
                                    dao.TASK_JSON)
            with open(taskfile, 'w') as f:
                json.dump({ diseasescope_rest_server.REMOTEIP_PARAM: '1.2.3.4'}, f)
                f.flush()

            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac._get_task_with_id('foo')
            self.assertEqual(res.get_taskdir(), submitdir)
            self.assertEqual(res.get_ipaddress(), '1.2.3.4')

            # try where loading json file raises exception
            os.unlink(taskfile)
            open(taskfile, 'a').close()
            res = tfac._get_task_with_id('foo')
            self.assertEqual(res.get_taskdir(), submitdir)
            self.assertEqual(res.get_taskdict(), {})
            shutil.rmtree(submitdir)

            # try where we match in processing dir
            procdir = os.path.join(temp_dir, dao.PROCESSING_STATUS,
                                   '4.5.5.5',
                                   '02e487ef-79df-4d99-8f22-1ff1d6d52a2a')
            os.makedirs(procdir, mode=0o755)
            res = tfac._get_task_with_id('02e487ef-79df-4d99-8f22-'
                                         '1ff1d6d52a2a')
            self.assertEqual(res.get_taskdir(), procdir)
            shutil.rmtree(procdir)

            # try where we match in done dir
            donedir = os.path.join(temp_dir, dao.DONE_STATUS,
                                   '192.168.5.5',
                                   '02e487ef-79df-4d99-8f22-1ff1d6d52a2a')
            os.makedirs(donedir, mode=0o755)
            res = tfac._get_task_with_id('02e487ef-79df-4d99-8f22-'
                                         '1ff1d6d52a2a')
            self.assertEqual(res.get_taskdir(), donedir)

        finally:
            shutil.rmtree(temp_dir)

    def test_deletefilebasedtaskfactory_get_next_task(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # test where delete request dir is None
            tfac = DeletedFileBasedTaskFactory(None)
            res = tfac.get_next_task()
            self.assertEqual(res, None)

            # test where delete request dir is not a directory
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac.get_next_task()
            self.assertEqual(res, None)

            # no delete requests found
            del_req_dir = os.path.join(temp_dir, dao.DELETE_REQUESTS)
            os.makedirs(del_req_dir, mode=0o755)
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac.get_next_task()
            self.assertEqual(res, None)

            # directory in delete requests dir
            dir_in_del_dir = os.path.join(del_req_dir, 'uhohadir')
            os.makedirs(dir_in_del_dir, mode=0o755)
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac.get_next_task()
            self.assertEqual(res, None)

            # Found a delete request, but no task found in system
            a_request = os.path.join(del_req_dir,
                                     '02e487ef-79df-4d99-8f22-1ff1d6d52a2a')
            with open(a_request, 'w') as f:
                f.write('1.2.3.4')
                f.flush()
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac.get_next_task()
            self.assertEqual(res, None)
            self.assertTrue(not os.path.isfile(a_request))

            # Found a valid request in system
            a_request = os.path.join(del_req_dir,
                                     '02e487ef-79df-4d99-8f22-1ff1d6d52a2a')
            with open(a_request, 'w') as f:
                f.write('1.2.3.4')
                f.flush()
            done_dir = os.path.join(temp_dir, dao.DONE_STATUS,
                                    '1.2.3.4',
                                    '02e487ef-79df-4d99-8f22-1ff1d6d52a2a')
            os.makedirs(done_dir, mode=0o755)
            tfac = DeletedFileBasedTaskFactory(temp_dir)
            res = tfac.get_next_task()
            self.assertEqual(res.get_taskdir(), done_dir)
            self.assertEqual(res.get_taskdict(), {})
            self.assertTrue(not os.path.isfile(a_request))

        finally:
            shutil.rmtree(temp_dir)
