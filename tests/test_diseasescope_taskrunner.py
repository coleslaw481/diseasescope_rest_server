#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `diseasescope_taskrunner` script."""

import os
import json
import unittest
import shutil
import tempfile
from unittest.mock import MagicMock

import diseasescope_rest_server
from diseasescope_rest_server import diseasescope_taskrunner as dt
from diseasescope_rest_server.dao import FileBasedTask
from diseasescope_rest_server.dao import FileBasedSubmittedTaskFactory
from diseasescope_rest_server.dao import DeletedFileBasedTaskFactory
from diseasescope_rest_server.diseasescope_taskrunner import Diseasescopetaskrunner
from diseasescope_rest_server import dao

class TestDiseasescopetaskrunner(unittest.TestCase):
    """Tests for `diseasescope_taskrunner` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        pass

    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def test_parse_arguments(self):
        """Test something."""
        res = dt._parse_arguments('hi', ['foo'])
        self.assertEqual(res.taskdir, 'foo')

        self.assertEqual(res.wait_time, 30)
        self.assertEqual(res.disabledelete, False)

    def test_nbgwastaskrunner_run_tasks_no_work(self):
        mocktaskfac = MagicMock()
        mocktaskfac.get_next_task = MagicMock(side_effect=[None, None])
        runner = Diseasescopetaskrunner(wait_time=0, taskfactory=mocktaskfac)
        loop = MagicMock()
        loop.side_effect = [True, True, False]
        runner.run_tasks(keep_looping=loop)
        self.assertEqual(loop.call_count, 3)
        self.assertEqual(mocktaskfac.get_next_task.call_count, 2)

    def test_nbgwastaskrunner_run_tasks_task_raises_exception(self):
        temp_dir = tempfile.mkdtemp()
        try:
            mocktaskfac = MagicMock()
            mocktask = MagicMock()
            mocktask.get_taskdir = MagicMock(return_value=temp_dir)
            mocktask.move_task = MagicMock()
            mocktaskfac.get_next_task.side_effect = [None, mocktask]

            mock_net_fac = MagicMock()
            mock_net_fac. \
                get_networkx_object = MagicMock(side_effect=Exception('foo'))

            runner = Diseasescopetaskrunner(wait_time=0,
                                      taskfactory=mocktaskfac)
            loop = MagicMock()
            loop.side_effect = [True, True, False]
            runner.run_tasks(keep_looping=loop)
            self.assertEqual(loop.call_count, 3)
            self.assertEqual(mocktaskfac.get_next_task.call_count, 2)
        finally:
            shutil.rmtree(temp_dir)

    def test_nbgwastaskrunner_remove_deleted_task(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # try where delete task factory is none
            runner = Diseasescopetaskrunner(wait_time=0)
            self.assertEqual(runner._remove_deleted_task(), False)

            # try where no task is returned
            mockfac = MagicMock()
            mockfac.get_next_task = MagicMock(return_value=None)
            runner = Diseasescopetaskrunner(wait_time=0,
                                      deletetaskfactory=mockfac)
            res = runner._remove_deleted_task()
            self.assertEqual(res, False)
            mockfac.get_next_task.assert_called()

            # try where task.get_taskdir() is None
            task = MagicMock()
            task.get_taskdir = MagicMock(return_value=None)
            mockfac.get_next_task = MagicMock(return_value=task)
            runner = Diseasescopetaskrunner(wait_time=0,
                                      deletetaskfactory=mockfac)
            res = runner._remove_deleted_task()
            self.assertEqual(res, True)
            mockfac.get_next_task.assert_called()
            task.get_taskdir.assert_called()

            # try where task.delete_task_files() raises Exception
            task = MagicMock()
            task.get_taskdir = MagicMock(return_value='/foo')
            task.delete_task_files = MagicMock(side_effect=Exception('some '
                                                                     'error'))
            mockfac.get_next_task = MagicMock(return_value=task)
            runner = Diseasescopetaskrunner(wait_time=0,
                                      deletetaskfactory=mockfac)
            res = runner._remove_deleted_task()
            self.assertEqual(res, False)
            mockfac.get_next_task.assert_called()
            task.get_taskdir.assert_called()
            task.delete_task_files.assert_called()

            # try with valid task to delete, but delete returns message
            task = MagicMock()
            task.get_taskdir = MagicMock(return_value='/foo')
            task.delete_task_files = MagicMock(return_value='a error')
            mockfac.get_next_task = MagicMock(return_value=task)
            runner = Diseasescopetaskrunner(wait_time=0,
                                      deletetaskfactory=mockfac)
            res = runner._remove_deleted_task()
            self.assertEqual(res, True)
            mockfac.get_next_task.assert_called()
            task.get_taskdir.assert_called()
            task.delete_task_files.assert_called()

            # try with valid task to delete
            task = MagicMock()
            task.get_taskdir = MagicMock(return_value='/foo')
            task.delete_task_files = MagicMock(return_value=None)
            mockfac.get_next_task = MagicMock(return_value=task)
            runner = Diseasescopetaskrunner(wait_time=0,
                                      deletetaskfactory=mockfac)
            res = runner._remove_deleted_task()
            self.assertEqual(res, True)
            mockfac.get_next_task.assert_called()
            task.get_taskdir.assert_called()
            task.delete_task_files.assert_called()
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

    def test_main(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # test no work and disable delete true
            loop = MagicMock()
            loop.side_effect = [True, True, False]
            dt.main(['foo.py', '--wait_time', '0',
                     '--nodaemon',
                     temp_dir],
                    keep_looping=loop)

            # test no work and disable delete false
            loop = MagicMock()
            loop.side_effect = [True, True, False]
            dt.main(['foo.py', '--wait_time', '0',
                     '--nodaemon',
                     '--disabledelete',
                     temp_dir],
                    keep_looping=loop)

            # test exception catch works
            loop = MagicMock()
            loop.side_effect = Exception('some error')
            dt.main(['foo.py', '--wait_time', '0',
                     '--nodaemon',
                     temp_dir],
                    keep_looping=loop)
        finally:
            shutil.rmtree(temp_dir)
