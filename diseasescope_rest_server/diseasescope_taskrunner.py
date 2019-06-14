#!/usr/bin/env python


import os
import sys
import argparse
import logging
import logging.config
import time
import shutil
import json
import glob
import subprocess
import daemon
import diseasescope_rest_server

logger = logging.getLogger('diseasescopetaskrunner')

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(relativeCreated)dms " \
             "%(filename)s::%(funcName)s():%(lineno)d %(message)s"


def _parse_arguments(desc, args):
    """Parses command line arguments"""
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)

    parser.add_argument('taskdir', help='Base directory where tasks'
                                        'are located')
    parser.add_argument('--wait_time', type=int, default=30,
                        help='Time in seconds to wait'
                             'before looking for new'
                             'tasks')
    parser.add_argument('--disabledelete', action='store_true',
                        help='If set, task runner will NOT monitor '
                             'delete requests')
    parser.add_argument('--nodaemon', default=False, action='store_true',
                        help='If set program will NOT run in daemon mode')
    parser.add_argument('--logconfig', help='Logging configuration file')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + diseasescope_rest_server.__version__))
    return parser.parse_args(args)


class FileBasedTask(object):
    """Represents a task
    """

    BASEDIR = 'basedir'
    STATE = 'state'
    IPADDR = 'ipaddr'
    UUID = 'uuid'
    TASK_FILES = [diseasescope_rest_server.RESULT,
                  diseasescope_rest_server.TASK_JSON]

    def __init__(self, taskdir, taskdict):
        self._taskdir = taskdir
        self._taskdict = taskdict
        self._resultdata = None

    def delete_task_files(self):
        """
        Deletes all files and directories pertaining to task
        on filesystem
        :return: None upon success or str with error message
        """
        if self._taskdir is None:
            return 'Task directory is None'

        if not os.path.isdir(self._taskdir):
            return ('Task directory ' + self._taskdir +
                    ' is not a directory')

        # this is a paranoid removal since we only are tossing
        # the directory in question and files listed in TASK_FILES
        try:
            for entry in os.listdir(self._taskdir):
                if entry not in FileBasedTask.TASK_FILES:
                    logger.error(entry + ' not in files created by task')
                    continue
                fp = os.path.join(self._taskdir, entry)
                if os.path.isfile(fp):
                    os.unlink(fp)
            os.rmdir(self._taskdir)
            return None
        except Exception as e:
            logger.exception('Caught exception removing ' + self._taskdir)
            return ('Caught exception ' + str(e) + 'trying to remove ' +
                    self._taskdir)

    def save_task(self):
        """
        Updates task in datastore. For filesystem based
        task this means rewriting the task.json file
        :return: None for success otherwise string containing error message
        """
        if self._taskdir is None:
            return 'Task dir is None'

        if self._taskdict is None:
            return 'Task dict is None'

        if not os.path.isdir(self._taskdir):
            return str(self._taskdir) + ' is not a directory'

        tjsonfile = os.path.join(self._taskdir, diseasescope_rest_server.TASK_JSON)
        logger.debug('Writing task data to: ' + tjsonfile)
        with open(tjsonfile, 'w') as f:
            json.dump(self._taskdict, f)

        if self._resultdata is not None:
            resultfile = os.path.join(self._taskdir, diseasescope_rest_server.RESULT)
            logger.debug('Writing result data to: ' + resultfile)
            with open(resultfile, 'w') as f:
                json.dump(self._resultdata, f)
                f.flush()
        return None

    def move_task(self, new_state,
                  error_message=None):
        """
        Changes state of task to new_state
        :param new_state: new state
        :return: None
        """
        taskattrib = self._get_uuid_ip_state_basedir_from_path()
        if taskattrib is None or taskattrib[FileBasedTask.BASEDIR] is None:
            return 'Unable to extract state basedir from task path'

        if taskattrib[FileBasedTask.STATE] == new_state:
            logger.debug('Attempt to move task to same state: ' +
                         self._taskdir)
            return None

        # if new state is error still put the task into
        # done directory, but update error message in
        # task json
        if new_state == diseasescope_rest_server.ERROR_STATUS:
            new_state = diseasescope_rest_server.DONE_STATUS

            if error_message is None:
                emsg = 'Unknown error'
            else:
                emsg = error_message
            logger.info('Task set to error state with message: ' +
                        emsg)
            self._taskdict['error'] = emsg
            self.save_task()
        logger.debug('Changing task: ' + str(taskattrib[FileBasedTask.UUID]) +
                     ' to state ' + new_state)
        ptaskdir = os.path.join(taskattrib[FileBasedTask.BASEDIR], new_state,
                                taskattrib[FileBasedTask.IPADDR],
                                taskattrib[FileBasedTask.UUID])
        shutil.move(self._taskdir, ptaskdir)
        self._taskdir = ptaskdir

        return None

    def _get_uuid_ip_state_basedir_from_path(self):
        """
        Parses taskdir path into main parts and returns
        result as dict
        :return: {'basedir': basedir,
                  'state': state
                  'ipaddr': ip address,
                  'uuid': task uuid}
        """
        if self._taskdir is None:
            logger.error('Task dir not set')
            return {FileBasedTask.BASEDIR: None,
                    FileBasedTask.STATE: None,
                    FileBasedTask.IPADDR: None,
                    FileBasedTask.UUID: None}
        taskuuid = os.path.basename(self._taskdir)
        ipdir = os.path.dirname(self._taskdir)
        ipaddr = os.path.basename(ipdir)
        if ipaddr == '':
            ipaddr = None
        statedir = os.path.dirname(ipdir)
        state = os.path.basename(statedir)
        if state == '':
            state = None
        basedir = os.path.dirname(statedir)
        return {FileBasedTask.BASEDIR: basedir,
                FileBasedTask.STATE: state,
                FileBasedTask.IPADDR: ipaddr,
                FileBasedTask.UUID: taskuuid}

    def get_ipaddress(self):
        """
        gets ip address
        :return:
        """
        res = self._get_uuid_ip_state_basedir_from_path()[FileBasedTask.IPADDR]
        return res

    def get_state(self):
        """
        Gets current state of task based on taskdir
        :return:
        """
        return self._get_uuid_ip_state_basedir_from_path()[FileBasedTask.STATE]

    def get_task_uuid(self):
        """
        Parses taskdir path to get uuid
        :return: string containing uuid or None if not found
        """
        return self._get_uuid_ip_state_basedir_from_path()[FileBasedTask.UUID]

    def get_task_summary_as_str(self):
        """
        Prints quick summary of task
        :return:
        """
        res = self._get_uuid_ip_state_basedir_from_path()
        return str(res)

    def set_result_data(self, result):
        """
        Sets result data object
        :param result:
        :return:
        """
        self._resultdata = result

    def set_taskdir(self, taskdir):
        """
        Sets task directory
        :param taskdir:
        :return:
        """
        self._taskdir = taskdir

    def get_taskdir(self):
        """
        Gets task directory
        :return:
        """
        return self._taskdir

    def set_taskdict(self, taskdict):
        """
        Sets task dictionary
        :param taskdict:
        :return:
        """
        self._taskdict = taskdict

    def get_taskdict(self):
        """
        Gets task dictionary
        :return:
        """
        return self._taskdict

    def get_diseaseid(self):
        """
        Gets alpha parameter
        :return: alpha parameter or None
        """
        if self._taskdict is None:
            return None
        if diseasescope_rest_server.DOID_PARAM not in self._taskdict:
            return None
        res = self._taskdict[diseasescope_rest_server.DOID_PARAM]
        return res

    def get_tmp_resultpath(self):
        """
        Gets tmp result path
        :return:
        """
        return os.path.join(self._taskdir,
                            diseasescope_rest_server.TMP_RESULT)

    def get_ndexname(self):
        """
        Gets ndex name parameter
        :return: ndex name parameter or None
        """
        if self._taskdict is None:
            return None
        if diseasescope_rest_server.NDEXNAME_PARAM not in self._taskdict:
            return None
        return self._taskdict[diseasescope_rest_server.NDEXNAME_PARAM]

    def get_ndexserver(self):
        """
        Gets ndex server parameter
        :return: ndex server or None
        """
        if self._taskdict is None:
            return None
        if diseasescope_rest_server.NDEXSERVER_PARAM not in self._taskdict:
            return None
        return self._taskdict[diseasescope_rest_server.NDEXSERVER_PARAM]

    def get_ndexuser(self):
        """
        Gets ndex user parameter
        :return: ndex user or None
        """
        if self._taskdict is None:
            return None
        if diseasescope_rest_server.NDEXUSER_PARAM not in self._taskdict:
            return None
        return self._taskdict[diseasescope_rest_server.NDEXUSER_PARAM]

    def get_ndexpass(self):
        """
        Gets ndex password
        :return: ndex password or None
        """
        if self._taskdict is None:
            return None
        if diseasescope_rest_server.NDEXPASS_PARAM not in self._taskdict:
            return None
        return self._taskdict[diseasescope_rest_server.NDEXPASS_PARAM]

    def get_hiviewurl(self):
        """
        Gets ndex password
        :return: ndex password or None
        """
        if self._taskdict is None:
            return None
        if diseasescope_rest_server.HIVIEWURL_PARAM not in self._taskdict:
            return None
        return self._taskdict[diseasescope_rest_server.HIVIEWURL_PARAM]


class FileBasedSubmittedTaskFactory(object):
    """
    Reads file system to get tasks
    """
    def __init__(self, taskdir):
        self._taskdir = taskdir
        self._submitdir = None
        if self._taskdir is not None:
            self._submitdir = os.path.join(self._taskdir,
                                           diseasescope_rest_server.SUBMITTED_STATUS)
        self._problemlist = []

    def get_next_task(self):
        """
        Looks for next task in task dir. currently finds the first
        :return:
        """
        if self._submitdir is None:
            logger.error('Submit directory is None')
            return None
        if not os.path.isdir(self._submitdir):
            logger.error(self._submitdir +
                         ' does not exist or is not a directory')
            return None
        logger.debug('Examining ' + self._submitdir + ' for new tasks')
        for entry in os.listdir(self._submitdir):
            fp = os.path.join(self._submitdir, entry)
            if not os.path.isdir(fp):
                continue
            for subentry in os.listdir(fp):
                subfp = os.path.join(fp, subentry)
                if os.path.isdir(subfp):
                    tjson = os.path.join(subfp, diseasescope_rest_server.TASK_JSON)
                    if os.path.isfile(tjson):
                        try:
                            with open(tjson, 'r') as f:
                                jsondata = json.load(f)
                            return FileBasedTask(subfp, jsondata)
                        except Exception as e:
                            if subfp not in self._problemlist:
                                logger.info('Skipping task: ' + subfp +
                                            ' due to error reading json' +
                                            ' file: ' + str(e))
                                self._problemlist.append(subfp)
        return None

    def get_size_of_problem_list(self):
        """
        Gets size of problem list
        :return:
        """
        return len(self._problemlist)

    def get_problem_list(self):
        """
        Gets problem list
        :return:
        """
        return self._problemlist


class DeletedFileBasedTaskFactory(object):
    """
    Reads filesystem for tasks that should be deleted
    """
    def __init__(self, taskdir):
        """
        Constructor
        :param taskdir:
        """
        self._taskdir = taskdir
        self._delete_req_dir = None
        self._searchdirs = []
        if self._taskdir is not None:
            self._delete_req_dir = os.path.join(self._taskdir,
                                                diseasescope_rest_server.DELETE_REQUESTS)
            self._searchdirs.append(os.path.join(self._taskdir,
                                                 diseasescope_rest_server.
                                                 PROCESSING_STATUS))
            self._searchdirs.append(os.path.join(self._taskdir,
                                                 diseasescope_rest_server.SUBMITTED_STATUS))
            self._searchdirs.append(os.path.join(self._taskdir,
                                                 diseasescope_rest_server.DONE_STATUS))
        else:
            logger.error('Taskdir is None')

    def get_next_task(self):
        """
        Gets next task that should be deleted
        :return:
        """
        if self._delete_req_dir is None:
            logger.error('Delete request dir is None')
            return None
        if not os.path.isdir(self._delete_req_dir):
            logger.error(self._delete_req_dir + ' is not a directory')
            return None
        logger.debug('Examining ' + self._delete_req_dir +
                     ' for delete task requests')
        for entry in os.listdir(self._delete_req_dir):
            fp = os.path.join(self._delete_req_dir, entry)
            if not os.path.isfile(fp):
                continue
            task = self._get_task_with_id(entry)

            logger.info('Removing delete request file: ' + fp)
            os.unlink(fp)
            if task is None:
                logger.info('Task ' + entry + ' not found')
                continue
            return task
        return None

    def _get_task_with_id(self, taskid):
        """
        Uses glob to look for task with id under taskdir
        :return: FileBasedTask object or None if not found
        """
        for search_dir in self._searchdirs:
            for entry in glob.glob(os.path.join(search_dir, '*', taskid)):
                if not os.path.isdir(entry):
                    logger.error('Found match (' + entry +
                                 '), but its not a directory')
                    continue
                tjson = os.path.join(entry, diseasescope_rest_server.TASK_JSON)
                if os.path.isfile(tjson):
                    try:
                        with open(tjson, 'r') as f:
                            jsondata = json.load(f)
                        return FileBasedTask(entry, jsondata)
                    except Exception as e:
                            logger.exception('Unable to parse json for task ' +
                                             entry + ' going to skip json: ' +
                                             str(e))
                            return FileBasedTask(entry, {})
                else:
                    logger.error('No json for task ' + entry +
                                 ' going to skip json')
                    return FileBasedTask(entry, {})
        return None


class Diseasescopetaskrunner(object):
    """
    Runs tasks created by DiseaseScope REST Server
    """
    def __init__(self, wait_time=30,
                 taskfactory=None,
                 deletetaskfactory=None):
        self._taskfactory = taskfactory
        self._wait_time = wait_time
        self._deletetaskfactory = deletetaskfactory

    def _process_task(self, task, delete_temp_files=True):
        """
        Processes a task
        :param taskdir:
        :return:
        """
        logger.info('Task dir: ' + task.get_taskdir())
        task.move_task(diseasescope_rest_server.PROCESSING_STATUS)

        # ADD PROCESSING LOGIC HERE
        result = ''
        emsg = None

        if emsg is not None:
            logger.error('Task had error: ' + emsg)
        else:
            logger.info('Task processing completed')

        task.set_result_data(result)
        task.save_task()
        if emsg is not None:
            status = diseasescope_rest_server.ERROR_STATUS
        else:
            status = diseasescope_rest_server.DONE_STATUS
        task.move_task(status,
                       error_message=emsg)
        return

    def run_tasks(self, keep_looping=lambda: True):
        """
        Main entry point, this function loops looking for
        tasks to run.
        :param keep_looping: Function that should return True to
                             denote this method should keep waiting
                             for new Tasks or False to exit
        :return:
        """
        while keep_looping():

            while self._remove_deleted_task() is True:
                pass

            task = self._taskfactory.get_next_task()
            if task is None:
                time.sleep(self._wait_time)
                continue

            logger.info('Found a task: ' + str(task.get_taskdir()))
            try:
                self._process_task(task)
            except Exception as e:
                emsg = ('Caught exception processing task: ' +
                        task.get_taskdir() + ' : ' + str(e))
                logger.exception('Skipping task cause - ' + emsg)
                task.move_task(diseasescope_rest_server.ERROR_STATUS,
                               error_message=emsg)

    def _remove_deleted_task(self):
        """
        Looks for delete task request and handles it
        :return: False if none found otherwise True
        """
        if self._deletetaskfactory is None:
            return False

        try:
            task = self._deletetaskfactory.get_next_task()
            if task is None:
                return False
            if task.get_taskdir() is not None:
                logger.info('Deleting task: ' + task.get_taskdir())
                res = task.delete_task_files()
                if res is not None:
                    logger.error('Error deleting task: ' + res)
                return True
            return True
        except Exception:
            logger.exception('Caught exception looking for delete task '
                             'requests')
            return False


def run(theargs, keep_looping=lambda: True):
    """

    :param parsed_args:
    :return:
    """
    try:
        logging.config.fileConfig(theargs.logconfig, disable_existing_loggers=False)
        logger.debug('Config file: ' + theargs.logconfig + ' loaded')
        ab_tdir = os.path.abspath(theargs.taskdir)
        logger.debug('Task directory set to: ' + ab_tdir)

        tfac = FileBasedSubmittedTaskFactory(ab_tdir)
        if theargs.disabledelete is True:
            logger.info('Deletion of tasks disabled')
            dfac = None
        else:
            dfac = DeletedFileBasedTaskFactory(ab_tdir)
        runner = Diseasescopetaskrunner(taskfactory=tfac,
                                wait_time=theargs.wait_time,
                                deletetaskfactory=dfac)

        runner.run_tasks(keep_looping=keep_looping)
    except Exception:
        logger.exception("Error caught exception")
        return 2
    finally:
        logging.shutdown()


def main(args, keep_looping=lambda: True):
    """Main entry point"""
    desc = """Runs tasks generated by DiseaseScope REST Server

    """
    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = diseasescope_rest_server.__version__

    if theargs.nodaemon is False:
        with daemon.DaemonContext():
            return run(theargs, keep_looping)
    else:
        return run(theargs, keep_looping)


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
