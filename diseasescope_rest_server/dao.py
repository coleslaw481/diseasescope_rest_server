# -*- coding: utf-8 -*-

"""Data Access Objects for diseasescope REST server"""
import os
import logging
import shutil
import json
import glob

logger = logging.getLogger(__name__)


TASK_JSON = 'task.json'
TMP_RESULT = 'result.tmp'
RESULT = 'result.json'

STATUS_RESULT_KEY = 'status'
NOTFOUND_STATUS = 'notfound'
UNKNOWN_STATUS = 'unknown'
SUBMITTED_STATUS = 'submitted'
PROCESSING_STATUS = 'processing'
DONE_STATUS = 'done'
ERROR_STATUS = 'error'

STATUS_LIST = [UNKNOWN_STATUS, SUBMITTED_STATUS,
               PROCESSING_STATUS, ERROR_STATUS,
               DONE_STATUS]

# directory where token files named after tasks to delete
# are stored
DELETE_REQUESTS = 'delete_requests'

# key in result dictionary denoting the
# result data
RESULT_KEY = 'result'
NDEXURL_KEY = 'ndexurl'
HIVIEWURL_KEY = 'hiviewurl'

# key in result dictionary denoting input parameters
PARAMETERS_KEY = 'parameters'
NDEXSERVER_PARAM = 'ndexserver'
NDEXUSER_PARAM = 'ndexuser'
NDEXPASS_PARAM = 'ndexpass'
NDEXNAME_PARAM = 'ndexname'
HIVIEWURL_PARAM = 'hiviewurl'

DOID_PARAM = 'doid'
TISSUE_PARAM = 'tissue'


class FileBasedTask(object):
    """Represents a task
    """

    BASEDIR = 'basedir'
    STATE = 'state'
    IPADDR = 'ipaddr'
    UUID = 'uuid'
    TASK_FILES = [TASK_JSON]

    def __init__(self, taskdir, taskdict):
        self._taskdir = taskdir
        self._taskdict = taskdict

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

        tjsonfile = os.path.join(self._taskdir, TASK_JSON)
        logger.debug('Writing task data to: ' + tjsonfile)
        with open(tjsonfile, 'w') as f:
            json.dump(self._taskdict, f)

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
        if new_state == ERROR_STATUS:
            new_state = DONE_STATUS

            if error_message is None:
                emsg = 'Unknown error'
            else:
                emsg = error_message
            logger.info('Task set to error state with message: ' +
                        emsg)
            self._taskdict['message'] = emsg
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
        if DOID_PARAM not in self._taskdict:
            return None
        res = self._taskdict[DOID_PARAM]
        return res

    def get_ndexname(self):
        """
        Gets ndex name parameter
        :return: ndex name parameter or None
        """
        if self._taskdict is None:
            return None
        if NDEXNAME_PARAM not in self._taskdict:
            return None
        return self._taskdict[NDEXNAME_PARAM]

    def get_ndexserver(self):
        """
        Gets ndex server parameter
        :return: ndex server or None
        """
        if self._taskdict is None:
            return None
        if NDEXSERVER_PARAM not in self._taskdict:
            return None
        return self._taskdict[NDEXSERVER_PARAM]

    def get_ndexuser(self):
        """
        Gets ndex user parameter
        :return: ndex user or None
        """
        if self._taskdict is None:
            return None
        if NDEXUSER_PARAM not in self._taskdict:
            return None
        return self._taskdict[NDEXUSER_PARAM]

    def get_ndexpass(self):
        """
        Gets ndex password
        :return: ndex password or None
        """
        if self._taskdict is None:
            return None
        if NDEXPASS_PARAM not in self._taskdict:
            return None
        return self._taskdict[NDEXPASS_PARAM]

    def get_hiviewurl(self):
        """
        Gets ndex password
        :return: ndex password or None
        """
        if self._taskdict is None:
            return None
        if HIVIEWURL_PARAM not in self._taskdict:
            return None
        return self._taskdict[HIVIEWURL_PARAM]


class FileBasedSubmittedTaskFactory(object):
    """
    Reads file system to get tasks
    """
    def __init__(self, taskdir):
        self._taskdir = taskdir
        self._submitdir = None
        if self._taskdir is not None:
            self._submitdir = os.path.join(self._taskdir,
                                           SUBMITTED_STATUS)
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
                    tjson = os.path.join(subfp, TASK_JSON)
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
                                                DELETE_REQUESTS)
            self._searchdirs.append(os.path.join(self._taskdir,
                                                 PROCESSING_STATUS))
            self._searchdirs.append(os.path.join(self._taskdir,
                                                 SUBMITTED_STATUS))
            self._searchdirs.append(os.path.join(self._taskdir,
                                                 DONE_STATUS))
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
                tjson = os.path.join(entry, TASK_JSON)
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
