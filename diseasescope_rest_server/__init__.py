# -*- coding: utf-8 -*-

"""Top-level package for diseasescope_rest_server."""

__author__ = """coleslaw481"""
__email__ = 'churas.camera@gmail.com'
__version__ = '0.1.0'

from datetime import datetime
import os
import uuid
import json
import shutil
import time
import copy
import flask

from flask import Flask, jsonify, request
from flask_restplus import reqparse, Api, Resource, fields, marshal
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address


desc = """DiseaseScope REST Server

DiseaseScope: Automatic Construction and Interpretation of Hierarchical Disease Models

A service that automatically organizes high-throughput gene-gene interaction data into interactive hierarchical models. This method 
takes a disease id (DOID) and returns biological information at multiple scales including a core set of disease-associated genes, interactions 
that form disease-relevant pathways and the hierarchical organization of these pathways. Furthermore, to elucidate how each gene cluster is related 
to the disease, DiseaseScope includes two interpretation tools: HiView Lens to explore the underlying structure of individual modules by overlaying 
additional networks and NetAnt to determine what biomedical concepts connect the gene module to disease by proposing mechanistic pathways to pathogenesis. 
Although the pipeline is automatic, each module is a self-contained service that can be invoked independently, allowing users to form custom applications. 
Together, DiseaseScope aggregates across massive amounts of biological knowledge about diseases and organize the knowledge to guide discovery. 

 **NOTE:** This service is experimental. The interface is subject to change.


""" # noqa


DISEASESCOPE_REST_SETTINGS_ENV = 'DISEASESCOPE_REST_SETTINGS'
# global api object
app = Flask(__name__)

JOB_PATH_KEY = 'JOB_PATH'
WAIT_COUNT_KEY = 'WAIT_COUNT'
SLEEP_TIME_KEY = 'SLEEP_TIME'
DEFAULT_RATE_LIMIT_KEY = 'DEFAULT_RATE_LIMIT'

app.config[JOB_PATH_KEY] = '/tmp'
app.config[WAIT_COUNT_KEY] = 60
app.config[SLEEP_TIME_KEY] = 10
app.config[DEFAULT_RATE_LIMIT_KEY] = '360 per hour'

app.config.from_envvar(DISEASESCOPE_REST_SETTINGS_ENV, silent=True)
app.logger.info('Job Path dir: ' + app.config[JOB_PATH_KEY])
SERVICE_NS = 'diseasescope'

TASK_JSON = 'task.json'
LOCATION = 'Location'
TMP_RESULT = 'result.tmp'
RESULT = 'result.json'

ERROR_PARAM = 'error'
REMOTEIP_PARAM = 'remoteip'


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

api = Api(app, version=str(__version__),
          title='DiseaseScope REST Server',
          description=desc, example='put example here')

# enable rate limiting
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=[app.config[DEFAULT_RATE_LIMIT_KEY]],
    headers_enabled=True
)

# add rate limiting logger to the regular app logger
for handler in app.logger.handlers:
    limiter.logger.addHandler(handler)

# need to clear out the default namespace
api.namespaces.clear()

ns = api.namespace(SERVICE_NS,
                   description='DiseaseScope REST Server Service')

app.config.SWAGGER_UI_DOC_EXPANSION = 'list'


def get_uuid():
    """
    Generates UUID and returns as string. With one caveat,
    if app.config[USE_SEQUENTIAL_UUID] is set and True
    then uuid_counter is returned and incremented
    :return: uuid as string
    """
    return str(uuid.uuid4())


def get_submit_dir():
    """
    Gets base directory where submitted jobs will be placed
    :return:
    """
    return os.path.join(app.config[JOB_PATH_KEY], SUBMITTED_STATUS)


def get_processing_dir():
    """
        Gets base directory where processing jobs will be placed
    :return:
    """
    return os.path.join(app.config[JOB_PATH_KEY], PROCESSING_STATUS)


def get_done_dir():
    """
        Gets base directory where completed jobs will be placed

    :return:
    """
    return os.path.join(app.config[JOB_PATH_KEY], DONE_STATUS)


def get_delete_request_dir():
    """
    Gets base directory where delete request token files will be placed
    :return:
    """
    return os.path.join(app.config[JOB_PATH_KEY], DELETE_REQUESTS)


def create_task(params):
    """
    Creates a task by consuming data from request_obj passed in
    and persisting that information to the filesystem under
    JOB_PATH/SUBMIT_DIR/<IP ADDRESS>/UUID with various parameters
    stored in TASK_JSON file and if the 'network' file is set
    that data is dumped to NETWORK_DATA file within the directory
    :param request_obj:
    :return: string that is a uuid which denotes directory name
    """
    params['uuid'] = get_uuid()
    params['tasktype'] = 'diseasescope_ontology'
    taskpath = os.path.join(get_submit_dir(), str(params[REMOTEIP_PARAM]),
                            str(params['uuid']))
    try:
        original_umask = os.umask(0)
        os.makedirs(taskpath, mode=0o775)
    finally:
        os.umask(original_umask)

    tmp_task_json = TASK_JSON + '.tmp'
    taskfilename = os.path.join(taskpath, tmp_task_json)
    with open(taskfilename, 'w') as f:
        json.dump(params, f)
        f.flush()
    os.chmod(taskfilename, mode=0o775)
    shutil.move(taskfilename, os.path.join(taskpath, TASK_JSON))
    return params['uuid']


def log_task_json_file(taskpath):
    """
    Writes information about task to logger
    :param taskpath: path to task
    :return: None
    """
    if taskpath is None:
        return None

    tmp_task_json = TASK_JSON
    taskfilename = os.path.join(taskpath, tmp_task_json)

    if not os.path.isfile(taskfilename):
        return None

    with open(taskfilename, 'r') as f:
        data = json.load(f)
        app.logger.info('Json file of task: ' + str(data))


def get_task(uuidstr, iphintlist=None, basedir=None):
    """
    Gets task under under basedir.
    :param uuidstr: uuid string for task
    :param iphintlist: list of ip addresses as strings to speed up search.
                       if set then each
                       '/<basedir>//<iphintlist entry>/<uuidstr>'
                       is first checked and if the path is a directory
                       it is returned
    :param basedir:  base directory as string ie /foo
    :return: full path to task or None if not found
    """
    if uuidstr is None:
        app.logger.warning('Path passed in is None')
        return None

    if basedir is None:
        app.logger.error('basedir is None')
        return None

    if not os.path.isdir(basedir):
        app.logger.error(basedir + ' is not a directory')
        return None

    # Todo: Add logic to leverage iphintlist
    # Todo: Add a retry if not found with small delay in case of dir is moving
    for entry in os.listdir(basedir):
        ip_path = os.path.join(basedir, entry)
        if not os.path.isdir(ip_path):
            continue
        for subentry in os.listdir(ip_path):
            if uuidstr != subentry:
                continue
            taskpath = os.path.join(ip_path, subentry)

            if os.path.isdir(taskpath):
                return taskpath
    return None


def wait_for_task(uuidstr, hintlist=None):
    """
    Waits for task to appear in done directory
    :param uuidstr: uuid of task
    :param hintlist: list of ip addresses to search under
    :return: string containing full path to task or None if not found
    """
    if uuidstr is None:
        app.logger.error('uuid is None')
        return None

    counter = 0
    taskpath = None
    done_dir = get_done_dir()
    while counter < app.config[WAIT_COUNT_KEY]:
        taskpath = get_task(uuidstr, iphintlist=hintlist,
                            basedir=done_dir)
        if taskpath is not None:
            break
        app.logger.debug('Sleeping while waiting for ' + uuidstr)
        time.sleep(app.config[SLEEP_TIME_KEY])
        counter = counter + 1

    if taskpath is None:
        app.logger.info('Wait time exceeded while looking for: ' + uuidstr)

    return taskpath


ERROR_RESP = api.model('ErrorResponseSchema', {
    'errorCode': fields.String(description='Error code to help identify issue'),
    'message': fields.String(description='Human readable description of error'),
    'description': fields.String(description='More detailed description of error'),
    'stackTrace': fields.String(description='stack trace of error'),
    'threadId': fields.String(description='Id of thread running process'),
    'timeStamp': fields.String(description='UTC Time stamp in YYYY-MM-DDTHH:MM.S')
})

TOO_MANY_REQUESTS = api.model('TooManyRequestsSchema', {
    'message': fields.String(description='Contains detailed message about exceeding request limits')
})

RATE_LIMIT_HEADERS = {
 'x-ratelimit-limit': 'Request rate limit',
 'x-ratelimit-remaining': 'Number of requests remaining',
 'x-ratelimit-reset': 'Request rate limit reset time'
}


class ErrorResponse(object):
    """Error response
    """
    def __init__(self):
        """
        Constructor
        """
        self.errorCode = ''
        self.message = ''
        self.description = ''
        self.stackTrace = ''
        self.threadId = ''
        self.timeStamp = ''

        dt = datetime.utcnow()
        self.timeStamp = dt.strftime('%Y-%m-%dT%H:%M.%s')


class TaskResponse(object):
    """
    Task id object
    """
    def __init__(self):
        """
        Constructor
        """
        self.id = ''


@api.doc('Runs query')
@ns.route('/', strict_slashes=False)
class RunDiseaseScope(Resource):
    """
    Runs DiseaseScope
    """
    POST_HEADERS = copy.deepcopy(RATE_LIMIT_HEADERS)
    POST_HEADERS['Location'] = 'URL containing resource/result generated by this request'

    taskres_obj = api.model('Task', {
        'id': fields.String(description='id of task',
                            example='9350e222-c5e7-42a8-ac70-69044ebcba80')
    })

    resource_fields = api.model('Query', {
        DOID_PARAM: fields.Integer(description='Disease ID as a number',
                                   example=1816, required=True),
        TISSUE_PARAM: fields.List(fields.String(description='Tissue')),
        NDEXNAME_PARAM: fields.String('DiseaseScopeOntology',
                                      description='Name to use for network '
                                                  'stored in NDEx'),
        NDEXSERVER_PARAM: fields.String('test.ndexbio.org',
                                        description='NDEx server to use',
                                        example='test.ndexbio.org'),
        NDEXUSER_PARAM: fields.String('diseasescope_anon',
                                      description='NDEx username',
                                      example='diseasescop_anon'),
        NDEXPASS_PARAM: fields.String('diseasescope_anon',
                                      description='NDEx password',
                                      example='diseasescope_anon'),
        HIVIEWURL_PARAM: fields.String('http://hiview-test.ucsd.edu',
                                       description='HiView server to use',
                                       example='http://hiview-test.ucsd.edu'),
    })

    @api.doc('Runs DiseaseScope')
    @api.response(202, 'The task was successfully submitted to the service. '
                       'Visit the URL'
                       ' specified in **Location** field in HEADERS to '
                       'status and results', taskres_obj, headers=POST_HEADERS)
    @api.response(400, 'Bad request, an invalid input was passed in')
    @api.response(429, 'Too many requests', TOO_MANY_REQUESTS,
                  headers=RATE_LIMIT_HEADERS)
    @api.response(500, 'Internal server error', ERROR_RESP,
                  headers=RATE_LIMIT_HEADERS)
    @api.expect(resource_fields)
    def post(self):
        """
        Runs DiseaseScope

        This call submits a job to run DiseaseScope and returns a status code of 202 and sets the header parameter
        Location to the URL that can be polled to get status and final result.
        """
        app.logger.debug("Post received")

        try:
            thereq = request.json
            thereq['remoteip'] = request.remote_addr
            res = create_task(thereq)

            resp = flask.make_response()
            resp.headers[LOCATION] = res
            resp.status_code = 202
            task = TaskResponse()
            task.id = res
            return marshal(task, RunDiseaseScope.taskres_obj), 202,\
                   {LOCATION: res}
        except OSError as ea:
            app.logger.exception('Error creating task due to Exception ' +
                                 str(ea))
            er = ErrorResponse()
            er.message = 'Error creating task due to Exception'
            er.description = str(ea)
            return marshal(er, ERROR_RESP), 500


@ns.route('/<string:id>', strict_slashes=False)
class GetQueryResult(Resource):
    """More class doc here"""

    param_respobj = api.model('ParametersSchema', {
        DOID_PARAM: fields.Integer(description='Disease ID (DOID) http://www.obofoundry.org/ontology/doid.html',
                                   example=1816),
        NDEXUSER_PARAM: fields.String('NDEx username for storing ontology network', example='diseasescope_anon'),
        NDEXPASS_PARAM: fields.String('NDEx password for storing ontology network', example='diseasescope_anonpass'),
        NDEXSERVER_PARAM: fields.String('NDEx server for storing ontology network', example='public.ndexbio.org'),
        NDEXNAME_PARAM: fields.String('Name to use for network stored in NDEx', example='DiseaseScope Ontology')
    })

    resultobj = api.model('ResultSchema', {
        'hiviewurl': fields.String(description='HiView URL link',
                                   example='http://hiview-test.ucsd.edu/2ee22eb8-8ec4-11e9-9bb5-0660b7976219?type=test&server=http://dev2.ndexbio.org'),
        'ndexurl': fields.String(description='URL of network used by HiView in NDEx',
                                 example='http://dev2.ndexbio.org/#/network/2ee22eb8-8ec4-11e9-9bb5-0660b7976219'),
    })
    completeresultobj = api.model('CompleteResultSchema', {
        'parameters': fields.Nested(param_respobj),
        'result': fields.Nested(resultobj),
        'message': fields.String(description='Any message about query, such as an error message',
                                 example='Message about processing'),
        'progress': fields.Integer(description='% completion, will be a value in range of 0-100',
                                   example=100),
        'wallTime': fields.Integer(description='Time in milliseconds query took to run',
                                   example=341),
        'status': fields.String(description='One of the following <' +
                                            ' | '.join(STATUS_LIST) + '>',
                                example=DONE_STATUS)
    })

    @api.response(200, 'Successful response from server', completeresultobj)
    @api.response(410, 'Task not found')
    @api.response(429, 'Too many requests', TOO_MANY_REQUESTS)
    @api.response(500, 'Internal server error', ERROR_RESP)
    def get(self, id):
        """
        Gets the status and results of a DiseaseScope task
        """
        cleanid = id.strip()

        taskpath = get_task(cleanid, basedir=get_submit_dir())

        if taskpath is not None:
            resp = jsonify({STATUS_RESULT_KEY: SUBMITTED_STATUS,
                            PARAMETERS_KEY: self._get_task_parameters(taskpath)})
            resp.status_code = 200
            return resp

        taskpath = get_task(cleanid, basedir=get_processing_dir())

        if taskpath is not None:
            resp = jsonify({STATUS_RESULT_KEY: PROCESSING_STATUS,
                            PARAMETERS_KEY: self._get_task_parameters(taskpath)})
            resp.status_code = 200
            return resp

        taskpath = get_task(cleanid, basedir=get_done_dir())

        if taskpath is None:
            resp = jsonify({STATUS_RESULT_KEY: NOTFOUND_STATUS,
                            PARAMETERS_KEY: None})
            resp.status_code = 410
            return resp

        result = os.path.join(taskpath, RESULT)
        if not os.path.isfile(result):
            er = ErrorResponse()
            er.message = 'No result found'
            er.description = self._get_task_parameters(taskpath)
            return marshal(er, ERROR_RESP), 500

        log_task_json_file(taskpath)
        app.logger.info('Result file is ' + str(os.path.getsize(result)) +
                        ' bytes')

        with open(result, 'r') as f:
            data = json.load(f)

        return jsonify({STATUS_RESULT_KEY: DONE_STATUS,
                        RESULT_KEY: data,
                        PARAMETERS_KEY: self._get_task_parameters(taskpath)})

    def _get_task_parameters(self, taskpath):
        """
        Gets task parameters from TASK_JSON file as
        a dictionary
        :param taskpath:
        :return: task parameters
        :rtype dict:
        """
        taskparams = None
        try:
            taskjsonfile = os.path.join(taskpath, TASK_JSON)

            if os.path.isfile(taskjsonfile):
                with open(taskjsonfile, 'r') as f:
                    taskparams = json.load(f)
                if 'remoteip' in taskparams:
                    # delete the remote ip
                    del taskparams['remoteip']
        except Exception:
            app.logger.exception('Caught exception getting parameters')
        return taskparams

    @api.doc('Creates request to delete query')
    @api.response(200, 'Delete request successfully received')
    @api.response(400, 'Invalid delete request', ERROR_RESP)
    @api.response(429, 'Too many requests', TOO_MANY_REQUESTS)
    @api.response(500, 'Internal server error', ERROR_RESP)
    def delete(self, id):
        """
        Deletes task associated with {id} passed in
        """
        resp = flask.make_response()
        try:
            req_dir = get_delete_request_dir()
            if not os.path.isdir(req_dir):
                app.logger.debug('Creating directory: ' + req_dir)
                try:
                    original_umask = os.umask(0)
                    os.makedirs(req_dir, mode=0o775)
                finally:
                    os.umask(original_umask)
            cleanid = id.strip()
            if len(cleanid) > 40 or len(cleanid) == 0:
                er = ErrorResponse()
                er.message = 'Invalid id'
                er.description = 'id is empty or greater then 40 chars'
                return marshal(er, ERROR_RESP), 400

            with open(os.path.join(req_dir, cleanid), 'w') as f:
                f.write(request.remote_addr)
                f.flush()
            resp.status_code = 200
            return resp
        except Exception as e:
            er = ErrorResponse()
            er.message = 'Caught exception'
            er.description = str(e)
            return marshal(er, ERROR_RESP), 500


class ServerStatus(object):
    """Represents status of server
    """
    def __init__(self):
        """Constructor
        """

        self.status = 'ok'
        self.message = ''
        self.pcDiskFull = 0
        self.load = [0, 0, 0]
        self.restVersion = __version__

        self.pcDiskFull = -1
        try:
            s = os.statvfs(get_submit_dir())
            self.pcDiskFull = int(float(s.f_blocks - s.f_bavail) /
                                  float(s.f_blocks) * 100)
        except Exception:
            app.logger.exception('Caught exception checking disk space')
            self.pcDiskFull = -1

        if self.pcDiskFull >= 90:
            self.status = 'error'
            self.message = 'Disk is full'
        else:
            self.status = 'ok'
        loadavg = os.getloadavg()

        self.load[0] = loadavg[0]
        self.load[1] = loadavg[1]
        self.load[2] = loadavg[2]


@ns.route('/status', strict_slashes=False, doc=False)
class SystemStatus(Resource):
    """
    System status
    """
    statusobj = api.model('StatusSchema', {
        'status': fields.String(description='ok|error'),
        'pcDiskFull': fields.Integer(description='How full disk is in %'),
        'load': fields.List(fields.Float(description='server load'),
                            description='List of 3 floats containing 1 minute,'
                                        ' 5 minute, 15minute load'),
        'restVersion': fields.String(description='Version of REST service')
    })
    @api.doc('Gets status')
    @api.response(200, 'Success', statusobj)
    @api.response(429, 'Too many requests', TOO_MANY_REQUESTS)
    @api.response(500, 'Internal server error', ERROR_RESP)
    def get(self):
        """
        Gets status of service

        """
        ss = ServerStatus()
        return marshal(ss, SystemStatus.statusobj), 200
