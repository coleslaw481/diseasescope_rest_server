#!/usr/bin/env python


import os
import sys
import argparse
import logging
import logging.config
import time
from datetime import datetime
import daemon
import diseasescope_rest_server
from diseasescope_rest_server import dao
from diseasescope_rest_server.dao import FileBasedSubmittedTaskFactory
from diseasescope_rest_server.dao import DeletedFileBasedTaskFactory
from diseasescope.diseasescope import DiseaseScope



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
    parser.add_argument('--doidmappingfile', required=True,
                        help='DOID mapping file')
    parser.add_argument('--genesetfile', required=True,
                        help='Gene set file')
    parser.add_argument('--logconfig', help='Logging configuration file')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + diseasescope_rest_server.__version__))
    return parser.parse_args(args)


class Diseasescopetaskrunner(object):
    """
    Runs tasks created by DiseaseScope REST Server
    """
    def __init__(self, wait_time=30,
                 taskfactory=None,
                 deletetaskfactory=None,
                 doidfile=None,
                 genesetfile=None):
        self._taskfactory = taskfactory
        self._wait_time = wait_time
        self._deletetaskfactory = deletetaskfactory
        self._doidfile = doidfile
        self._geneset_file = genesetfile

    def _process_task(self, task, delete_temp_files=True):
        """
        Processes a task
        :param taskdir:
        :return:
        """
        logger.info('Task dir: ' + task.get_taskdir())
        task.move_task(dao.PROCESSING_STATUS)
        taskdict = task.get_taskdict()
        scope = (
            DiseaseScope(taskdict['doid'], convert_doid=True,
                         doid_mapping_file=self._doidfile,
                         geneset_file=self._geneset_file)
                .get_disease_genes(method="biothings")
                .get_disease_tissues(n=10)
                .expand_gene_set(method='biggim')
                .get_network(method="biggim")
                .convert_edge_table_names(
                ["Gene1", "Gene2"],
                'entrezgene',
                "symbol",
                keep=False
            )
                .infer_hierarchical_model(
                edge_attr="mean",
                method='clixo-api',
                temp_path=task.get_taskdir(),
                method_kwargs={
                    'alpha': 0.01,
                    'beta': 0.5,
                }
            )
        )
        logger.info('Task finished')
        # ADD PROCESSING LOGIC HERE
        emsg = None
        taskdict = task.get_taskdict()
        taskdict['progress'] = 100
        taskdict['result'] = {
            "hiviewurl": scope.hiview_url,
            "ndexurl": ""}

        if emsg is not None:
            logger.error('Task had error: ' + emsg)
        else:
            logger.info('Task processing completed')

        curtime = diseasescope_rest_server.milliseconds_since_epoch(datetime.utcnow())
        taskdict['wallTime'] = curtime - taskdict['submitTime']
        task.save_task()
        if emsg is not None:
            status = dao.ERROR_STATUS
        else:
            status = dao.DONE_STATUS
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
                task.move_task(dao.ERROR_STATUS,
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
                                deletetaskfactory=dfac,
                                doidfile=theargs.doidmappingfile,
                                genesetfile=theargs.genesetfile)

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
