# -*- coding: utf-8 -*-

"""
file: task_python_type.py

Task for running a Python function in threaded or blocking mode
"""
import sys
from importlib import import_module
from twisted.internet import reactor, threads

from graphit.graph_mixin import NodeTools
from graphit.graph_combinatorial.graph_split_join_operations import graph_join

from mdstudio_workflow import __twisted_logger__
from mdstudio_workflow.workflow_task_types.task_base_type import TaskBase, load_task_schema

if __twisted_logger__:
    from twisted.logger import Logger
    logging = Logger()
else:
    import logging

# Preload Task definitions from JSON schema in the package schema/endpoints/
TASK_SCHEMA = 'workflow_python_task.v1.json'
TASK = load_task_schema(TASK_SCHEMA)


class LoadCustomFunc(NodeTools):

    def load(self):
        """
        Python function or class loader

        Custom Python function can be run on the local machine using a blocking
        or non-blocking task runner.
        These functions are loaded dynamically ar runtime using the Python URI
        of the function as stored in the task 'custom_func' attribute.
        A function URI is defined as a dot-separated string in which the last
        name defines the function name and all names in front the absolute or
        relative path to the module containing the function. The module needs
        to be in the python path.

        Example: 'path.to.module.function'

        :param python_uri: Python absolute or relative function URI
        :type python_uri:  :py:str

        :return:           function object
        """

        func = None
        python_uri = self.get()
        print(sys.path)
        if python_uri:
            module_name = '.'.join(python_uri.split('.')[:-1])
            function_name = python_uri.split('.')[-1]

            try:
                imodule = import_module(module_name)
                func = getattr(imodule, function_name)
                logging.debug(
                    'Load task runner function: {0} from module: {1}'.format(function_name, module_name))
            except (ValueError, ImportError):
                msg = 'Unable to load task runner function: "{0}" from module: "{1}"'
                logging.error(msg.format(function_name, module_name))
        else:
            logging.error('No Python path to function or class defined')

        return func


class PythonTaskBase(TaskBase):

    def new(self, **kwargs):
        """
        Implements 'new' abstract base class method to create new
        task node tree.

        Load task from JSON Schema workflow_python_task.v1.json in package
        /schemas/endpoints folder.
        """

        # Do not initiate twice in case method gets called more then once.
        if not len(self.children()):

            logging.info('Init task {0} ({1}) from schema: {2}'.format(self.nid, self.key, TASK_SCHEMA))

            graph_join(self.origin, TASK.descendants(),
                       links=[(self.nid, i) for i in TASK.children(return_nids=True)])

            # Set unique task uuid
            self.task_metadata.task_id.set(self.data.value_tag, self.task_metadata.task_id.create())

    def validate(self, key=None):
        """
        Python task specific validation.

        Check if custom function can be loaded
        """

        if self.custom_func() and not self.custom_func.load():
            logging.error('Task {0} ({1}), ValidationError: unable to load python function'.format(self.nid, self.key))
            return False

        return super(PythonTaskBase, self).validate()


class PythonTask(PythonTaskBase):
    """
    Task class for running Python functions or classes in threaded
    mode using Twisted `deferToThread`.
    """

    def run_task(self, callback, **kwargs):
        """
        Implements run_task method

        Runs Python function or class in threaded mode using Twisted
        'deferToThread' method.

        :param callback:    WorkflowRunner callback method called from Twisted
                            deferToThread when task is done.
        """

        # Empty task if no custom_func defined, output == input
        if self.custom_func() is None:
            logging.info('No python function or class defined. Empty task returns input as output')
            callback(self.get_input(), self.nid)

        else:
            deferred = threads.deferToThread(self.custom_func.load(), **self.get_input())
            deferred.addCallback(callback, self.nid)
            deferred.addErrback(callback, self.nid)

            if not reactor.running:
                reactor.run(installSignalHandlers=0)

    def check_task(self, callback, **kwargs):
        """
        Implement check_task method for asynchronous tasks

        :param callback:    WorkflowRunner callback method called from Twisted
                            deferred when task is done.
        :param kwargs:      Additional keyword arguments should contain the main
                            mdstudio.component.session.ComponentSession instance as
                            'task_runner'
        """

        # Empty task if no custom_func defined, output == input
        if self.custom_func() is None:
            logging.info('No python function or class defined. Empty task returns input as output')
            callback(self.get_input(), self.nid)

        else:
            deferred = threads.deferToThread(self.custom_func.load(),
                                             task_id=self.task_metadata.external_task_id(),
                                             status=self.status,
                                             query_url=self.custom_func(),
                                             checks=self.task_metadata.checks())
            deferred.addCallback(callback, self.nid)
            deferred.addErrback(callback, self.nid)

            if not reactor.running:
                reactor.run(installSignalHandlers=0)


class BlockingPythonTask(PythonTaskBase):
    """
    Task class for running Python functions or classes in blocking
    mode resulting in the main workflow thread to be blocked until
    a result is returned or an exception is raised.
    """

    def run_task(self, callback, **kwargs):
        """
        Implements run_task method

        Runs Python function or class in blocking mode

        :param callback:    WorkflowRunner callback method called when task
                            is done.
        """

        # Empty task if no custom_func defined, output == input
        if self.custom_func() is None:
            logging.info('No python function or class defined. Empty task returns input as output')
            callback(self.get_input(), self.nid)

        else:
            python_func = self.custom_func.load()
            output = None
            try:
                output = python_func(**self.get_input())
            except Exception as e:
                logging.error(e)

            callback(output, self.nid)
