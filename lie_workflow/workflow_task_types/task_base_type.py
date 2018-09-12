# -*- coding: utf-8 -*-

"""
file: task_base_type.py

Abstract base class defining the Task interface including methods that every
task type should implement
"""

import abc
import pkg_resources
import logging
import json
import os

from lie_graph import GraphAxis
from lie_graph.graph_mixin import NodeTools
from lie_graph.graph_py2to3 import prepaire_data_dict, PY_STRING
from lie_graph.graph_io.io_jsonschema_format import read_json_schema

from lie_workflow.workflow_common import WorkflowError, collect_data


def load_task_schema(schema_name):
    """
    Load task template graphs from JSON Schema template files

    Template files are located in the package /schemas/endpoints directory

    :param schema_name: task JSON Schema file name to load
    :type schema_name:  :py:str

    :return:            Directed GraphAxis task template graph
    :rtype:             :lie_graph:GraphAxis
    """

    # Set 'is_directed' to True to import JSON schema as directed graph
    template_graph = GraphAxis()
    template_graph.is_directed = True

    task_schema = pkg_resources.resource_filename('lie_workflow', '/schemas/endpoints/{0}'.format(schema_name))
    task_template = read_json_schema(task_schema, graph=template_graph,
                                     exclude_args=['title', 'description', 'schema_label'])
    task_node = task_template.query_nodes(key='task')

    if task_node.empty():
        raise ImportError('Unable to load {0} task defintions'.format(schema_name))
    return task_node


class TaskBase(NodeTools):
    """
    Abstract Base class for workflow Task classes
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def cancel(self):
        """
        Cancel the task
        """

        return

    @abc.abstractmethod
    def run_task(self, callback, errorback, **kwargs):
        """
        A task requires a run_task method with the logic on how to run the task
        """

        return

    @property
    def is_active(self):
        """
        Is the task currently active or not
        """

        return self.status in ("submitted","running")

    @property
    def has_input(self):
        """
        Check if input is available

        TODO: does not yet distinguish between predefined input and output
        of other tasks
        """

        return self.task_metadata.input_data.get() is not None

    @property
    def status(self):

        return self.task_metadata.status.get()

    @status.setter
    def status(self, state):

        self.task_metadata.status.value = state

    def next_task(self, exclude_disabled=True):
        """
        Get downstream tasks connected to the current task

        :param exclude_disabled: exclude disabled tasks
        :type exclude_disabled:  :py:bool

        :return:                 downstream task relative to root
        :rtype:                  :py:list
        """

        tasks = []
        for nid in self.neighbors(return_nids=True):
            edge = self.edges.get((self.nid, nid))
            if edge.get('label') == 'task_link':
                task = self.getnodes(nid)
                if exclude_disabled and task.status == 'disabled':
                    continue
                tasks.append(task)

        return tasks

    def previous_task(self):
        """
        Get upstream tasks connected to the current task

        :return: upstream task relative to root
        :rtype:  :py:list
        """

        task_nid = []
        for nid in self.all_parents(return_nids=True):
            edge = self.edges.get((nid, self.nid))
            if edge.get('label') == 'task_link':
                task_nid.append(nid)

        return [self.getnodes(nid) for nid in task_nid]

    def get_input(self):
        """
        Base method for preparing task input

        If the task is configured to store output to disk (store_output == True)
        the dictionary with input data is serialized to JSON and stored in the
        task directory.
        """

        input_data = self.task_metadata.input_data.get(default={})
        input_dict = {}
        for key, value in input_data.items():

            # Resolve reference
            if isinstance(value, PY_STRING):
                input_dict[key] = self._process_reference(value)
            elif isinstance(value, list):
                input_dict[key] = [self._process_reference(v) if isinstance(v, PY_STRING) else v for v in value]
            else:
                input_dict[key] = value

        # Write input to disk as JSON?
        if self.task_metadata.store_output():
            task_dir = self.task_metadata.workdir.get()
            if task_dir and os.path.exists(task_dir):
                input_json = os.path.join(task_dir, 'input.json')
                json.dump(input_dict, open(input_json, 'w'), indent=2)
            else:
                raise WorkflowError('Task directory does not exist: {0}'.format(task_dir))

        return input_dict

    def set_input(self, **kwargs):
        """
        Register task input
        :return:
        """

        data = self.task_metadata.input_data.get(default={})
        data.update(prepaire_data_dict(kwargs))
        self.task_metadata.input_data.set('value', data)

    def get_output(self):
        """
        Get task output

        Return dictionary of output data registered in task_metadata.output_data
        If the data is serialized to a local JSON file, load.

        :return:    Output data
        :rtype:     :py:dict
        """

        output = self.task_metadata.output_data.get(default={})
        project_dir = self._full_graph.query_nodes(key='project_metadata').project_dir()
        if '$ref' in output:

            # $ref should be relative
            ref_path = output['$ref']
            if not os.path.isabs(ref_path):
                ref_path = os.path.join(project_dir, ref_path )

            if os.path.exists(ref_path ):
                output = json.load(open(ref_path))
            else:
                raise WorkflowError('Task {0} ({1}), output.json does not exist at: {2}'.format(self.nid, self.key,
                                                                                                ref_path))

        return output

    def set_output(self, output):
        """
        Set the output of the task.

        If the task is configured to store output to disk (store_output == True)
        the dictionary with output data is serialized to JSON and stored in the
        task directory. A JSON schema $ref directive is added to the project file
        to enable reloading of the output data.
        """

        # Output should be a dictionary for now
        if not isinstance(output, dict):
            raise WorkflowError('Task {0} ({1}). Output should be a dictionary, got {2}'.format(self.nid, self.key,
                                                                                                type(output)))

        # Store to file or not
        if self.task_metadata.store_output():
            project_dir = self._full_graph.query_nodes(key='project_metadata').project_dir()
            task_dir = self.task_metadata.workdir.get()
            if task_dir and os.path.exists(task_dir):

                # Check for file paths, copy data to workdir
                output = collect_data(output, task_dir)

                output_json = os.path.join(task_dir, 'output.json')
                json.dump(output, open(output_json, 'w'), indent=2)

                output = {'$ref': os.path.relpath(output_json, project_dir)}
            else:
                raise WorkflowError('Task directory does not exist: {0}'.format(task_dir))

        outnode = self.task_metadata.output_data
        if outnode.get() is None:
            outnode.set('value', output)

    def _process_reference(self, ref):
        """
        Resolve reference
        """

        if ref.startswith('$'):
            split = ref.strip('$').split('.')
            ref_nid = int(split[0])
            ref_key = split[1]

            reftask = self.getnodes(ref_nid)
            data = reftask.get_output()

            return data.get(ref_key, None)
        return ref

    def validate(self, key=None):
        """
        Validate task data
        """

        is_valid = True
        for node in self.descendants().query_nodes(required=True):
            if node.get() is None:
                logging.error('Parameter "{0}" is required'.format(node.key))
                is_valid = False

        return is_valid

    def prepaire_run(self):
        """
        Task specific preparations just before running the task.

        This method is used by the workflow manager when scheduling a task.
        By default the method performs:

        * Set task metadata
        * Prepare a working directory if `store_output` set and switch to
          that directory

        A task can overload `prepaire_run` with custom methods but the base
        method has to be called using `super`:

            super(Task, self).prepaire_run()
        """

        # Always start of by registering the task as running
        self.status = 'running'
        self.task_metadata.startedAtTime.set()
        logging.info('Task {0} ({1}), status: {2}'.format(self.nid, self.key, self.status))

        # If store_data, create output dir and switch
        if self.task_metadata.store_output():
            project_dir = self._full_graph.query_nodes(key='project_dir').get()
            workdir = self.task_metadata.workdir
            workdir.set('value', os.path.join(project_dir, 'task-{0}-{1}'.format(self.nid, self.key.replace(' ', '_'))))
            workdir.makedirs()

            os.chdir(workdir.get())
