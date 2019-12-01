# -*- coding: utf-8 -*-

"""
file: task_base_type.py

Abstract base class defining the Task interface including methods that every
task type should implement
"""

import abc
import pkg_resources
import json
import os

from graphit import GraphAxis
from graphit.graph_mixin import NodeTools
from graphit.graph_py2to3 import prepaire_data_dict
from graphit.graph_io.io_jsonschema_format import read_json_schema

from mdstudio_workflow.workflow_common import WorkflowError, collect_data, concat_dict

# Set twisted logger
from twisted.python.failure import Failure
from twisted.logger import Logger
logging = Logger()


def load_task_schema(schema_name):
    """
    Load task template graphs from JSON Schema template files

    Template files are located in the package /schemas/endpoints directory

    :param schema_name: task JSON Schema file name to load
    :type schema_name:  :py:str

    :return:            directed GraphAxis task template graph
    :rtype:             :graphit:GraphAxis

    :raises:            ImportError, unable to import JSON template
    """

    # Set 'directed' to True to import JSON schema as directed graph
    template_graph = GraphAxis(directed=True)

    task_schema = pkg_resources.resource_filename('mdstudio_workflow', '/schemas/endpoints/{0}'.format(schema_name))
    task_template = read_json_schema(task_schema, graph=template_graph,
                                     exclude_args=['title', 'description', 'schema_label'])
    task_node = task_template.query_nodes(key='task')

    if task_node.empty():
        raise ImportError('Unable to load {0} task definitions'.format(schema_name))
    return task_node


def edge_select_transform(data, edge):
    """
    Select and transform output from previous task based on selection and
    mapping definitions stored in the edge connecting two tasks.

    If there is no selection defined, all output will be forwarded as
    input to the new task.

    :param data:  output of previous task
    :type data:   :py:dict
    :param edge:  edge connecting tasks
    :type edge:   :graphit:GraphAxis

    :return:      curated output
    :rtype:       :py:dict
    """

    mapper = edge.get('data_mapping', {})
    select = edge.get('data_select', data.keys())

    def recursive_key_search(keys, search_data):

        if keys[0] in search_data:
            search_data = search_data[keys[0]]
        else:
            return None

        if len(keys) > 1:
            return recursive_key_search(keys[1:], search_data)
        return search_data

    transformed_data = {}
    for key in select:

        # search recursively for dot separated keys
        value = recursive_key_search(key.split('.'), data)
        if value is None:
            logging.warn('Data selection: parameter {0} not in output of task {1}'.format(key, edge.nid))
            continue

        key = key.split('.')[-1]
        transformed_data[mapper.get(key, key)] = value

    return transformed_data


def load_referenced_output(output_dict, base_path=None):
    """
    Resolve referred output

    Referred output is defined in the output dictionary using the JSON Schema
    '$ref' keyword that points to a json file on disk.

    :param output_dict: output dict to be updated
    :type output_dict:  :py:dict
    :param base_path:   base project path
    :type base_path:    :py:str

    :return:            updated output dict
    :rtype:             :py:dict
    """

    for key, value in output_dict.items():

        if key == '$ref':

            if not os.path.isabs(value):
                value = os.path.join(base_path, value)

            if os.path.exists(value):
                json_parsed = json.load(open(value))
                if isinstance(json_parsed, dict):
                    output_dict.update(json_parsed)
                del output_dict[key]
            else:
                raise WorkflowError('No such references output file: {0}'.format(value))

        elif isinstance(value, dict):
            output_dict[key] = load_referenced_output(value, base_path=base_path)

    return output_dict


class TaskBase(NodeTools):
    """
    Abstract Base class for workflow Task classes

    Defines abstract methods and common methods for task specific functions.
    Every task type needs to inherit from `TaskBase`. Abstract methods need
    to be implemented and common methods may be overloaded or extended.
    In the latter case, don't forget to use `super` to call the base
    implementation in the base class.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run_task(self, callback, **kwargs):
        """
        Run the task itself

        This method contains the logic for running the task itself using the
        data stored within the task data structure (graph) or provided
        externally.

        The method requires a callback method as argument. This is the
        `WorkflowRunner.output_callback` method by default. This method needs
        to be called with the output of the task and the task ID as arguments
        whatever the status of the tak may be. This may be:

        * A dictionary representing the results of a task when finished.
        * A dictionary representing a 'Future' object that is required to
          check the progress of the task at regular intervals
        * None in case the tasked failed.

        The `run_task` method may accept any additional number of keyword
        arguments required for it to perform its function.

        :param callback:    WorkflowRunner callback method called with the
                            results of the task and the task ID.
        :param kwargs:      Additional keyword arguments required by the
                            method.
        :type kwargs:       :py:dict
        """

        return

    @property
    def is_active(self):
        """
        Is the task currently active or not

        Not active is any status other then 'submitted' or 'running'

        :rtype: :py:bool
        """

        return self.status in ('submitted', 'running')

    @property
    def has_input(self):
        """
        Check if input is available

        :rtype: :py:bool
        """

        return self.task_metadata.input_data.get() is not None

    @property
    def status(self):
        """
        Return the current status of the task

        :return: status as 'ready', 'submitted', 'running', 'failed', 'aborted',
                 'completed' or 'disabled'
        :rtype:  :py:str
        """

        return self.task_metadata.status.get()

    @status.setter
    def status(self, state):
        """
        Set the status of the task

        :param state: status as 'ready', 'submitted', 'running', 'failed',
                      'aborted', 'completed' or 'disabled'
        :type state:  :py:str
        """

        self.task_metadata.status.set(self.data.value_tag, state)

    def cancel(self, **kwargs):
        """
        Cancel the task

        This method will perform a administrative canceling of the task by
        default. This involves changing the task status to 'aborted' and
        making it inactive.

        The method may be overloaded or extended by specific Task classes to
        actively cancel the associated task by for instance 'killing' an
        active process based on process ID or canceling a remote WAMP task
        using the service job cancel endpoint.

        :param kwargs: additional keyword arguments required to cancel the task
        :type kwargs:  :py:dict
        """

        if not self.is_active:
            logging.info('Unable to cancel task {0} ({1}) not active'.format(self.nid, self.key))
            return

        self.status = 'aborted'
        self.task_metadata.active.value = False
        logging.info('Canceled task {0} ({1})'.format(self.nid, self.key))

    def get_input(self, **kwargs):
        """
        Prepare task input

        The final input of a task is a combination between the input defined as
        part of the task itself (using `set_input`) and the output of the tasks
        connected to it. The latter may be post-processed selecting and
        transforming data based on edge definitions.

        mdstudio_workflows are dynamic and therefore the `get_input` and `get_output`
        methods are always called even if input/output was previously stored
        locally.

        If the task is configured to store output to disk (store_output == True)
        the dictionary with input data is serialized to JSON and stored in the
        task directory.

        :param kwargs: additional keyword arguments required to create the
                       dictionary of input data
        :type kwargs:  :py:dict

        :return:       input data to the task such as used in the `run_task`
                       method
        :rtype:        :py:dict
        """

        # Get output of tasks connected to this task that are completed
        collected_input = []
        for prev_task in self.previous_tasks(status='completed'):

            # Select and transform based on edge definitions
            task_output = edge_select_transform(prev_task.get_output(),
                                                self.origin.getedges((prev_task.nid, self.nid)))
            collected_input.append(task_output)

        # Concatenate multiple input dictionaries to a new dict
        input_dict = {}
        if collected_input:
            input_dict = concat_dict(collected_input)

        # Get input defined in current task and update
        input_dict.update(self.task_metadata.input_data.get(default={}))

        # Update with additional keyword arguments
        input_dict.update(kwargs)

        # Write input to disk as JSON? Task working directory should exist
        if self.task_metadata.store_output():
            input_json = os.path.join(self.task_metadata.workdir.get(), 'input.json')
            json.dump(input_dict, open(input_json, 'w'), indent=2)

        return input_dict

    def get_output(self, **kwargs):
        """
        Prepare task output

        Return dictionary of output data registered in task_metadata.output_data
        If the data is serialized to a local JSON file, load.

        :param kwargs: additional keyword arguments required to create the
                       dictionary of output data
        :type kwargs:  :py:dict

        :return:       output data of the task
        :rtype:        :py:dict
        """

        output_dict = self.task_metadata.output_data.get(default={})
        if not output_dict:
            logging.info('Task {0} ({1}). No output'.format(self.nid, self.key))

        output_dict = load_referenced_output(output_dict,
                                        base_path=self.origin.query_nodes(key='project_metadata').project_dir())
        output_dict.update(kwargs)

        return output_dict

    def next_tasks(self, exclude_disabled=True):
        """
        Get downstream tasks connected to the current task

        :param exclude_disabled: exclude disabled tasks
        :type exclude_disabled:  :py:bool

        :return:                 downstream task relative to root
        :rtype:                  :py:list
        """

        nxt = self.children(include_self=True).query_edges(key='task_link')

        if exclude_disabled:
            return [task for task in nxt if task.nid != self.nid and task.status != 'disabled']
        return [task for task in nxt if task.nid != self.nid]

    def previous_tasks(self, status=None):
        """
        Get upstream tasks connected to the current task

        :param status:  filter previous tasks based on task status
                        no filtering by default
        :type status:   :py:str

        :return:        upstream task relative to root
        :rtype:         :py:list
        """

        pvt = self.all_parents(include_self=True).query_edges(key='task_link')

        if status is not None:
            return [task for task in pvt if task.nid != self.nid and task.status == status]
        return [task for task in pvt if task.nid != self.nid]

    def prepare_run(self, **kwargs):
        """
        Task specific preparations just before running the task.

        This method is used by the workflow manager when scheduling a task.
        By default the method performs:

        * Set task metadata
        * Prepare a working directory if `store_output` set and switch to
          that directory

        A task can overload `prepare_run` with custom methods but the base
        method has to be called using `super`:

            super(Task, self).prepare_run()

        :return:    task preparation status
        :rtype:     :py:bool
        """

        # If store_data, create output dir and switch
        if self.task_metadata.store_output():
            project_dir = self.origin.query_nodes(key='project_dir').get()
            workdir = self.task_metadata.workdir
            workdir.set(self.data.value_tag,
                        os.path.join(project_dir, 'task-{0}-{1}'.format(self.nid, self.key.replace(' ', '_'))))
            path = workdir.makedirs()

            os.chdir(path)

        return True

    def set_input(self, *args, **kwargs):
        """
        Register task input

        Set task input directly in the task itself.
        Input is defined as predefined dictionary or keyword arguments
        to the method.

        :param args:    predefined dictionary of task input
        :type args:     :py:dict
        :param kwargs:  input data as keyword arguments
        :type kwargs:   :py:dict
        """

        data = self.task_metadata.input_data.get(default={})

        predefined = [n for n in args if isinstance(n, dict)]
        predefined.append(kwargs)
        for indict in predefined:
            data.update(prepaire_data_dict(indict))

        self.task_metadata.input_data.set(self.data.value_tag, data)

    def set_output(self, output, **kwargs):
        """
        Set the output of the task.

        If the task is configured to store output to disk (store_output == True)
        the dictionary with output data is serialized to JSON and stored in the
        task directory. A JSON schema $ref directive is added to the project file
        to enable reloading of the output data.

        :param output:  task output data to set
        :type output:   :py:dict
        :param kwargs:  additional output data as keyword arguments
        :type kwargs:   :py:dict

        :raises:        WorkflowError, output should be of type 'dict',
                        task directory should exist if store_output
        """

        # Output should be a dictionary for now
        if not isinstance(output, dict):
            raise WorkflowError('Task {0} ({1}). Output should be a dictionary, got {2}'.format(self.nid, self.key,
                                                                                                type(output)))

        # Update with any keyword arguments
        output.update(kwargs)

        # Store to file or not
        if self.task_metadata.store_output():
            project_dir = self.origin.query_nodes(key='project_metadata').project_dir()
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
            outnode.set(self.data.value_tag, output)

    def task_graph(self):
        """
        Returns all task specific nodes as one task subgraph

        By default, a task node only represents the root or head-node of a task
        subgraph containing more task specific nodes such as task configuration
        and results. This method return all of these nodes as a subgraph view.

        :rtype: :graphit:GraphAxis
        """

        next_tasks = [t.nid for t in self.next_tasks()]

        node_tasks = [self.nid]
        for nid in self.adjacency[self.nid]:
            if nid not in next_tasks:
                node_tasks.extend(self.getnodes(nid).descendants(include_self=True, return_nids=True))

        return self.getnodes(node_tasks)

    def update(self, output):
        """
        Update the task metadata

        Usually called when the task is launched, the status is checked or the
        results are processed.

        :return:    task status after update
        :rtype:     :py:str
        """

        self.task_metadata.checks.set(self.data.value_tag, self.task_metadata.checks.get(default=0) + 1)

        # Output or not
        status = 'failed'
        if output is None:
            logging.error('Task {0} ({1}) returned no output'.format(self.nid, self.key))

            # If task successfully run before, use output.
            prev_output = self.get_output()
            if prev_output and isinstance(prev_output, dict) and self.status == 'completed':
                logging.info('Task {0} ({1}) successfully run before. Use output'.format(self.nid, self.key))
                return 'completed', prev_output

        elif isinstance(output, Failure):
            logging.error('Task {0} ({1}) returned a failure: {2}'.format(self.nid, self.key, output.printTraceback()))
        elif not isinstance(output, dict):
            logging.error('Task {0} ({1}) returned unexpected {2} object'.format(self.nid, self.key, type(output)))
        else:
            status = output.get('status', 'completed')

        self.status = status
        if status == 'failed':
            output = {}
            self.task_metadata.endedAtTime.set()
        elif status == 'completed':
            self.set_output(output)
            self.task_metadata.endedAtTime.set()

        if 'status' in output:
            del output['status']

        logging.info('Task {0} ({1}), status: {2}'.format(self.nid, self.key, status))
        return status, output

    def validate(self, key=None):
        """
        Validate task

        Run task data validation. Usually called at the beginning of the
        workflow execution.
        By default the method validates if all required data is set.
        """

        is_valid = True
        for node in self.descendants().query_nodes(required=True):
            if node.get() is None:
                logging.error('Parameter "{0}" is required'.format(node.key))
                is_valid = False

        return is_valid
