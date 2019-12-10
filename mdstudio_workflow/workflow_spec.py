# -*- coding: utf-8 -*-

"""
file: workflow_spec.py

Classes required to build microservice oriented workflow specifications
that can be run using the `Workflow` runner class
"""

import os
import pkg_resources

from graphit import GraphAxis
from graphit.graph_axis.graph_axis_mixin import NodeAxisTools
from graphit.graph_io.io_jgf_format import write_jgf, read_jgf
from graphit.graph_io.io_jsonschema_format import read_json_schema
from graphit.graph_py2to3 import to_unicode, prepaire_data_dict
from graphit.graph_helpers import renumber_id

from mdstudio_workflow import __version__, logging
from mdstudio_workflow.workflow_common import WorkflowError
from mdstudio_workflow.workflow_task_types import task_types, WORKFLOW_ORM

# Path to default workflow JSON schema part of the module
workflow_metadata_template = pkg_resources.resource_filename('mdstudio_workflow',
                                                             '/schemas/resources/workflow_metadata_template.json')


class WorkflowSpec(object):
    """
    Interface class for building workflow specifications.

    A workflow is a Directed Acyclic Graph (DAG) in which the nodes represent
    tasks and the edges the connections between them. The Workflow class
    acts as manager collecting output from tasks and forwarding it to other
    tasks along the edges (push execution).
    The `mdstudio_workflow` DAG follows the workflow principles as described by the
    Workflow Patterns initiative supporting many but not all of the described
    patterns (http://www.workflowpatterns.com).

    The task node is the basic functional unit in the DAG. It accepts task
    configuration and input from other task nodes and provides these to the
    specific task that may be a microservice or dedicated Python function or
    class that performs work and collects the returned output.
    Offloading work is performed by a dedicated task runner class that
    knows how to call external microservice or local Python function/class in
    asynchronous or blocking mode.
    """

    def __init__(self, workflow=None, **kwargs):
        """
        Init the workflow specification

        If no workflow provided init a default empty one.
        Additional keyword parameters are used to update the workflow project
        metadata.

        :param workflow: workflow specification
        :type workflow:  :graphit:GraphAxis
        :param kwargs:   additional keyword arguments used to update project
                         metadata
        :type kwargs:    :py:dict

        :raises:         WorkflowError, if 'workflow' not valid
        """

        self.workflow = workflow
        if workflow is None:
            self.new()

        if not isinstance(self.workflow, GraphAxis):
            raise WorkflowError('Not a valid workflow {0}'.format(self.workflow))

        # Update project metadata
        if kwargs:
            project_metadata = self.workflow.query_nodes(key='project_metadata').descendants()
            if not project_metadata.empty():
                project_metadata.update(kwargs)

    def __len__(self):
        """
        Implement class __len__

        :return: return number of tasks in workflow
        :rtype:  :py:int
        """

        return len(self.workflow.query_nodes(format='task'))

    def add_task(self, task_name, task_type='PythonTask', **kwargs):
        """
        Add a new task to the workflow from the set of supported workflow
        task types defined in the workflow ORM.

        The 'new' method of each task type is called at first creation to
        construct the task data object in the graph.
        Additional keyword arguments provided to the 'add_task' method will
        used to update the task data

        :param task_name: administrative name of the task
        :type task_name:  :py:str
        :param task_type: task type to add
        :type task_type:  :py:str
        :param kwargs:    additional keyword arguments passed to the task
                          init_task method.
        :type kwargs:     :py:dict

        :return:          Task object
        :rtype:           :graphit:GraphAxis

        :raises:          WorkflowError, unsupported 'task_type'
        """

        # Task type needs to be supported by ORM
        if task_type not in task_types:
            raise WorkflowError('Task type "{0}" not supported, requires: {1}'.format(task_type, ', '.join(task_types)))

        # Add the task as node to the workflow graph. The task 'new' method is
        # called for initial task initiation.
        nid = self.workflow.add_node(task_name, run_node_new=True, task_type=task_type, format='task')

        # Update Task metadata
        task = self.workflow.getnodes(nid)
        task.descendants().update(kwargs)

        # If this is the first task added, set the root to task nid
        if len(self.workflow.query_nodes(format='task')) == 1:
            self.workflow.root = nid

        return task

    def connect_task(self, task1, task2, *args, **kwargs):
        """
        Connect tasks by task ID (graph nid)

        Creates the directed graph edge connecting two tasks (nodes) together.
        An edge also defines which parameters in the output of one task serve
        as input for another task and how they are named.

        Parameter selection is defined by all additional arguments and keyword
        arguments provided to `connect_task`. Keyword arguments also define the
        name translation of the argument.

        :param task1:         first task of two tasks to connect
        :type task1:          :py:int
        :param task2:         second task of two tasks to connect
        :type task2:          :py:int

        :return:              edge identifier
        :rtype:               :py:tuple
        """

        for task in (task1, task2):
            if task not in self.workflow.nodes:
                raise WorkflowError('Task {0} not in workflow'.format(task))
            if self.workflow.nodes[task].get('format') != 'task':
                raise WorkflowError('Node {0} not of format "task"'.format(task))

        if task1 == task2:
            raise WorkflowError('Connection to self not allowed')

        edge_data = {'key': u'task_link'}
        data_mapping = prepaire_data_dict(kwargs)
        if data_mapping:
            edge_data['data_mapping'] = data_mapping
        if len(args):
            data_select = [to_unicode(arg) for arg in args]

            # If data_select and data_mapping, the mapping keys should be in data_select
            for key in data_mapping:
                if key not in data_select:
                    data_select.append(key)
                    logging.debug('Added {0} data key to data selection list'.format(key))

            edge_data['data_select'] = data_select

        eid = self.workflow.add_edge(task1, task2, **edge_data)

        return eid

    def get_tasks(self):
        """
        Return all tasks in a workflow

        :return:          workflow graph task objects
        :rtype:           :py:list
        """

        tasks = self.workflow.query_nodes(format='task')
        if len(tasks) == 1:
            return [tasks]

        return list(tasks)

    def new(self, schema=workflow_metadata_template):
        """
        Construct new empty workflow based on template JSON Schema file

        :param schema: JSON schema
        """

        # Build workflow template from schema.
        # Set 'directed' to True to import JSON schema as directed graph
        template_graph = GraphAxis()
        template_graph.directed = True
        template = read_json_schema(schema, graph=template_graph, exclude_args=['title', 'description', 'schema_label'])

        self.workflow = template.query_nodes(key='project_metadata').descendants(include_self=True).copy()
        self.workflow.node_tools = NodeAxisTools
        self.workflow.orm = WORKFLOW_ORM
        renumber_id(self.workflow, 1)

        # Update workflow meta-data
        metadata = self.workflow.query_nodes(key='project_metadata')
        metadata.create_time.set()
        metadata.user.set()
        metadata.version.set(metadata.data.value_tag, __version__)

        logging.info('Init default empty workflow')

    def load(self, workflow):
        """
        Load workflow specification

        Initiate a workflow from a workflow specification or instance thereof.

        :param workflow: File path to predefined workflow in .jgf format
        :type workflow:  :py:str
        """

        # Construct a workflow GraphAxis object
        self.workflow = read_jgf(workflow)
        self.workflow.node_tools = NodeAxisTools
        self.workflow.orm = WORKFLOW_ORM

        assert self.workflow.root is not None, WorkflowError('Workflow does not have a root node defined')

        # Get metadata
        metadata = self.workflow.query_nodes(key='project_metadata')
        logging.info('Load workflow "{0}"'.format(metadata.title.get()))
        logging.info('Created: {0}, updated: {1}'.format(metadata.create_time.get(), metadata.update_time.get()))
        logging.info('Description: {0}'.format(metadata.description.get()))

    def save(self, path=None):
        """
        Serialize the workflow specification to a graph JSON format (.jgf)

        :param path: optionally write JSON string to file
        :type path:  :py:str

        :return: serialized workflow
        :rtype:  :py:str
        """

        # Update workflow meta-data
        metadata = self.workflow.query_nodes(key='project_metadata')

        json_string = write_jgf(self.workflow)
        if path:
            path = os.path.abspath(path)
            pred = os.path.exists(os.path.dirname(path))
            msg = 'Directory does not exist: {0}'.format(os.path.dirname(path))
            assert pred, msg
            try:
                with open(path, 'w') as json_to_file:
                    json_to_file.write(json_string)
                logging.info('Save workflow "{0}" to file: {1}'.format(metadata.title.get(), path))
            except IOError:
                logging.error('Unable to write workflow to file: {0}'.format(path))

        return json_string
