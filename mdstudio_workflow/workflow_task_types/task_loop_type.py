# -*- coding: utf-8 -*-

"""
file: task_mapper_type.py

Task for running a Python function in threaded or blocking mode
"""

from graphit.graph_utils.graph_utilities import edges_parent_to_subgraph
from graphit.graph_combinatorial.graph_split_join_operations import graph_join

from mdstudio_workflow import logging
from mdstudio_workflow.workflow_task_types.task_base_type import TaskBase, load_task_schema, edge_select_transform

# Preload Task definitions from JSON schema in the package schema/endpoints/
TASK_SCHEMA = 'workflow_loop_task.v1.json'
TASK = load_task_schema(TASK_SCHEMA)


class LoopTask(TaskBase):
    """
    Parallelization task by mapping input
    """

    def new(self, **kwargs):
        """
        Implements 'new' abstract base class method to create new
        task node tree.

        Load task from JSON Schema workflow_mapper_task.v1.json in package
        /schemas/endpoints folder.
        """

        # Do not initiate twice in case method gets called more then once.
        if not len(self.children()):

            logging.info('Init task {0} ({1}) from schema: {2}'.format(self.nid, self.key, TASK_SCHEMA))
            graph_join(self.origin, TASK.descendants(),
                       links=[(self.nid, i) for i in TASK.children(return_nids=True)])

            # Set unique task uuid
            self.task_metadata.task_id.set(self.data.value_tag, self.task_metadata.task_id.create())

    def cancel(self):

        self.status = 'aborted'

    def run_task(self, callback, **kwargs):
        """
        A task requires a run_task method with the logic on how to run the task

        :param callback:    WorkflowRunner callback method called with the
                            results of the task and the task ID.
        """

        mapper_arg = self.mapper_arg()
        input = self.get_input()
        next_tasks = self.next_tasks()

        if len(input[mapper_arg]) != len(next_tasks):
            logging.error('Looping error, number of mapped arguments does not match number of unique workflow loops')
            callback(None, self.nid)

        # Only set mapper argument on next tasks of the loop before they are run
        # Other parameter will be collected by the tasks when the run using
        # get_output
        for i, task in enumerate(next_tasks):

            # Select and transform selected mapper argument based on edge definitions
            arg = edge_select_transform({mapper_arg: input[mapper_arg][i]},
                                        self.origin.getedges((self.nid, task.nid)))

            task.set_input(**arg)

        del input[mapper_arg]
        callback(input, self.nid)

    def prepare_run(self, **kwargs):
        """
        Loop task specific preparations just before running the task.

        #TODO: Check parallel jobs first in case workflow is rerun to prevent
        #      re-duplication of workflows

        :return:    task preparation status
        :rtype:     :py:bool
        """

        # Run base checks first
        prep = super(LoopTask, self).prepare_run(**kwargs)
        if not prep:
            return prep

        # Check if mapper argument defined in input
        input = self.get_input()
        mapper_arg = self.mapper_arg()
        if mapper_arg is None or not mapper_arg in input:
            logging.error('Mapper argument {0} not in task input'.format(mapper_arg))
            return False

        # Mapper should be mappable
        if not isinstance(input[mapper_arg], (list, tuple, set)):
            logging.error('Mapper argument {0} is not a container of items'.format(mapper_arg))
            return False

        # Mapper should contain more than one item
        if len(input[mapper_arg]) <= 1:
            logging.error('Mapper argument {0} should contain more then one item'.format(mapper_arg))
            return False

        # Duplicate subgraphs
        start = self.origin.getnodes([t.nid for t in self.next_tasks()])
        ds = start.descendants(include_self=True)

        finish = self.origin.query_nodes({self.data.key_tag: self.loop_end_task(), 'format': 'task'})
        if finish.empty():
            logging.error("Loop end task '{0}' not found in workflow".format(self.loop_end_task()))
            return False
        df = finish.descendants(include_self=True)

        subgraph = self.origin.getnodes(ds.nodes.difference(df.nodes))
        connecting_edges = edges_parent_to_subgraph(subgraph)

        for x in range(len(input[mapper_arg])-1):

            mapping = graph_join(self.origin, subgraph, run_node_new=False, run_edge_new=False)
            for ledge in connecting_edges:
                attr = self.origin.edges[ledge]
                self.origin.add_edge(mapping.get(ledge[0], ledge[0]), mapping.get(ledge[1], ledge[1]),
                                          run_edge_new=False, directed=self.origin.directed, **attr)

        logging.info('Copied sub-workflow of {0} tasks, {1} times'.format(len(subgraph.query_nodes(format='task')),
                                                                          len(input[mapper_arg])-1))

        return True

    def validate(self, key=None):
        """
        Loop task specific validation.

        A loop task needs to have a "closing" or "end" task responsible for
        collecting all iteration results.
        """

        # loop_end_task should exist
        loop_end_task = self.loop_end_task()
        if self.origin.query_nodes({self.data.key_tag: loop_end_task}).empty():
            logging.error('Loop_end_task "{0}" not in workflow'.format(loop_end_task))
            return False

        return super(LoopTask, self).validate()
