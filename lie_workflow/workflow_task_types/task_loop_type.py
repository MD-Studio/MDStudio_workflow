# -*- coding: utf-8 -*-

"""
file: task_mapper_type.py

Task for running a Python function in threaded or blocking mode
"""

import logging

from lie_graph.graph_math_operations import graph_join
from lie_workflow.workflow_task_types.task_base_type import TaskBase, load_task_schema, edge_select_transform

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
            graph_join(self._full_graph, TASK.descendants(),
                       links=[(self.nid, i) for i in TASK.children(return_nids=True)])

            # Set unique task uuid
            self.task_metadata.task_id.set('value', self.task_metadata.task_id.create())

    def cancel(self):

        self.status = 'aborted'

    def run_task(self, callback, errorback, **kwargs):
        """
        A task requires a run_task method with the logic on how to run the task

        :param callback:    WorkflowRunner callback method called with the
                            results of the task and the task ID.
        :param errorback:   WorkflowRunner errorback method called when the
                            task failed with the failure message and task ID.
        """

        mapper_arg = self.mapper_arg()
        input = self.get_input()
        next_tasks = self.next_tasks()

        if len(input[mapper_arg]) != len(next_tasks):
            errorback('Looping error, number of mapped arguments does not match number of unique workflow loops',
                      self.nid)

        # Only set mapper argument on next tasks of the loop before they are run
        # Other parameter will be collected by the tasks when the run using
        # get_output
        for i, task in enumerate(next_tasks):

            # Select and transform selected mapper argument based on edge definitions
            arg = edge_select_transform({mapper_arg: input[mapper_arg][i]},
                                        self._full_graph.getedges((self.nid, task.nid)))

            task.set_input(**arg)

        del input[mapper_arg]
        callback(input, self.nid)

    def prepare_run(self, **kwargs):
        #TODO: Check parallel jobs first in case workflow is rerun to prevent
        #      re-duplication of workflows

        prep = super(LoopTask, self).prepare_run(**kwargs)
        if not prep:
            return prep

        # prepare workflow duplication
        mapper_arg = self.mapper_arg()
        if not mapper_arg:
            return False

        input = self.get_input()
        if not mapper_arg in input:
            logging.error('Mapper argument {0} not in task input'.format(mapper_arg))
            return False

        if isinstance(input[mapper_arg], (list, tuple, set)) and len(input[mapper_arg]) > 1:

            def get_sub_workflow(wf, start, end):

                task_linage = []
                end_nid = end.nid

                def _walk_tasks(start):

                    for task in start.next_tasks():
                        tid = task.nid
                        if tid != end_nid and tid not in task_linage:
                            task_linage.append(tid)
                            _walk_tasks(task)

                _walk_tasks(start)

                sub_workflow_nids = []
                if task_linage:
                    for tid in task_linage:
                        task = wf.getnodes(tid).task_graph()
                        sub_workflow_nids.extend(list(task.nodes.keys()))

                return wf.getnodes(sub_workflow_nids)

            v = get_sub_workflow(self._full_graph, self,
                                 self._full_graph.query_nodes({self.node_key_tag: self.loop_end_task()}))

            ww = list(v.nodes.keys())
            subgraph = v.copy(clean=False)
            loose_edges = []
            subgraph_endpoints = []
            for edge in list(subgraph.edges.keys()):
                intr = set(edge).intersection(ww)
                if not intr:
                    subgraph.remove_edge(edge)
                if len(intr) == 1:
                    tid = list(intr)[0]
                    rid = list(set(edge).difference(ww))[0]
                    loose = (tid, rid)
                    if not loose in self._full_graph.edges():
                        loose = (rid, tid)
                    loose_edges.append(loose)
                    subgraph_endpoints.append(tid)

            # Remove loose edges from subgraph
            subgraph.remove_edges(loose_edges)

            # Duplicate subgraph
            for x in range(len(input[mapper_arg])-1):

                mapping = {}
                for nid, attr in subgraph.nodes.items():
                    newnid = self._full_graph.add_node(run_node_new=False, **attr)
                    mapping[nid] = newnid

                for eid, attr in subgraph.edges.items():
                    if eid[0] in mapping and eid[1] in mapping:
                        neweid = (mapping[eid[0]], mapping[eid[1]])
                        self._full_graph.add_edge(neweid, directed=True, run_edge_new=False, **attr)

                for ledge in loose_edges:
                    attr = self._full_graph.edges[(ledge)]
                    self._full_graph.add_edge(mapping.get(ledge[0], ledge[0]), mapping.get(ledge[1], ledge[1]),
                                              run_edge_new=False, directed=self.is_directed, **attr)

        return True

    def validate(self, key=None):
        """
        Loop task specific validation.

        A loop task needs to have a "closing" or "end" task responsible for
        collecting all iteration results.
        """

        # loop_end_task should exist
        loop_end_task = self.loop_end_task()
        if self._full_graph.query_nodes({self.node_key_tag: loop_end_task}).empty():
            logging.error('Loop_end_task "{0}" not in workflow'.format(loop_end_task))
            return False

        return super(LoopTask, self).validate()
