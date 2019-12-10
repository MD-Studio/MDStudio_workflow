# -*- coding: utf-8 -*-

"""
file: module_mapper_test.py

Unit tests construction and running the mapreduce workflow:

           6 -- 7
          /      \
    1 -- 2 -- 3 -- 4 -- 5
         \      /
          8 -- 9

"""

import os
import time

from tests.module.unittest_baseclass import UnittestPythonCompatibility

from mdstudio_workflow import Workflow, WorkflowSpec

currpath = os.path.dirname(__file__)
workflow_file_path = os.path.abspath(os.path.join(currpath, '../files/test-mapreduce-workflow.jgf'))


class TestBuildMapperWorkflow(UnittestPythonCompatibility):
    """
    Build the map-reduce workflow a shown in the file header using the default
    threader PythonTask runner
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup up workflow spec class
        """

        cls.spec = Workflow()

    def test1_set_project_meta(self):
        """
        Set project meta data
        """

        metadata = self.spec.workflow.query_nodes(key='project_metadata')
        self.assertFalse(metadata.empty())

        metadata.title.set('value', 'Simple mapper workflow')
        metadata.description.set('value', 'Test a simple mapreduce workflow of 9 threaded python tasks')

        self.assertTrue(all([n is not None for n in [metadata.title(), metadata.description()]]))

    def test2_add_methods(self):
        """
        Test adding 10 blocking python tasks
        """

        # Add first task, serves as empty container. Input passed as output.
        self.spec.add_task('test1', store_output=False)

        # Add mapper function
        self.spec.add_task('test2', task_type='LoopTask', mapper_arg='dummy', loop_end_task='test5', store_output=False)

        # Add mapper workflow
        for task in range(3, 7):
            self.spec.add_task('test{0}'.format(task), custom_func="dummy_task_runners.task_runner", store_output=False)

        self.assertEqual(len(self.spec), 5)

    def test3_add_connections(self):
        """
        Test connecting 9 tasks in a branched fashion
        """

        edges = ((1, 2), (2, 3), (3, 4), (4, 5), (4, 6))
        tasks = dict([(i, t.nid) for i, t in enumerate(self.spec.get_tasks(), start=1)])

        for edge in edges:
            self.spec.connect_task(tasks[edge[0]], tasks[edge[1]])

    def test4_define_input(self):
        """

        """

        self.spec.input(self.spec.workflow.root, dummy=[1, 2, 3, 4])

    def test5_run_workflow(self):
        """
        Test running the workflow
        """

        # Run the workflow
        self.spec.run()

        # Blocking: wait until workflow is no longer running
        while self.spec.is_running:
            time.sleep(1)
