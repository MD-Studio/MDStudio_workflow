# -*- coding: utf-8 -*-

"""
file: module_workdir_test.py

Unit tests the local directory structure for saving the output of tasks
"""

import os
import time
import shutil

from mdstudio_workflow import Workflow
from tests.module.unittest_baseclass import UnittestPythonCompatibility

currpath = os.path.dirname(__file__)
tmp_project_dir = os.path.abspath(os.path.join(currpath, '../files/test_project'))


class TestLocalWorkdir(UnittestPythonCompatibility):

    def setUp(self):
        """
        Build two task workflow
        """

        self.wf = Workflow(project_dir=tmp_project_dir)

        tid1 = self.wf.add_task('test1', custom_func="dummy_task_runners.task_runner")
        tid1.set_input(add_number=10, dummy=2)

        tid2 = self.wf.add_task('test2', custom_func="dummy_task_runners.task_runner")
        tid2.set_input(add_number=8, output_to_disk=True)
        self.wf.connect_task(tid1.nid, tid2.nid)

    def tearDown(self):
        """
        tearDown method called after each unittest to cleanup
        the project directory
        """

        if os.path.exists(tmp_project_dir):
            shutil.rmtree(tmp_project_dir)

    def test_store_output_all(self):
        """
        Test run workflow storing output of all tasks (default)
        """

        # Run the workflow
        self.wf.run()

        # Blocking: wait until workflow is no longer running
        while self.wf.is_running:
            time.sleep(1)

        # Check existence of project dir, tasks dirs and workflow graph file.
        self.assertTrue(os.path.exists(tmp_project_dir))
        self.assertEqual(len([d for d in os.listdir(tmp_project_dir) if os.path.isdir(d)]), 2)
        self.assertTrue(os.path.isfile(os.path.join(tmp_project_dir, 'workflow.jgf')))

        # Check output
        expected = {'test1': 12, 'test2': 20}
        for task in self.wf.get_tasks():
            self.assertEqual(task.get_output().get('dummy'), expected[task.key])

    def test_store_output_partial(self):
        """
        Test run workflow storing output only for last task
        """

        task = self.wf.get_task(key='test1')
        task.task_metadata.store_output.value = False

        # Run the workflow
        self.wf.run()

        # Blocking: wait until workflow is no longer running
        while self.wf.is_running:
            time.sleep(1)

        # Check existence of project dir, tasks dirs and workflow graph file.
        self.assertTrue(os.path.exists(tmp_project_dir))
        self.assertEqual(len([d for d in os.listdir(tmp_project_dir) if os.path.isdir(d)]), 1)
        self.assertTrue(os.path.isfile(os.path.join(tmp_project_dir, 'workflow.jgf')))

        # Check output
        expected = {'test1': 12, 'test2': 20}
        for task in self.wf.get_tasks():
            self.assertEqual(task.get_output().get('dummy'), expected[task.key])
