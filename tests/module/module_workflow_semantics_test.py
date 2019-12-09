# -*- coding: utf-8 -*-

"""
file: module_workflow_semantics_test.py

Unit tests naming and renaming of task parameters between tasks
"""

import os
import time

from mdstudio_workflow import Workflow
from mdstudio_workflow.workflow_task_types.task_base_type import edge_select_transform

from tests.module.unittest_baseclass import UnittestPythonCompatibility

currpath = os.path.dirname(__file__)


class TestEdgeSelectTransform(UnittestPythonCompatibility):

    def test_default(self):
        """
        No select/mapping directives in edge, all data returned
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {}

        self.assertDictEqual(edge_select_transform(data, edge), data)

    def test_data_select(self):
        """
        Only select 'dummy' attribute
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_select': ['dummy']}

        self.assertDictEqual(edge_select_transform(data, edge), {'dummy': 12})

    def test_data_select_two(self):
        """
        Select 'dummy' and 'param2' attributes
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_select': ['dummy', 'param2']}

        self.assertDictEqual(edge_select_transform(data, edge), {'dummy': 12, 'param2': [1, 2, 3]})

    def test_data_select_undefined(self):
        """
        When data is not available log a warning but continue as the parameter
        may be optional.
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_select': ['dummy', 'param4']}

        self.assertDictEqual(edge_select_transform(data, edge), {'dummy': 12})

    def test_data_mapping(self):
        """
        Return all data but translate 'dummy' to 'bar'
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_mapping': {'dummy': 'bar'}}

        self.assertDictEqual(edge_select_transform(data, edge),
                             {'bar': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5})

    def test_data_mapping_two(self):
        """
        Return all data but translate 'param3' to 'dummy'.
        Results in dummy parameter being overwritten by param3 value
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_mapping': {'param3': 'dummy'}}

        self.assertDictEqual(edge_select_transform(data, edge),
                             {'dummy': 5, 'param1': True, 'param2': [1, 2, 3]})

    def test_data_mapping_three(self):
        """
        Return all data but translate two parameters
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_mapping': {'param1': 'bar1', 'param2': 'bar2'}}

        self.assertDictEqual(edge_select_transform(data, edge),
                             {'dummy': 12, 'bar1': True, 'bar2': [1, 2, 3], 'param3': 5})

    def test_data_mapping_four(self):
        """
        Mapping may result in key/value pairs being overwritten.
        Parameter renaming is handled in alphabetic order.
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}
        edge = {'data_mapping': {'dummy': 'param1', 'param1': 'dummy'}}

        self.assertDictEqual(edge_select_transform(data, edge),
                             {'param1': 12, 'dummy': True, 'param2': [1, 2, 3], 'param3': 5})

    def test_data_mapping_select(self):
        """
        Key/value mapping overwrites can be limited by selecting data first
        """

        data = {'dummy': 12, 'param1': True, 'param2': [1, 2, 3], 'param3': 5}

        edge = {'data_mapping': {'dummy': 'param1', 'param1': 'dummy'}, 'data_select': ['param1']}
        self.assertDictEqual(edge_select_transform(data, edge), {'dummy': True})

        edge = {'data_mapping': {'dummy': 'param1', 'param1': 'dummy'}, 'data_select': ['dummy']}
        self.assertDictEqual(edge_select_transform(data, edge), {'param1': 12})

    def test_data_select_nested(self):
        """
        Collect single key/value pairs in nested data sets
        """

        data = {'dummy': 12, 'param1': True, 'param2': {'dummy': 5}}
        edge = {'data_select': ['param2.dummy']}

        self.assertDictEqual(edge_select_transform(data, edge), {'dummy': 5})

    def test_data_select_nested_two(self):
        """
        Identically named (nested) parameters are handled in alphabetic order
        """

        data = {'dummy': 12, 'param1': True, 'param2': {'dummy': 5}}
        edge = {'data_select': ['param2.dummy', 'dummy']}

        self.assertDictEqual(edge_select_transform(data, edge), {'dummy': 12})

    def test_data_select_nested_three(self):
        """
        Select and rename nested parameters
        """

        data = {'dummy': 12, 'param1': True, 'param2': {'dummy': 5}}
        edge = {'data_select': ['param2.dummy', 'dummy'], 'data_mapping': {'param2.dummy': 'bar'}}

        self.assertDictEqual(edge_select_transform(data, edge), {'bar': 5, 'dummy': 12})


class TestInputOutputMapping(UnittestPythonCompatibility):

    def setUp(self):
        """
        Build a two task workflow
        """

        self.wf = Workflow()

        self.tid1 = self.wf.add_task('test1', custom_func="dummy_task_runners.task_runner", store_output=False)
        self.tid1.set_input(add_number=10, dummy=2, return_more=True)

        self.tid2 = self.wf.add_task('test2', custom_func="dummy_task_runners.task_runner", store_output=False)
        self.tid2.set_input(add_number=8, return_more=True)

    def test_run_default(self):
        """
        Test run workflow storing output of all tasks.
        Default task connection communicates all results
        """

        # Default task connect
        self.wf.connect_task(self.tid1.nid, self.tid2.nid)

        # Run the workflow
        self.wf.run()

        # Blocking: wait until workflow is no longer running
        while self.wf.is_running:
            time.sleep(1)

        # Check expected output. All output should be returned
        expected = {'test1': 12, 'test2': 20}
        for task in self.wf.get_tasks():
            self.assertEqual(task.get_output().get('dummy'), expected[task.key])

    def test_run_keyword_selection(self):
        """
        Test run workflow storing output of all tasks
        Task connection communicating only the 'dummy' output variable
        """

        # Connect tasks with keyword selection
        self.wf.connect_task(self.tid1.nid, self.tid2.nid, 'dummy')

        # Run the workflow
        self.wf.run()

        # Blocking: wait until workflow is no longer running
        while self.wf.is_running:
            time.sleep(1)

        # Check expected output.
        expected = {'test1': 12, 'test2': 20}
        for task in self.wf.get_tasks():
            self.assertEqual(task.get_output().get('dummy'), expected[task.key])

    def test_run_keyword_mapping(self):
        """
        Test run workflow storing output of all tasks
        Task connection translates the param3 parameter to 'dummy'
        """

        # Connect tasks with keyword selection and mapping
        self.wf.connect_task(self.tid1.nid, self.tid2.nid, param3='dummy')

        # Run the workflow
        self.wf.run()

        # Blocking: wait until workflow is no longer running
        while self.wf.is_running:
            time.sleep(1)

        # Check expected output.
        expected = {'test1': 12, 'test2': 13}
        for task in self.wf.get_tasks():
            self.assertEqual(task.get_output().get('dummy'), expected[task.key])

    def test_run_keyword_selection_mapping(self):
        """
        Test run workflow storing output of all tasks
        Task connection communicating the 'dummy' and 'param3' output variables
        where param3 is translated to 'dummy', replacing default 'dummy'
        """

        # Connect tasks with keyword selection and mapping
        self.wf.connect_task(self.tid1.nid, self.tid2.nid, 'dummy', param3='dummy')

        # Run the workflow
        self.wf.run()

        # Blocking: wait until workflow is no longer running
        while self.wf.is_running:
            time.sleep(1)

        # Check expected output.
        expected = {'test1': 12, 'test2': 13}
        for task in self.wf.get_tasks():
            self.assertEqual(task.get_output().get('dummy'), expected[task.key])
