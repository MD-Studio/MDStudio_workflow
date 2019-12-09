# -*- coding: utf-8 -*-

"""
file: module_workflowspec_test.py

Unit tests for the WorkflowSpec class
"""

import os
import json
import jsonschema
import pkg_resources

from graphit.graph_io.io_pydata_format import write_pydata
from graphit.graph_exceptions import GraphitValidationError

from mdstudio_workflow import WorkflowSpec

from tests.module.dummy_task_runners import task_runner
from tests.module.unittest_baseclass import UnittestPythonCompatibility, STRING_TYPES

currpath = os.path.dirname(__file__)


class TestTaskBaseClass(object):
    """
    Task base class.
    Common test methods for all task types
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup up workflow spec class
        """

        cls.spec = WorkflowSpec()
        cls.task = cls.spec.add_task(task_name='test', task_type=cls.template_name)

    def test_add_task_from_template(self):
        """
        Test creation of class based on JSON schema template in package and
        validate against same template
        """

        self.assertFalse(self.task.empty())

        schema_file = pkg_resources.resource_filename('mdstudio_workflow',
                                                      '/schemas/endpoints/{0}'.format(self.template_file))
        schema = json.load(open(schema_file))
        self.assertIsNone(jsonschema.validate(write_pydata(self.task), schema))

    def test_add_task_default_meta(self):
        """
        Test creation of default Task meta data
        """

        metadata = self.task.task_metadata

        self.assertFalse(metadata.empty())
        self.assertItemsEqual(metadata.children().keys(),
                              [u'status', u'task_id', u'input_data', u'output_data', u'endedAtTime', u'startedAtTime',
                               u'retry_count', u'store_output', u'breakpoint', u'workdir', u'active', u'checks',
                               u'external_task_id'])
        self.assertEqual(metadata.status.value, 'ready')
        self.assertIsInstance(metadata.task_id.value, STRING_TYPES)

    def test_add_task_task_arguments(self):
        """
        Test 'add_task' with task specific arguments
        """

        task = self.spec.add_task('test', breakpoint=True, retry_count=3)

        self.assertEqual(task.task_metadata.breakpoint.value, True)
        self.assertEqual(task.task_metadata.retry_count.value, 3)


class TestWorkflowSpecPythonTask(TestTaskBaseClass, UnittestPythonCompatibility):
    """
    Test addition of Python workflow tasks
    """

    template_name = 'PythonTask'
    template_file = 'workflow_python_task.v1.json'

    def test_python_task_function_loader(self):
        """
        Test loading custom Python functions or classes
        """

        self.task.custom_func.set('value', 'dummy_task_runners.task_runner')
        self.assertEqual(self.task.custom_func.load(), task_runner)

        # Basic dot-separated path validation (regex)
        self.assertRaises(GraphitValidationError, self.task.custom_func.set, 'value', 'dummy_task_runners')


class TestWorkflowSpecBlockingPythonTask(TestTaskBaseClass, UnittestPythonCompatibility):
    """
    Test addition of Blocking Python workflow tasks
    """

    template_name = 'BlockingPythonTask'
    template_file = 'workflow_python_task.v1.json'


class TestWorkflowSpecWampTask(TestTaskBaseClass, UnittestPythonCompatibility):
    """
    Test addition of WAMP workflow tasks
    """

    template_name = 'WampTask'
    template_file = 'workflow_wamp_task.v1.json'
