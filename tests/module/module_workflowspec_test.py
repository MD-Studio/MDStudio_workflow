# -*- coding: utf-8 -*-

"""
file: module_workflowspec_test.py

Unit tests for the WorkflowSpec class
"""

import os
import json
import jsonschema

from graphit.graph_io.io_pydata_format import write_pydata
from tests.module.unittest_baseclass import UnittestPythonCompatibility

from mdstudio_workflow import WorkflowSpec
from mdstudio_workflow.workflow_common import WorkflowError
from mdstudio_workflow.workflow_spec import workflow_metadata_template

currpath = os.path.abspath(os.path.dirname(__file__))


class TestWorkflowSpec(UnittestPythonCompatibility):
    """
    Test WorkflowSpec class
    """

    tempfiles = []

    def tearDown(self):
        """
        tearDown method called after each unittest to cleanup
        the working directory
        """

        for temp in self.tempfiles:
            if os.path.exists(temp):
                os.remove(temp)

    def test_new_default(self):
        """
        Test creation of new empty workflow based on JSON Schema.
        """

        spec = WorkflowSpec()

        self.assertEqual(spec.workflow.root, 1)

        # Test meta data
        metadata = spec.workflow.query_nodes(key='project_metadata')
        self.assertFalse(metadata.empty())
        self.assertTrue(metadata.parent().empty())  # Metadata is not rooted.
        self.assertItemsEqual(metadata.children().keys(), [u'update_time', u'description', u'title', u'finish_time',
                                                           u'start_time', u'create_time', u'user', u'version',
                                                           u'project_dir'])

        # Test schema, should validate OK
        schema = json.load(open(workflow_metadata_template))
        self.assertIsNone(jsonschema.validate(write_pydata(metadata), schema))

    def test_add_task_unsupported(self):
        """
        Test 'add_task' with task type not loaded in the ORM
        """

        spec = WorkflowSpec()
        self.assertRaises(WorkflowError, spec.add_task, 'test', task_type='Unsupported')

    def test_connect_task(self):
        """
        Test connection of tasks
        """

        spec = WorkflowSpec()

        # Add 2 default tasks and connect
        for task in range(3):
            t = spec.add_task('task{0}'.format(task+1))
            if task != 0:
                spec.connect_task(spec.workflow.root, t.nid)

    def test_save_spec(self):
        """
        Save a constructed workflow specification to file and load it again
        """

        spec = WorkflowSpec()

        # Add 5 default tasks
        tasks = {}
        for task in range(5):
            t = spec.add_task('task{0}'.format(task+1))
            tasks[task] = t.nid
            if task > 0:
                spec.connect_task(tasks[task-1], tasks[task])

        # Set some metadata
        metadata = spec.workflow.query_nodes(key='project_metadata')
        metadata.title.value = 'Test project'

        path = os.path.abspath(os.path.join(currpath, '../files/test_workflow_save.jgf'))
        self.tempfiles.append(path)
        spec.save(path=path)

        self.assertTrue(os.path.exists(path))

        # Load saved spec again
        spec.load(path)

        self.assertEqual(spec.workflow.root, 11)
        self.assertEqual(spec.workflow[spec.workflow.root]['task_type'], 'PythonTask')
        self.assertEqual(len(spec.workflow.query_nodes(task_type='PythonTask')), 5)

    def test_load_valid_spec(self):
        """
        Load a predefined and valid workflow specification from JSON file.
        """

        spec = WorkflowSpec()
        spec.load(os.path.abspath(os.path.join(currpath, '../files/test-linear-finished.jgf')))

        self.assertEqual(spec.workflow.root, 11)
        self.assertEqual(spec.workflow[spec.workflow.root]['task_type'], 'BlockingPythonTask')
        self.assertEqual(len(spec), 5)
