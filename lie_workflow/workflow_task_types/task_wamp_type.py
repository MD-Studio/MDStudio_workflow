# -*- coding: utf-8 -*-

"""
file: task_wamp_type.py

Task class for calling a method on a (remote) microservice using the WAMP
protocol (Web Agnostic Messaging Protocol)
"""

import os
import logging

from mdstudio.component.session import ComponentSession
from lie_graph.graph_math_operations import graph_join

from lie_workflow.workflow_task_types.task_base_type import TaskBase, load_task_schema
from lie_workflow.workflow_common import is_file

# Preload Task definitions from JSON schema in the package schema/endpoints/
TASK_SCHEMA = 'workflow_wamp_task.v1.json'
TASK = load_task_schema(TASK_SCHEMA)


class WampTask(TaskBase):

    def new(self, **kwargs):
        """
        Implements 'new' abstract base class method to create new
        task node tree.

        Load task from JSON Schema workflow_wamp_task.v1.json in package
        /schemas/endpoints folder.
        """

        # Do not initiate twice in case method gets called more then once.
        if not len(self.children()):

            logging.info('Init task {0} ({1}) from schema: {2}'.format(self.nid, self.key, TASK_SCHEMA))
            graph_join(self._full_graph, TASK.descendants(),
                       links=[(self.nid, i) for i in TASK.children(return_nids=True)])

            # Set unique task uuid
            self.task_metadata.task_id.set('value', self.task_metadata.task_id.create())

    def get_input(self):
        """
        Prepare input dictionary for the WAMP call

        All data is transferred over the network by default in WAMP
        communication and therefor all file paths are read to string.
        """

        input_dict = super(WampTask, self).get_input()

        # If local files are defined as input, read content.
        # TODO: replace with method that uses metadata to assess what is a file
        for key, value in input_dict.items():

            # If value already create_path_file_obj
            if not isinstance(value, PY_STRING):
                continue

            # In case of file as string
            elif len(value.split('\n')) > 1:
                input_dict[key] = create_path_file_obj(content=value)

            # In case of a file
            elif is_file(value) and os.path.isfile(value) and not os.access(value, os.X_OK):
                input_dict[key] = create_path_file_obj(path=value)

        return input_dict

    def validate_input(self, schema):
        """
        Validate defined input against MDStudio JSON Schema.

        Type checking is performed by the MDStudio router
        """

        input = self.task_metadata.input_data.get(default={})
        request = read_json_schema(schema)
        request.orm.map_node(FileType, title='path_file')
        properties = request.query_nodes({'schema_label': u'properties'})

        # Check for parameters not defined in the schema
        undefined_keys = set(input.keys()).difference(set(properties.keys()))
        if undefined_keys:
            raise AttributeError('Undefined arguments: {0}'.format(undefined_keys))

        # Register parameters not defined for provenance
        for param in properties:
            if param.key not in input and u'value' in param.nodes:
                input[param.key] = param.value

        # Check for file types
        for node in request.query_nodes({'title': u'path_file'}).iternodes():
            input[node.key] = node.to_dict(input.get(node.key))

        self.set_input(**input)

    @chainable
    def run_task(self, callback, errorback, **kwargs):
        """
        Implements run_task method

        Runs a WAMP method by calling the endpoint URI with a dictionary as input.

        :param callback:    WorkflowRunner callback method called from Twisted
                            deferred when task is done.
        :param errorback:   WorkflowRunner errorback method called from Twisted
                            deferred when task failed.
        :param kwargs:      Additional keyword arguments should contain the main
                            mdstudio.component.session.ComponentSession instance as
                            'task_runner'
        """

        task_runner = kwargs.get('task_runner')

        # Task_runner should be defined and of type
        # mdstudio.component.session.ComponentSession
        if isinstance(task_runner, ComponentSession):

            # Check if there is a group_context defined and if the WAMP uri
            # starts with the group_context.
            group_context = self.group_context()
            wamp_uri = self.uri()
            if group_context and not wamp_uri.startswith(group_context):
                wamp_uri = '{0}.{1}'.format(group_context, wamp_uri)

            # Try to get the JSON schemas for the endpoint request and response
            schemaparser = SchemaParser(task_runner)

            # Check input against schema
            request = yield schemaparser.get(uri=wamp_uri, request=True)
            self.validate_input(request)

            # Call the service
            deferred = task_runner.call(wamp_uri, self.get_input())

            # Attach error callback
            deferred.addErrback(errorback, self.nid)

            # Attach callback
            deferred.addCallback(callback, self.nid)

        else:
            errorback('task_runner not of type mdstudio.component.session.ComponentSession', self.nid)

    def cancel(self):
        """
        Cancel the task
        """

        if not self.task_metadata.active:
            logging.info('Unable to cancel task {0} ({1}) not active'.format(self.nid, self.key))
            return

        self.task_metadata.status.value = 'aborted'
        self.task_metadata.active.value = False
        logging.info('Canceled task {0} ({1})'.format(self.nid, self.key))
