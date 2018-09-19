# -*- coding: utf-8 -*-

"""
file: task_wamp_type.py

Task class for calling a method on a (remote) microservice using the WAMP
protocol (Web Agnostic Messaging Protocol)
"""

import os
import re
import json
import logging

from mdstudio.component.session import ComponentSession
from mdstudio.deferred.chainable import chainable
from mdstudio.deferred.return_value import return_value

from lie_graph.graph_math_operations import graph_join
from lie_graph.graph_axis.graph_axis_mixin import NodeAxisTools
from lie_graph.graph_io.io_jsonschema_format import read_json_schema
from lie_workflow.workflow_task_types.task_base_type import TaskBase, load_task_schema
from lie_workflow.workflow_common import is_file

# Preload Task definitions from JSON schema in the package schema/endpoints/
TASK_SCHEMA = 'workflow_wamp_task.v1.json'
TASK = load_task_schema(TASK_SCHEMA)

urisplitter = re.compile("[^\\w']+")
mdstudio_urischema = (u'type', u'group', u'component', u'name', u'version')
wamp_urischema = (u'group', u'component', u'type', u'name')


class FileType(NodeAxisTools):

    def to_dict(self, data):

        if isinstance(data, dict):
            if data.keys() == [u'content', u'path', u'extension', u'encoding']:
                return data

        desc = self.descendants()
        fileobj = dict(desc.items())

        fileobj[u'extension'] = 'smi'
        if is_file(data):
            fileobj[u'path'] = os.path.abspath(data)
            fileobj[u'extension'] = data.split('.')[-1]
            with open(data, 'r') as df:
                fileobj[u'content'] = df.read()
        else:
            fileobj[u'content'] = data

        return fileobj


def schema_uri_to_dict(uri, request=True):
    """
    Parse MDStudio WAMP JSON schema URI to dictionary

    The function accepts both the WAMP standard URI as well as the MDStudio
    resource URI. The latter one defines explicitly if the URI describes a
    'resource' or 'endpoint', uses the full request or response endpoint
    schema name as stored in the database (e.a. <endpoint name>_<request or
    response>) and defines the schema version.

    The WAMP URI style will always default to version 1 of the schema and
    uses the `request` argument to switch between retrieving the 'request' or
    'response' schema for the endpoint

    MDStudio resource URI syntax:
        <resource or endpoint>://<context>/<component>/<endpoint>/v<version ID>'

    WAMP URI syntax:
        <context>.<component>.<endpoint or resource>.<name>

    :param uri:     MDStudio WAMP JSON Schema URI
    :type uri:      :py:str
    :param request: return the request schema for a WAMP style URI else return
                    the response schema
    :type request:  :py:bool

    :return:        parsed JSON schema URI
    :rtype:         :py:dict
    """

    split_uri = re.split(urisplitter, uri)

    # Parse MDStudio resource URI
    if '//' in uri:
        if len(split_uri) != 5:
            raise FormatError('Invalid MDStudio schema uri: {0}'.format(uri))
        uri_dict = dict(zip(mdstudio_urischema[:4], split_uri[:4]))
        uri_dict[u'version'] = int(split_uri[-1].strip(u'v'))

    # Parse WAMP URI
    else:
        if len(split_uri) != 4:
            raise FormatError('Invalid WAMP schema uri: {0}'.format(uri))
        uri_dict = dict(zip(wamp_urischema, split_uri))
        uri_dict[u'name'] = u'{0}_{1}'.format(uri_dict[u'name'], u'request' if request else u'response')
        uri_dict[u'version'] = 1

    return uri_dict


def dict_to_schema_uri(uri_dict):
    """
    Build MDStudio WAMP JSON schema URI from dictionary

    :param uri_dict: dictionary describing WAMP JSON Schema URI
    :type uri_dict:  :py:dict

    :return:         MDStudio WAMP JSON Schema URI
    :rtype:          :py:str
    """

    return u'{type}://{group}/{component}/{name}/v{version}'.format(**uri_dict)


class SchemaParser(object):
    """
    MDStudio WAMP JSON Schema parser

    Obtain the JSON Schema describing a MDStudio micro service endpoint
    or resource registered with the MDStudio router using its URI.
    The MDStudio router exposes the `endpoint`
    """

    def __init__(self, session):
        """
        :param session: MDStudio WAMP session required to make WAMP calls.
        :type session:  :mdstudio:component:session:ComponentSession
        """

        self.session = session
        self.schema_endpoint = u'mdstudio.schema.endpoint.get'

        # Cache schema's to limit calls
        self._schema_cache = {}

    def _get_refs(self, schema, refs=[]):
        """
        Get JSON Schema reference URI's ($ref) from a JSON Schema document.

        :param schema: JSON Schema
        :type schema:  :py:dict

        :return:       list of refered JSON schema's
        :rtype:        :py:list
        """

        for key, value in schema.items():
            if key == u'$ref':
                refs.append(value)
            elif isinstance(value, dict):
                self._get_refs(value, refs=refs)

        return refs

    @chainable
    def _recursive_schema_call(self, uri_dict):
        """
        Recursivly obtain endpoint schema's

        Recursivly calls the MDStudio schema endpoint to retrieve JSON schema
        definitions for the (nested) endpoint and resources based on a URI.
        In document references to other schema's use the JSON Schema '$ref'
        argument accepting a MDStudio schema URI as value.

        :param uri_dict: dictionary from the `schema_uri_to_dict` function
                         describing WAMP JSON Schema URI.
        :type uri_dict:  :py:dict
        """

        uri = dict_to_schema_uri(uri_dict)
        if uri not in self._schema_cache:

            response = {}
            try:
                response = yield self.session.group_context(
                    self.session.component_config.static.vendor).call(
                    self.schema_endpoint, uri_dict,
                    claims={u'vendor': self.session.component_config.static.vendor})
            except:
                logging.error('Unable to call endpoint: {0}'.format(uri))

            self._schema_cache[uri] = response
            for refs in set(self._get_refs(response)):
                yield self._recursive_schema_call(schema_uri_to_dict(refs))

    def _build_schema(self, schema):
        """
        Build full JSON Schema from source and referenced schemas
        """

        for key, value in schema.items():
            if isinstance(value, dict):
                if u'$ref' in value:
                    schema[key].update(self._schema_cache[value[u'$ref']])
                self._build_schema(value)
            elif key == '$ref':
                prop = self._schema_cache[value][u'properties']
                schema[u'properties'] = self._build_schema(prop)

        return schema

    @chainable
    def get(self, uri, clean_cache=True, **kwargs):
        """
        Retrieve the JSON Schema describing an MDStudio endpoint (request or
        response) or resource based on a WAMP or MDStudio schema URI.

        The method returns a Twisted deferred object for which the results
        can be obtained using `yield`.

        :param uri:     MDStudio endpoint or resource JSON Schema URI to
                        retrieve
        :type uri:      :py:str
        :param kwargs:  additional keyword arguments are passed to the
                        `schema_uri_to_dict` function
        :type kwargs:   :py:dict

        :return:        Schema as Twisted deferred object
        """

        # Parse uri elements to dictionary
        uri_dict = schema_uri_to_dict(uri, **kwargs)
        uri = dict_to_schema_uri(uri_dict)

        # Clean the uri cache
        if clean_cache:
            self._schema_cache = {}

        # Recursively call the MDStudio schema endpoint to obtain schema's
        yield self._recursive_schema_call(uri_dict)
        return_value(self._build_schema(self._schema_cache.get(uri, {})))


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

    @chainable
    def get_input(self, **kwargs):
        """
        Prepare input dictionary for the WAMP call

        All data is transferred over the network by default in WAMP
        communication and therefor all file paths are read to string.
        """

        input_dict = super(WampTask, self).get_input()

        # Retrieve JSON schemas for the endpoint request and response
        schemaparser = SchemaParser(kwargs.get('task_runner'))
        request = yield schemaparser.get(uri=self.uri(), request=True)

        # Check input against schema
        request = read_json_schema(request)
        request.orm.map_node(FileType, title='path_file')
        properties = request.query_nodes({'schema_label': u'properties'})

        # Check for parameters not defined in the schema
        # Warn about it and remove.
        undefined_keys = set(input_dict.keys()).difference(set(properties.keys()))
        for undefined in undefined_keys:
            logging.warn('Undefined argument removed: {0}'.format(undefined))
            del input_dict[undefined]
        
        # Register parameters (first level) not defined for provenance
        for param in properties.children():
            if param.key not in input_dict:
                value = param.get(u'value', defaultattr=u'default')
                if value is not None:
                    logging.debug('Set default for parameter: {0}'.format(param.key))
                    input_dict[param.key] = value

        # Check for file types
        for node in request.query_nodes({'title': u'path_file'}).iternodes():
            input_dict[node.key] = node.to_dict(input_dict.get(node.key))

        # Write input to disk as JSON? Task working directory should exist
        if self.task_metadata.store_output():
            input_json = os.path.join(self.task_metadata.workdir.get(), 'input.json')
            json.dump(input_dict, open(input_json, 'w'), indent=2)

        return_value(input_dict)

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

            # Call the service
            input_dict = yield self.get_input(task_runner=task_runner)
            deferred = task_runner.call(wamp_uri, input_dict)

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
