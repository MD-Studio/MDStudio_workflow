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

from tempfile import mktemp
from mdstudio.component.session import ComponentSession
from mdstudio.deferred.chainable import chainable
from mdstudio.deferred.return_value import return_value

from graphit.graph_combinatorial.graph_split_join_operations import graph_join
from graphit.graph_axis.graph_axis_mixin import NodeAxisTools
from graphit.graph_io.io_jsonschema_format import read_json_schema
from graphit.graph_io.io_pydata_format import write_pydata
from lie_workflow.workflow_task_types.task_base_type import TaskBase, load_task_schema
from lie_workflow.workflow_common import is_file

# Preload Task definitions from JSON schema in the package schema/endpoints/
TASK_SCHEMA = 'workflow_wamp_task.v1.json'
TASK = load_task_schema(TASK_SCHEMA)

urisplitter = re.compile("[^\\w']+")
mdstudio_urischema = (u'type', u'group', u'component', u'name', u'version')
wamp_urischema = (u'group', u'component', u'type', u'name')


def to_file_obj(data, inline_files=True):

    new_file_obj = {u'extension': 'smi', u'encoding': 'utf8', u'content': None, u'path': None}

    # Data could already be a path_file object from a previous WAMP task
    if isinstance(data, dict) and u'content' in data:
        new_file_obj.update(data)

    # Data could be a path
    elif is_file(data):
        new_file_obj[u'path'] = os.path.abspath(data)
        new_file_obj[u'extension'] = data.split('.')[-1]

    # Else it is 'content'
    else:
        new_file_obj[u'content'] = data

    # If path and inline content
    if new_file_obj[u'path'] is not None and os.path.exists(new_file_obj[u'path']) and inline_files:
        new_file_obj[u'extension'] = new_file_obj[u'path'].split('.')[-1]
        with open(new_file_obj[u'path'], 'r') as df:
            new_file_obj[u'content'] = df.read()

    return new_file_obj


class FileType(NodeAxisTools):

    def to_dict(self, data, inline_files=True, workdir=None):
        """
        Create a path_file object for communication of files in a WAMP message

        A path_file object contains information on local path where the file
        is stored, the file extension, the encoding and the contents of the
        file.
        Not all of this data is required, availability depends on:

        * If `inline_files` is True (default) the content of the file is
          send inline to the WAMP endpoint. If `inline_files` is False, the
          `path` parameter needs to be set to a local file system path that is
          also accessible by the endpoint the WAMP message is send to.

        :param data:         file related information to construct path_file
                             object from. Either an existing path_file object,
                             a file path or the content of the file.
        :type data:          :py:dict or :py:str
        :param inline_files: include content of file inline to the WAMP message
        :type inline_files:  :py:bool
        :param workdir:      local working directory to store files to
        :type workdir:       :py:str

        :return:             path_file obejct
        :rtype:              :py:dict
        """

        new_file_obj = to_file_obj(data, inline_files=inline_files)

        # No inline file content, ensure valid file path
        if not inline_files and new_file_obj[u'content'] and workdir and new_file_obj[u'extension'] != 'smi':

            if new_file_obj[u'path']:

                # If local path does not exists, store content in workdir else use path
                if not os.path.exists(new_file_obj[u'path']):
                    newfile = os.path.join(workdir, os.path.basename(new_file_obj[u'path']))
                    with open(newfile, 'w') as outfile:
                        outfile.write(new_file_obj[u'content'])
                    new_file_obj[u'path'] = newfile
                    new_file_obj[u'content'] = None

            # No path, make one using a unique filename.
            else:
                tmpfilename = os.path.basename(mktemp())
                newfile = os.path.join(workdir, '{0}.{1}'.format(tmpfilename, new_file_obj[u'extension'] or 'txt'))
                with open(newfile, 'w') as outfile:
                    outfile.write(new_file_obj[u'content'])
                new_file_obj[u'path'] = newfile
                new_file_obj[u'content'] = None

        elif not inline_files and new_file_obj[u'content'] and not workdir and new_file_obj[u'extension'] != 'smi':
            raise IOError('Unable to store output in local task directory. Set store_output to True for this task.')

        return new_file_obj


class FileArrayType(NodeAxisTools):

    def to_dict(self, data, inline_files=True, workdir=None):
        """
        Create a path_file object for communication of files in a WAMP message

        A path_file object contains information on local path where the file
        is stored, the file extension, the encoding and the contents of the
        file.
        Not all of this data is required, availability depends on:

        * If `inline_files` is True (default) the content of the file is
          send inline to the WAMP endpoint. If `inline_files` is False, the
          `path` parameter needs to be set to a local file system path that is
          also accessible by the endpoint the WAMP message is send to.

        :param data:         file related information to construct path_file
                             object from. Either an existing path_file object,
                             a file path or the content of the file.
        :type data:          :py:dict or :py:str
        :param inline_files: include content of file inline to the WAMP message
        :type inline_files:  :py:bool
        :param workdir:      local working directory to store files to
        :type workdir:       :py:str

        :return:             path_file obejct
        :rtype:              :py:dict
        """

        # Course input to list
        if not isinstance(data, (list, tuple)):
            data = [data]

        file_array = []
        for item in data:

            new_file_obj = to_file_obj(item, inline_files=inline_files)

            # No inline file content, ensure valid file path
            if not inline_files and new_file_obj[u'content'] and workdir and new_file_obj[u'extension'] != 'smi':

                if new_file_obj[u'path']:

                    # If local path does not exists, store content in workdir else use path
                    if not os.path.exists(new_file_obj[u'path']):
                        newfile = os.path.join(workdir, os.path.basename(new_file_obj[u'path']))
                        with open(newfile, 'w') as outfile:
                            outfile.write(new_file_obj[u'content'])
                        new_file_obj[u'path'] = newfile
                        new_file_obj[u'content'] = None

                # No path, make one using a unique filename.
                else:
                    tmpfilename = os.path.basename(mktemp())
                    newfile = os.path.join(workdir,'{0}.{1}'.format(tmpfilename, new_file_obj[u'extension'] or 'txt'))
                    with open(newfile, 'w') as outfile:
                        outfile.write(new_file_obj[u'content'])
                    new_file_obj[u'path'] = newfile
                    new_file_obj[u'content'] = None

            elif not inline_files and new_file_obj[u'content'] and not workdir and new_file_obj[u'extension'] != 'smi':
                raise IOError('Unable to store output in local task directory. Set store_output to True for this task.')

            file_array.append(new_file_obj)

        return file_array


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
            graph_join(self.origin, TASK.descendants(),
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
        request = read_json_schema(request)
        request.orm.map_node(FileType, format='file')
        request.orm.map_node(FileArrayType, format='file_array')

        # Register parameters wherever they are defined
        for key, value in input_dict.items():
            node = request.query_nodes({request.key_tag: key})
            if len(node) == 1:
                node.set(request.value_tag, value)
            elif node.empty():
                logging.warn('Task task {0} ({1}): parameter {2} not defined in endpoint schema'.format(self.nid,
                                                                                                        self.key, key))
            elif len(node) > 1:
                logging.warn('Task task {0} ({1}): parameter {2} defined multiple times in schema'.format(self.nid,
                                                                                                        self.key, key))

        # Check for file types, remove schema parameters not defined
        for node in request.query_nodes({'format': u'file'}).iternodes():
            if node.get(node.key_tag) not in input_dict:
                request.remove_node(node.nid)
            else:
                fileobj = node.to_dict(input_dict.get(node.key),
                                       inline_files=self.inline_files(),
                                       workdir=self.task_metadata.workdir.get())

                for key, value in fileobj.items():
                    obj = node.descendants().query_nodes({node.key_tag: key})
                    obj.set(node.value_tag, value)

                # Reset original 'value' with file path
                node.set(node.value_tag, None)

        # Check for file arrays, remove schema parameters not defined
        for node in request.query_nodes({'format': u'file_array'}).iternodes():
            if node.get(node.key_tag) not in input_dict:
                request.remove_node(node.nid)
            else:
                fileobj = node.to_dict(input_dict.get(node.key),
                                       inline_files=self.inline_files(),
                                       workdir=self.task_metadata.workdir.get())

                # Reset original 'value' with file path
                node.set(node.value_tag, fileobj)

        # Check for parameters that have defaults, remove others
        nodes_to_remove = []
        for node in request.query_nodes({u'schema_label': u'properties'}):
            if node.get(u'value') is None:
                node_type = node.get(u'type', [])
                if not isinstance(node_type, list):
                    node_type = [node_type]
                if u'object' in node_type:
                    continue
                if node.get(u'default') is None and u'null' not in node_type:
                    nodes_to_remove.append(node.nid)
        request.remove_nodes(nodes_to_remove)

        # Build parameter dictionary from JSON Schema
        param_dict = write_pydata(request)

        # Remove all 'value' parameters with value None.
        # TODO: These are left overs from lie_graph.
        def recursive_remove_none(d):

            for k, v in d.items():
                if k == 'value' and v is None:
                    del d[k]
                elif isinstance(v, dict):
                    d[k] = recursive_remove_none(v)

            return d

        param_dict = recursive_remove_none(param_dict)

        # Write input to disk as JSON? Task working directory should exist
        if self.task_metadata.store_output():
            input_json = os.path.join(self.task_metadata.workdir.get(), 'input.json')
            json.dump(param_dict, open(input_json, 'w'), indent=2)

        return_value(param_dict)

    @chainable
    def run_task(self, callback, **kwargs):
        """
        Implements run_task method

        Runs a WAMP method by calling the endpoint URI with a dictionary as input.

        :param callback:    WorkflowRunner callback method called from Twisted
                            deferred when task is done.
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
            deferred.addCallback(callback, self.nid)

        else:
            logging.error('task_runner not of type mdstudio.component.session.ComponentSession')
            callback(None, self.nid)

    @chainable
    def check_task(self, callback, **kwargs):
        """
        Implement check_task method for asynchronous tasks

        :param callback:    WorkflowRunner callback method called from Twisted
                            deferred when task is done.
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
            wamp_uri = self.task_metadata.query_url()
            if group_context and not wamp_uri.startswith(group_context):
                wamp_uri = '{0}.{1}'.format(group_context, wamp_uri)

            # Call the service
            deferred = task_runner.call(wamp_uri, task_id=self.task_metadata.external_task_id(), status=self.status,
                                        query_url=wamp_uri, checks=self.task_metadata.checks())
            deferred.addCallback(callback, self.nid)

        else:
            logging.error('task_runner not of type mdstudio.component.session.ComponentSession')
            callback(None, self.nid)
