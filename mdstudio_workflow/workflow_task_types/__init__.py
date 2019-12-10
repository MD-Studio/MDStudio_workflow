# -*- coding: utf-8 -*-

"""
package: workflow_task_types

Collection of classes defining workflow task types
"""

from graphit.graph_orm import GraphORM
from graphit.graph_model_classes.model_user import User
from graphit.graph_model_classes.model_datetime import DateTime
from graphit.graph_model_classes.model_identifiers import UUID
from graphit.graph_model_classes.model_files import FilePath
from graphit.graph_io.io_jsonschema_format_drafts import StringType

from .task_python_type import PythonTask, BlockingPythonTask, LoadCustomFunc
#from .task_wamp_type import WampTask
from .task_loop_type import LoopTask

task_types = ('PythonTask', 'BlockingPythonTask', 'WampTask', 'LoopTask')

# Define the workflow Task ORM
WORKFLOW_ORM = GraphORM(inherit=False)
WORKFLOW_ORM.node_mapping.add(PythonTask, lambda x: x.get('task_type') == 'PythonTask')
WORKFLOW_ORM.node_mapping.add(BlockingPythonTask, lambda x: x.get('task_type') == 'BlockingPythonTask')
#WORKFLOW_ORM.node_mapping.add(WampTask, lambda x: x.get('task_type') == 'WampTask')
WORKFLOW_ORM.node_mapping.add(LoopTask, lambda x: x.get('task_type') == 'LoopTask')
WORKFLOW_ORM.node_mapping.add(User, lambda x: x.get('key') == 'user')
WORKFLOW_ORM.node_mapping.add(DateTime, lambda x: x.get('format') == 'date-time')
WORKFLOW_ORM.node_mapping.add(UUID, lambda x: x.get('format') == 'uuid')
WORKFLOW_ORM.node_mapping.add(LoadCustomFunc, lambda x: x.get('key') == 'custom_func')
WORKFLOW_ORM.node_mapping.add(StringType, lambda x: x.get('key') == 'custom_func')
WORKFLOW_ORM.node_mapping.add(StringType, lambda x: x.get('key') == 'status')
WORKFLOW_ORM.node_mapping.add(StringType, lambda x: x.get('key') == 'uri')
WORKFLOW_ORM.node_mapping.add(FilePath, lambda x: x.get('key') == 'project_dir')
WORKFLOW_ORM.node_mapping.add(FilePath, lambda x: x.get('key') == 'workdir')
