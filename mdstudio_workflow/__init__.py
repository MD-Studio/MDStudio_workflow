# -*- coding: utf-8 -*-

import os

__module__ = 'mdstudio_workflow'
__docformat__ = 'restructuredtext'
__version__ = '{major:d}.{minor:d}'.format(major=1, minor=0)
__author__ = 'Marc van Dijk'
__status__ = 'pre-release beta1'
__date__ = '15 april 2016'
__licence__ = 'Apache Software License 2.0'
__url__ = 'https://github.com/MD-Studio/MDStudio_structures'
__copyright__ = "Copyright (c) VU University, Amsterdam"
__rootpath__ = os.path.dirname(__file__)
__all__ = ['Workflow', '__version__']

from .workflow_runner import WorkflowRunner as Workflow
from .workflow_spec import WorkflowSpec
