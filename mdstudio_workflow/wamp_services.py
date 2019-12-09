# -*- coding: utf-8 -*-

"""
file: wamp_services.py

WAMP service methods the module exposes.
"""

from autobahn.wamp import RegisterOptions
from mdstudio.api.endpoint import endpoint
from mdstudio.component.session import ComponentSession


class WorkflowWampApi(ComponentSession):
    """
    Workflow WAMP methods.

    Defines `require_config` to retrieve system and database configuration
    upon WAMP session setup
    """

    def authorize_request(self, uri, claims):

        return True

    @endpoint('run_workflow', 'run_workflow_request', 'run_workflow_response',
              options=RegisterOptions(invoke=u'roundrobin'))
    def retrieve_structures(self, workflow, session=None):
        """
        Run a LIE Workflow
        """

        print(workflow)
        print(session)

        return session
