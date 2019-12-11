# -*- coding: utf-8 -*-

import os
import threading
import random

from mdstudio_workflow import __twisted_logger__
from mdstudio_workflow.workflow_common import WorkflowError, validate_workflow
from mdstudio_workflow.workflow_spec import WorkflowSpec

if __twisted_logger__:
    from twisted.logger import Logger
    logging = Logger()
else:
    import logging


class WorkflowRunner(WorkflowSpec):
    """
    This is the main class for running microservice oriented workflows.

    Running a workflow is based on a workflow specification build using the
    `WorkflowSpec` class. Such a workflow can be loaded into the `Workflow`
    class simply by overloading the Workflow.workflow attribute.
    The `Workflow` class also inherits the methods from the `WorkflowSpec`
    class allowing to build specification right from the `Workflow` class
    and even change a running workflow.

    The execution of workflow steps is performed on a different thread than
    the main Workflow object allowing the user to interact with the running
    workflow.
    The DAG including the metadata dat is generated while executing the steps
    can be serialized to JSON for persistent storage. The same JSON object is
    used as input to the Workflow class validated by a Workflow JSON schema.

    :param workflow:   the workflow DAG to run
    :type workflow:    JSON object
    """

    def __init__(self, workflow=None, **kwargs):

        # Init inherit classes such as the WorkflowSpec
        super(WorkflowRunner, self).__init__(workflow=workflow, **kwargs)

        # Define task runner
        self.task_runner = None
        self.workflow_thread = None

        # Workflow state
        self.project_metadata = None
        self._is_running = False

    def process_check_run(self, task, output):
        """
        Process Future object and register an new task status check for the future.

        :param task:    task to check
        :type task:     :graphit:Graph
        :param output:  Future object
        :type output:   :py:dict
        """

        if not task.task_metadata.external_task_id():
            task.task_metadata.external_task_id.set(task.data.value_tag, output.get('task_id'))

        if 'query_url' in output:
            task.query_url.set(task.data.value_tag, output['query_url'])

        delta_t = output.get('delta_t', 600)
        timer = threading.Timer(delta_t, task.check_task, (self.output_callback,), {'task_runner':self.task_runner})
        timer.deamon = True
        timer.start()

        logging.info('Task {0} ({1}): {2} check {3} next after {4} sec.'.format(task.status, task.nid, task.key,
                                                                            task.task_metadata.checks(), delta_t))

    def output_callback(self, output, tid, update=False):
        """
        Process the output of a task and stage the next task(s) to run.

        A successful task is expected to return some output. If None it
        is considered to have failed by the workflow manager.

        :param output: output of the task
        :type output:  :py:dict
        :param tid:    task ID
        :type tid:     :py:int
        """

        # Get and update the task
        task = self.get_task(tid)
        status, output = task.update(output)

        # Update project metadata
        if update:
            self.project_metadata.update_time.set()

        # Save results as part of workflow to file
        if self.project_metadata.project_dir():
            self.save(os.path.join(self.project_metadata.project_dir(), 'workflow.jgf'))

        # Process Future object
        if output.get('object_type') == 'FutureObject' and status == 'running':
            self.process_check_run(task, output)
            return

        # Switch workdir if needed
        if self.project_metadata.project_dir.get():
            os.chdir(self.project_metadata.project_dir.get())

        # If the task is completed, go to next
        next_task_nids = []
        if status == 'completed':

            # Get next task(s) to run
            next_tasks = task.next_tasks()
            next_task_nids.extend([ntask.nid for ntask in next_tasks])
            logging.info('{0} new tasks to run with output of {1} ({2}): {3}'.format(len(next_task_nids), task.nid,
                                                                    task.key, ','.join([nt.key for nt in next_tasks])))

        # If the task failed, retry if allowed and reset status to "ready"
        if status == 'failed' and task.task_metadata.retry_count():
            task.task_metadata.retry_count.value -= 1
            task.status = 'ready'

            logging.warn('Task {0} ({1}) failed. Retry ({2} times left)'.format(task.nid, task.key,
                                                                                task.task_metadata.retry_count()))
            next_task_nids.append(task.nid)

        # If the active failed an no retry is allowed, save workflow and stop.
        if task.status == 'failed' and task.task_metadata.retry_count() == 0:
            logging.error('Task {0} ({1}) failed'.format(task.nid, task.key))
            self.is_running = False
            return

        # If the task is completed but a breakpoint is defined, wait for the
        # breakpoint to be lifted
        if task.task_metadata.breakpoint():
            logging.info('Task {0} ({1}) finished but breakpoint is active'.format(task.nid, task.key))
            self.is_running = False
            return

        # No more new tasks
        if not next_task_nids:

            # Not finished but no active tasks anymore/breakpoint
            if not self.active_tasks and not self.is_completed:
                breakpoints = self.active_breakpoints
                if breakpoints:
                    logging.info('Active breakpoint: {0}'.format(', '.join([t.key for t in breakpoints])))
                self.is_running = False
                return

            # Finish of if there are no more tasks to run and all are completed
            if self.is_completed or self.has_failed:
                logging.info('finished workflow')
                if not self.project_metadata.finish_time():
                    self.project_metadata.finish_time.set()
                self.is_running = False
                return

        # Launch new tasks
        for tid in next_task_nids:
            self.run_task(tid)

    def run_task(self, tid):
        """
        Run a task by task ID (tid)

        Handles the setup procedure for running a task using a dedicated Task
        runner. The output or errors of a task are handled by the
        `output_callback` method.

        Tasks to run are processed using the following rules:

        * If the task is currently active, stop and have the output callback
          function deal with it.
        * If the task has status 'ready' run it.
        * In all other cases, pass the task data to the output callback
          function. This is useful for hopping over finished tasks when
          relaunching a workflow for instance.

        :param tid: Task node identifier
        :type tid:  :py:int
        """

        task = self.get_task(tid)

        # Do not continue if the task is active
        if task.is_active:
            logging.debug('Task {0} ({1}) already active'.format(task.nid, task.key))
            return

        # Only continue if all connected tasks are done
        unfinished_prev_tasks = [t for t in task.previous_tasks() if t.status != 'completed']
        if unfinished_prev_tasks:

            logging.info('Task {0} ({1}): output of tasks {2} not available'.format(task.nid, task.key,
                                                                ', '.join([str(t.nid) for t in unfinished_prev_tasks])))

            # In cases if previous failed tasks, try unset is_running
            for unfinished in unfinished_prev_tasks:
                if unfinished.status == 'failed':
                    self.is_running = False
                    break

            return

        # Run the task if status is 'ready'
        if task.status == 'ready':
            logging.info('Task {0} ({1}), status: preparing'.format(task.nid, task.key))

            # Confirm again that the workflow is running
            self.is_running = True
            self.project_metadata.update_time.set()

            # Perform run preparations and run the task
            if task.prepare_run():
                task.status = 'running'

                if task.task_type == 'WampTask':
                    wait = random.randint(5,15)
                    logging.info('Task {0} ({1}): start task in {2} sec.'.format(task.nid, task.key, wait))
                    threading.Timer(wait, task.run_task, (self.output_callback,),
                                    {'task_runner': self.task_runner}).start()
                else:
                    task.run_task(self.output_callback, task_runner=self.task_runner)
            else:
                logging.error('Task preparation failed')
                self.output_callback(None, task.nid)

        # In all other cases, pass None and have the task output update
        # method decide what to do next.
        else:
            logging.info('Task {0} ({1}), status: {0}'.format(task.nid, task.key, task.status))
            self.output_callback(None, tid, update=False)

    @property
    def is_running(self):
        """
        Returns the global state of the workflow as running or not.

        :rtype: :py:bool
        """

        return self._is_running

    @is_running.setter
    def is_running(self, state):
        """
        Set the global state of the workflow as running or not.
        If the new state is 'False' first check if there are no other parallel
        active tasks.

        :rtype: :py:bool
        """

        if not state:
            state = len(self.active_tasks) >= 1
        self._is_running = state

    @property
    def is_completed(self):
        """
        Is the workflow completed successfully or not

        :rtype: :py:bool
        """

        return all([task.status in ('completed', 'disabled') for task in self.get_tasks()])

    @property
    def has_failed(self):
        """
        Did the workflow finish unsuccessfully?
        True if there are no more active tasks and at least one task has failed
        or was aborted

        :rtype: :py:bool
        """

        if not len(self.active_tasks) and any([task.status in ('failed', 'aborted') for task in self.get_tasks()]):
            return True

        return False

    @property
    def starttime(self):
        """
        Return the time stamp at which the workflow was last started

        :rtype: :py:int
        """

        return self.project_metadata.start_time.timestamp()

    @property
    def updatetime(self):
        """
        Return the time stamp at which the workflow was last updated

        :rtype: :py:int
        """

        return self.project_metadata.update_time.timestamp()

    @property
    def finishtime(self):
        """
        Return the time stamp at which the workflow finished or None
        if it has not yet finished

        :rtype: :py:int
        """

        if not self.is_running:
            return self.project_metadata.finish_time.timestamp()
        return None

    @property
    def runtime(self):
        """
        Return the total workflow runtime in seconds as the different between
        the start time and the finish time or last update time

        :rtype: :py:int
        """

        start = self.starttime or 0
        end = self.finishtime or self.updatetime

        # No update and finish time means the workflow was not started yet
        if not end:
            return 0

        return end - start

    @property
    def active_tasks(self):
        """
        Return all active tasks in the workflow

        :rtype: :py:list
        """

        return [task for task in self.get_tasks() if task.is_active]

    @property
    def failed_tasks(self):
        """
        Return failed tasks in the workflow

        :rtype: :py:list
        """

        return [task for task in self.get_tasks() if task.status == 'failed']

    @property
    def active_breakpoints(self):
        """
        Return tasks with active breakpoint in the workflow

        :rtype: :py:list
        """

        return [task for task in self.get_tasks() if task.task_metadata.breakpoint.get(default=False)]

    def cancel(self):
        """
        Cancel the full workflow.

        This method will send a cancel request to all active tasks in the
        running workflow. Once there are no more active tasks the workflow
        run method will stop and the deamon thread will be closed.

        For canceling specific tasks please use the `cancel` function of the
        specific task retrieved using the `WorkflowSpec.get_task` method or
        workflow graph methods.
        """

        if not self.is_running:
            logging.info('Unable to cancel workflow that is not running.')
            return

        # Get active task
        active_tasks = self.active_tasks
        logging.info('Cancel tasks: {0}'.format(', '.join([t.key for t in active_tasks])))

        for task in active_tasks:
            task.cancel()

        self.project_metadata.update_time.set()
        self.is_running = False

    def get_task(self, tid=None, key=None):
        """
        Return a task by task ID (graph nid) or task name (key).

        :param tid:       nid of task to return
        :type tid:        :py:int
        :param key:       task name
        :type key:        :py:str

        :return:          task object
        :rtype:           :graphit:Graph
        """

        if tid:
            task = self.workflow.getnodes(tid)
        elif key:
            task = self.workflow.query_nodes(key=key)
        else:
            raise WorkflowError('Search on task ID (tid) or task name (key). None defined')

        if task.empty():
            raise WorkflowError('Task with tid {0} not in workflow'.format(tid))
        if not task.get('format') == 'task':
            raise WorkflowError('Node with tid {0} is no task object'.format(tid))

        return task

    def step_breakpoint(self, tid):
        """
        Continue a workflow at a task that is paused by a breakpoint

        :param tid: workflow task ID with active breakpoint
        :type tid:  :py:int
        """

        task = self.get_task(tid)
        if not task.task_metadata.breakpoint.get(default=False):
            logging.warn('No active breakpoint set on task {0}'.format(task.key))
            return

        # Remove the breakpoint
        task.task_metadata.breakpoint.set(task.data.value_tag, False)
        logging.info('Remove breakpoint on task {0} ({1})'.format(tid, task.key))

    def input(self, tid, **kwargs):
        """
        Define task input and configuration data

        :param tid:    task ID to define input for
        :type tid:     :py:int
        :param kwargs: keyword arguments to register as input
        """

        task = self.get_task(tid)
        task.set_input(**kwargs)

    def output(self, tid=None):
        """
        Get workflow output
        Returns the output associated to all terminal tasks (leaf nodes) of
        the workflow or of any intermediate tasks identified by the task ID

        :param tid: task ID to return output for
        :type tid:  :py:int

        :rtype:     :py:dict
        """

        task = self.get_task(tid)

        output = {}
        if task.status == 'completed':
            output = task.get_output()

        return output

    def run(self, project_dir="./md_workflow", tid=None, validate=True):
        """
        Run a workflow specification

        Runs the workflow until finished, failed or a breakpoint is reached.
        A workflow is a rooted Directed Acyclic Graph (DAG) that is started
        from the root node. It can be started from any node relative to the
        root as long as its parent(s) are successfully completed.

        The workflow will be executed on a different thread allowing for
        interactivity with the workflow instance while the workflow is
        running.

        By default, the workflow specification will be validated using the
        `validate` method of the WorkflowSpec class.

        :param tid:         start the workflow from task ID
        :type tid:          :py:int
        :param validate:    Validate the workflow before running it
        :type validate:     :py:bool
        :param project_dir: directory to store task output
        :type project_dir:  :py:str
        """

        # Empty workflow, return
        if self.workflow.empty() or not len(self.workflow.query_nodes(format='task')):
            logging.info('Workflow contains no tasks')
            return

        # Start from workflow root by default
        tid = tid or self.workflow.root

        # Check if tid exists
        if tid not in self.workflow.nodes:
            raise WorkflowError('Task with tid {0} not in workflow'.format(tid))

        # Validate workflow before running?
        if validate:
            if not validate_workflow(self.workflow):
                raise WorkflowError('Workflow validation failed')

        # If there are steps that store results locally (store_output == True)
        # Create a project directory.
        self.project_metadata = self.workflow.query_nodes(key='project_metadata')
        if any(self.workflow.query_nodes(key="store_output").values()):
            self.project_metadata.project_dir.set(self.workflow.data.value_tag,
                                                  self.project_metadata.project_dir.get(default=project_dir))
            if self.project_metadata.project_dir.exists and self.is_completed:
                raise WorkflowError('Directory for finished project exists: {0}'.format(
                    self.project_metadata.project_dir()))
            self.project_metadata.project_dir.makedirs()
        else:
            self.project_metadata.project_dir.set(self.workflow.data.value_tag, None)

        logging.info('Running workflow: {0}, start task ID: {1}'.format(self.project_metadata.title(), tid))

        # Set is_running flag. Function as a thread-safe signal to indicate
        # that the workflow is running.
        if self.is_running:
            logging.warning('Workflow {0} is already running'.format(self.project_metadata.title()))
            return
        self.is_running = True

        # Set workflow start time if not defined. Don't rerun to allow
        # continuation of unfinished workflow.
        if not self.project_metadata.start_time():
            self.project_metadata.start_time.set()

        # Spawn a workflow thread
        self.workflow_thread = threading.Thread(target=self.run_task, args=[tid])
        self.workflow_thread.daemon = True
        self.workflow_thread.start()
