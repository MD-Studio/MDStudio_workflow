# -*- coding: utf-8 -*-

import os
import re
import itertools
import shutil
import stat

from collections import Counter

from graphit.graph_io.io_pydata_format import read_pydata, write_pydata
from graphit.graph_py2to3 import PY_STRING

from mdstudio_workflow import __twisted_logger__

if __twisted_logger__:
    from twisted.logger import Logger
    logging = Logger()
else:
    import logging


class WorkflowError(Exception):

    def __init__(self, message):

        super(WorkflowError, self).__init__(message)

        logging.error(message)


def copytree(src, dst, symlinks=False, ignore=None):
    if not os.path.exists(dst):
        os.makedirs(dst)
        shutil.copystat(src, dst)
    lst = os.listdir(src)
    if ignore:
        excl = ignore(src, lst)
        lst = [x for x in lst if x not in excl]
    for item in lst:
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if symlinks and os.path.islink(s):
            if os.path.lexists(d):
                os.remove(d)
            os.symlink(os.readlink(s), d)
            try:
                st = os.lstat(s)
                mode = stat.S_IMODE(st.st_mode)
                os.lchmod(d, mode)
            except Exception:
                pass  # lchmod not available
        elif os.path.isdir(s):
            copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def concat_dict(dict_list):
    """
    Concatenate list of dictionaries to one new dictionary

    Duplicated keys will have their values combined as list.

    :param dict_list:   list of dictionaries to concatenate into a new dict
    :type dict_list:    :py:list

    :return:            Concatenated dictionary
    :rtype:             :py:dict
    """
    keys = list(itertools.chain.from_iterable([d.keys() for d in dict_list]))
    key_count = Counter(keys)
    concatenated = dict([(k, None) if c == 1 else (k, []) for k, c in key_count.items()])

    for d in dict_list:
        for k, v in d.items():
            if key_count[k] > 1:
                concatenated[k].append(v)
            else:
                concatenated[k] = v

    return concatenated


def validate_workflow(workflow):
    """
    Validate the constructed workflow

    :param workflow: Workflow graph to validate
    :type workflow:  :graphit:GraphAxis

    :return: Validated or not
    :rtype:  :py:bool
    """

    validated = True

    # Required in case the workflow has only one task
    # TODO: fix this, behaviour due to GraphAxis iterator that iterates
    # children if only one top level element.
    tasks = workflow.query_nodes(format='task')
    if len(tasks) == 1:
        tasks = [tasks]

    # All Task objects should have a 'run_task' method
    for task in tasks:
        if not hasattr(task, 'run_task'):
            logging.warning('Task {0} ({1}) has no "run_task" method'.format(task.nid, task.key))
            validated = False

        if not task.validate():
            validated = False

    return validated


def is_file(param):

    if not isinstance(param, PY_STRING):
        return False

    if param.count('\n') > 0:
        return False

    if os.path.isdir(param):
        return False

    if re.match('^(.+)/([^/]+)$', param) and '(' not in param:
        return True

    return False


def process_duplicate_filenames(path):

    if os.path.exists(path):
        count = 1
        base, ext = os.path.splitext(path)
        while os.path.exists(path):
            path = '{0}_{1}{2}'.format(base, count, ext)
            count += 1

    return path


def collect_data(output, task_dir):

    graph = read_pydata(output, level=1)
    for node in graph.nodes.values():

        for key, value in node.items():

            if isinstance(value, PY_STRING) or is_file(value):

                # Serialized file
                if value.count('\n') > 0:
                    output_file = os.path.join(task_dir,
                                               'file_{0}.{1}'.format(node.get('key', key),
                                                                     node.get('extension', 'out').lstrip('.')))
                    with open(output_file, 'w') as outf:
                        outf.write(value)

                    node['path'] = output_file
                    logging.info('Store output to file: {0}'.format(output_file))

                elif os.path.isdir(value):

                    # Copy the full directory
                    dirname = os.path.basename(value)
                    destination = os.path.join(task_dir, dirname)
                    copytree(value, destination)
                    node[key] = destination

                    logging.info('Collect directory: {0} to {1}'.format(value, destination))
                elif os.path.isfile(value):

                    # Copy the file
                    filename = os.path.basename(value)
                    destination = process_duplicate_filenames(os.path.join(task_dir, filename))
                    shutil.copy(value, destination)
                    node[key] = destination

                    logging.info('Collect file: {0} to {1}'.format(value, destination))
                else:
                    logging.debug('Value might be a file but the path does not exist: {0}'.format(value))

    return write_pydata(graph, export_all=True)
