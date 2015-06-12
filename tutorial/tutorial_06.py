#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import random

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../src' not in sys.path:
    sys.path.append(current_dirpath +'../src')

from job import Job
from job import JobNode
from job import JobBlock
from job import ParallelJobBlock

from hadoop import Delegatee
from hadoop import DFS
from hadoop import Pig
from hadoop import Shell

from log    import Logger

'''
you could access the delegatees by their registered id
the delegatees will return the standard output/error message
or False if the command failed
'''
def delegated_job(self):
    dfs = self.get_delegatee('my_dfs')
    result = dfs.ls('/some/path')
    self.log(Logger.INFO, "the results: \n%s" % (result))

    shell = self.get_delegatee('my_shell')
    result = shell.run('ls')
    self.log(Logger.INFO, "the results: \n%s" % (result))

    result = dfs.lswc('/some/file')
    self.log(Logger.INFO, "the results: \n%s" % (result))

    return Job.DONE

def failed_delegated_job(self):
    dfs = self.get_delegatee('my_dfs')
    result = dfs.ls('/path/not/existed')
    self.log(Logger.INFO, "the results: \n%s" % (result))

    shell = self.get_delegatee('my_shell')
    result = shell.run('you can put any command here')
    self.log(Logger.INFO, "the results: \n%s" % (result))
    return Job.DONE



if __name__ == '__main__':

    '''
    in this tutorial, we want introduce a plugin mechanism, called Delegator,
    to help you delegate some complex job (say, cat out all the hadoop result
    and pipe to a local file. Or execute few pig scripts with some common
    parameters and some customozed parameters). you could add your "helpers"
    by inherit the Delegatee class and implement some interfaces.
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    here we register a DFS delegatee abd a Shell delegatee.
    DFS already prepare some common commands to use: ls, cat, mv and copyFromLocal
    check the code for more.
    '''
    Job.DELEGATEES['my_dfs'] = DFS()
    Job.DELEGATEES['my_shell'] = Shell()

    '''
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to use delegatees, say DFS or Pig
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'hadoop delegatee')
    wrapper.add_plan('hadoop delegatee', Job.DONE, 'wrong command')
    wrapper.add_plan('wrong command', Job.DONE, Job.LAST_JOB)

    '''
    prepare the jobs
    '''
    j = JobNode(id='hadoop delegatee', desc='''
        cat some file on the dfs (to run this tutorial, you have to prepare
        your own data on the dfs)
    ''')
    j.set_callback(delegated_job)
    wrapper.add_sub_job(j)
    # ==
    j = JobNode(id='wrong command', desc='''
        execute some error command
    ''')
    j.set_callback(failed_delegated_job)
    wrapper.add_sub_job(j)


    '''
    run this tutorial on the Hadoop system
    '''
    # ==
    job_id, state = wrapper.execute()
    #raw_input()
