#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../lib' not in sys.path:
    sys.path.append(current_dirpath +'../lib')

from job import Job
from job import JobNode
from job import JobBlock

from log    import Logger

'''
the things we put here are some "callbacks"
but before dig into this part, we may start from the main block.
the reason we put these callbacks in the very beginning is that
some things are needed to be declared before using.
'''

'''
[read these when you touch the callback issue in the following document]
each callback method should implement the interface which contains only one
parameter, "self". the "self" object contains all you need in the implementation.
for example, if you need to print something (or log. because the process may be
a cron job and we have no way to check standard output but log), you can use
build-in method of "self". we will show the power of self in the rest of tutorial.
and after you do something in the callback, (and maybe get some result) you need
to declare the result state by returning a corresponding value. then, the job
wrapper will follow the state to find next job from the plan tree.
the states are finite -
  Job.DONE: the normal state
  Job.SKIP: indicates that there's a optional path if some condition matched
  Job.ERROR: which will up-forward the error state to the parent job. and finally
             interrupt the process.
OK, it's time to go back to the main block.
'''
def normal_job(self):
    self.log(Logger.INFO, 'do something')
    return Job.DONE

if __name__ == '__main__':

    '''
    first of all, we need to prepare a log object to record the
    message from planning diagnose to runtime output.
    assigning the Logger instance to Job singleton,
    we could use self.log as a build-in method for each job.
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    then, we plan the process with two separated jobs, job0 and job1.
    each plan is comprising of a starting job, a state and an ending job.
    you may notice there are two special job in the very begin and the end.
    they are part of "syntax" of the plan; you should assign Job.INIT_JOB and
    Job.LAST_JOB for any of your process.
    in this step, we just give some job name (id) and compose their sequence
    without thing any detail implementation of them. that helps you be focus on
    the planning.
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to add simple JobBlocks
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'job0')
    wrapper.add_plan('job0', Job.DONE, 'job1')
    wrapper.add_plan('job1', Job.DONE, Job.LAST_JOB)

    '''
    now we start to plan the detail of each job.
    each job should have a id and a paragraph of desc(ription) which will be
    generated into document and you won't be bother to prepare any other document.
    this mechanism helps the code to be kept alive.
    the job we need here are some very simple job. let's say we wanna print
    something in each job, so we don't need to prepare any input. (we leave this
    to other tutorial codes.) so we assign a "callback" method, normal_job, to the
    job. now you could check the callbacks in the beginning of this code.
    '''
    # ==
    j = JobNode(id='job0',desc='desc0')
    j.set_callback(normal_job)
    wrapper.add_sub_job(j)
    # ==
    j = JobNode(id='job1',desc='desc1')
    j.set_callback(normal_job)
    wrapper.add_sub_job(j)
    # ==

    '''
    things are almost done.
    all we need to do is to trigger the execution!
    check the result to re-exame the flow of the process
    '''
    # ==
    job_id, state = wrapper.execute()
    #raw_input()
