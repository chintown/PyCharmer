#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import random

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../lib' not in sys.path:
    sys.path.append(current_dirpath +'../lib')

from job import Job
from job import JobNode
from job import JobBlock

from log    import Logger

def foo_job(self):
    if self.is_dry_run():
        self.log(Logger.INFO, "I'm working in dry run mode.")
    else:
        self.log(Logger.INFO, "I'm virtually executed")

    return Job.DONE

def bar_job(self):
    if self.is_dry_run():
        self.log(Logger.INFO, "I'm working in dry run mode.")
    else:
        self.log(Logger.INFO, "I'm virtually executed")

    return Job.DONE

def foobar_job(self):
    if self.is_dry_run():
        self.log(Logger.INFO, "I'm working in dry run mode.")
    else:
        self.log(Logger.INFO, "something strange")

    return Job.DONE

def fob_job(self):
    if self.is_dry_run():
        self.log(Logger.INFO, "I'm working in dry run mode.")
    else:
        self.log(Logger.INFO, "I'm virtually executed")

    return Job.DONE


if __name__ == '__main__':

    '''
    in this tutorial, we want introduce a handy tools for development:
    dry_run mechanism
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    imaging that you have a bunch of job in the flow. (all right, we only demo by
    four jobs) under development, you want to check the inputs/outputs
    you assigned are correct before running the process. (because some job may
    cause *permanent* affect, say rm -rf?) the "dry run" mode provides you a way
    to print out the command and evaluate the outputs without actualy executing them.
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to use dry run mechanism
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'foo')
    wrapper.add_plan('foo', Job.DONE, 'block')
    wrapper.add_plan('block', Job.DONE, 'fob')
    wrapper.add_plan('fob', Job.DONE, Job.LAST_JOB)

    '''
    now, we enable a secret switch to tell the whole process in the dry run mode
    '''
    wrapper.set_dry_run(True)

    '''
    prepare job sea
    '''
    j = JobNode(id='foo', desc=''' foo ''')
    j.set_callback(foo_job)
    wrapper.add_sub_job(j)
    # ==
    j = JobBlock(id='block', desc=''' block ''')
    j.add_plan(Job.INIT_JOB, Job.START, 'bar')
    j.add_plan('bar', Job.DONE, 'foobar')
    j.add_plan('foobar', Job.DONE, Job.LAST_JOB)
    # --
    j_sub = JobNode(id='bar', desc=''' bar ''')
    j_sub.set_callback(foo_job)
    j.add_sub_job(j_sub)
    # --
    j_sub = JobNode(id='foobar', desc=''' foobar ''')
    j_sub.set_callback(foo_job)
    j.add_sub_job(j_sub)
    # --
    wrapper.add_sub_job(j)
    # ==
    j = JobNode(id='fob', desc=''' fob ''')
    j.set_callback(fob_job)
    wrapper.add_sub_job(j)

    '''
    after several round of testing, i found most jobs work fine.
    but there's something stange in the second job. so I want to enable the
    real mode only for that job. then, we could set dry_run for indivisual job.
    however, the dry_run config is a global setting; once you set for any job,
    it will affect on the whole proces. to specify the scope on the certain job,
    you need to set the config locally by set_as_local parameter.

    wait. there is another trick in the config: the sub jobs of a JobBlock will
    inherit the config from its parent. so you could limit the scope of real mode
    step by step to testing the ill job.
    '''
    j = wrapper.find_job('block')
    j.set_dry_run(False, set_as_local=True)


    # ==
    job_id, state = wrapper.execute()
    #raw_input()
