#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import random
from time import sleep
from datetime import datetime

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../lib' not in sys.path:
    sys.path.append(current_dirpath +'../lib')

from job import Job
from job import JobNode
from job import JobBlock
from job import ParallelJobBlock

from log    import Logger

def lazy_job(self):
    for i in xrange(5):
        self.log(Logger.INFO, 'wake up at %s' % datetime.now())
        sleep(0.1)
    return Job.DONE

if __name__ == '__main__':

    '''
    in this tutorial, we will introduce ParallelJobBlock which allows you to
    execute multiple JobBlocks parallelly
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    following the previous tutorial, we use the existed two JobNodes, but wrap
    them into on ParallelJobBlock
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to execute job parallelly
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'para block1')
    wrapper.add_plan('para block1', Job.DONE, Job.LAST_JOB)

    '''
    first, as usual top-down design strategy, we define ParallelJobBlock, which
    is like wrapper with its own plan.
    however, to add parallel plans is a little different from JobBlock.
    we call add_papallel_plan and assign all the inner job IDs in the same time.
    no plan is allowed in the ParallelJobBlock!
    '''
    # ==
    j = ParallelJobBlock(id='para block1', desc='para block1')
    j.add_papallel_plan('job0','job1')
    wrapper.add_sub_job(j)

    '''
    then, we define the inner JobNodes.
    this time, we want let the job print something and sleep for a while few times.
    because both parallel jobs will print messages, applying one buffer will
    result in a mass. therefore, the flow engine will prepare one buffer for
    each parallel job. at the end of the job, the parent job will dump the
    children buffers sequentially. (first done, first dump)
    '''
    # ==
    j_sub = JobNode(id='job0',desc='desc0')
    j_sub.set_callback(lazy_job)
    j.add_sub_job(j_sub)
    # ==
    j_sub = JobNode(id='job1',desc='desc1')
    j_sub.set_callback(lazy_job)
    j.add_sub_job(j_sub)
    # ==

    '''
    check the result to re-exame the flow of the process
    '''
    # ==
    job_id, state = wrapper.execute()
    #raw_input()

    '''
    now, we introduced all the building blocks. try to contruct your job flow!
    the rest tutorial will introduce some advanced features (configs and delegations)
    '''