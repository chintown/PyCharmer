#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../src' not in sys.path:
    sys.path.append(current_dirpath +'../src')

from job import Job
from job import JobNode
from job import JobBlock

from log    import Logger

def normal_job(self):
    self.log(Logger.INFO, 'do something')
    return Job.DONE

if __name__ == '__main__':

    '''
    in this tutorial, we will introduce JobBlock which allows you to compose
    your JobNodes into tree-style structure. With hierarchical structure, you
    could organize your job better (from writer and reader point of view)
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    following the previous tutorial, we use the existed two JobNodes, but wrap
    them into on JobBlock
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to organize your plan by JobBlocks
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'block1')
    wrapper.add_plan('block1', Job.DONE, Job.LAST_JOB)

    '''
    first, with the top-down design strategy, we define JobBlock, which is like
    wrapper with its own plan.
    '''
    # ==
    j = JobBlock(id='block1', desc='block1')
    j.add_plan(Job.INIT_JOB, Job.START, 'job0')
    j.add_plan('job0', Job.DONE, 'job1')
    j.add_plan('job1', Job.DONE, Job.LAST_JOB)
    wrapper.add_sub_job(j)

    '''
    then, we define the inner JobNodes (same as previous tutorial)
    '''
    # ==
    j_sub = JobNode(id='job0',desc='desc0')
    j_sub.set_callback(normal_job)
    j.add_sub_job(j_sub)
    # ==
    j_sub = JobNode(id='job1',desc='desc1')
    j_sub.set_callback(normal_job)
    j.add_sub_job(j_sub)
    # ==

    '''
    BTW, here's a small tips.
    while designing a large flow, you may want to well-organize your code by
    putting the related things together.
    but sometimes, you can't assign the value you want right after the job is
    initiated because the value should be calculated/generated later.
    we provide the flexibility to delay the manipulation.
    '''

    # some other code
    # ...
    # ...

    '''
    say, we generate something here
    '''
    some_value = ' blah blah blah'

    '''
    and we want to append the value to the description of job1.
    how to get the object of job1?
    there's no need to give a unique variable, say job1, for just accessing it.
    (that increases the things you need to remember in code )
    all you need to do is asking the wrapper to search for that job.
    no matter how deep is the job inside the wrapper, it could be retrieved by
    recursion search.
    '''
    j =  wrapper.find_job('job1')
    j.desc += some_value


    '''
    check the result to re-exame the flow of the process
    '''
    # ==
    job_id, state = wrapper.execute()
    #raw_input()
