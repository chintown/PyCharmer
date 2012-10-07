#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import random
from copy import deepcopy

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


'''
in the callback, we also need to explicitly declare the input we need.
(ideally, they should be matched with the inputs in the plan)
the engine will check whether all the required inputs here are properly
declared in the plan before executing the process
'''
def hello_job(self):
    message = self.get_input('msg')
    self.log(Logger.INFO, message)
    return Job.DONE

def Serious_avator(self):
    message = self.get_input('msg_to_Serious')
    self.log(Logger.INFO, '(%s)' % (message))
    self.log(Logger.INFO, 'I barely know you!')
    return Job.DONE


if __name__ == '__main__':

    '''
    in this tutorial, we will introduce the configuration mechanism which helps
    you handle the communication of inputs and outputs between the jobs.
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    we want to prepare two JobNodes, which say hello to the input name it gets.
    the most parts of the jobs are the same, except the input.
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to use configuration mechanism for input data
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'hello Serious')
    wrapper.add_plan('hello Serious', Job.DONE, 'hello Kidding')
    wrapper.add_plan('hello Kidding', Job.DONE, Job.LAST_JOB)

    '''
    first, we build a template/prototype job for the hello jobs and assign
    a key-value pair input. the input could be access in the callback
    by self.get_input(<key_of_the_input>). note that we bracket the name in the
    config value. it's a variablized config. we will explain it later.
    '''
    # ==
    j_temp = JobNode(id='template', desc='say hello to someone')
    j_temp.need_input('msg', 'hello! Mr.[name]')
    j_temp.set_callback(hello_job)
    '''
    instead of directly add the template job into wrapper
    '''
    # wrapper.add_sub_job(j_temp)

    '''
    we make two copies from the templates and give the correct id and description.
    then, we assign the name to each job. you may guess the result - the input of
    template job, "msg", would be "completed" by replacing the "[name]" with
    the actual value we assign to the each job.
    '''
    # ==
    j = deepcopy(j_temp)
    j.id = 'hello Serious'
    j.desc = 'say hello to Serious'
    j.need_input('name', 'Serious')
    wrapper.add_sub_job(j)
    # ==
    j = deepcopy(j_temp)
    j.id = 'hello Kidding'
    j.desc = 'say hello to Kidding'
    j.need_input('name', 'Kidding')
    wrapper.add_sub_job(j)

    '''
    another thing should be highlight is - we need to explicitly declare the
    inputs/outputs. thus, we could automatically generate the document.
    the benefit of the strategy is we could have a well-organized
    document while we need to discuss with colleagues; rather than opening a
    thousand-lines code to make a review
    check the callback, there is a similar philosophy you need to follow
    '''

    '''
    check the result to re-exame the flow of the process
    the completion mechanism could ease your work of composing the input values.
    the same effect could be worked on the config "key" rather than value.
    we will talk about it in the next tutorial
    '''
    # ==
    job_id, state = wrapper.execute()
