#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import random
from copy import deepcopy

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../src' not in sys.path:
    sys.path.append(current_dirpath +'../src')

from job import Job
from job import JobNode
from job import JobBlock
from job import ParallelJobBlock

from log    import Logger

def hello_job(self):
    name = self.get_input('name')
    message = self.get_input('msg')
    self.log(Logger.INFO, message)
    self.set_output('msg_to_'+name, message)
    return Job.DONE

def Serious_avatar(self):
    message = self.get_input('msg_to_Serious')
    self.log(Logger.INFO, '(%s)' % (message))
    self.log(Logger.INFO, 'Mr.Serious: Hey, how\'s going')
    return Job.DONE

def Kidding_avatar(self):
    message = self.get_input('msg_to_Kidding')
    self.log(Logger.INFO, '(%s)' % (message))
    self.log(Logger.INFO, 'Mr. Kidding: I barely know you!')
    return Job.DONE

if __name__ == '__main__':

    '''
    in this tutorial, we will introduce more features of configuration mechanism.
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    we follow the previous example, remember Mr. Serious and Mr.Kidding?
    Imaging this scenario:
    our job say hello to Mr. Serious and Mr. Kidding, we want to keep
    result message, and pass them to Mr. Serious and Mr. Kidding separately.
    (Serious and Kidding are conceptualize into two jobs)
    '''
    wrapper = JobBlock('entry job', '''
        this job demonstrate how to utilize variablized configuration
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'hello Serious')
    wrapper.add_plan('hello Serious', Job.DONE, 'hello Kidding')
    wrapper.add_plan('hello Kidding', Job.DONE, 'Serious')
    wrapper.add_plan('Serious', Job.DONE, 'Kidding')
    wrapper.add_plan('Kidding', Job.DONE, Job.LAST_JOB)

    '''
    same as previous tutorial
    but we declare the output, 'msg_to_[name]',which represent the message to be kept.
    the callback are also modified.
    '''
    # ==
    j_temp = JobNode(id='hello template', desc='say hello to someone')
    j_temp.need_input('msg', 'hello! Mr.[name]')
    j_temp.need_output('msg_to_[name]')
    j_temp.set_callback(hello_job)

    '''
    remember we mentioned in the tutorial_04 that all the inputs should be
    explicitly declared. same as output. actually, it's fine if you don't
    declare the outputs; the process will still be executed correctly.
    however, this strategy is trying to improve the readability of the code.
    a person just take your code may be not familiar with the flow. the declared
    outputs will be listed in the generated document and help the folk to catch
    the key concepts of the job.
    '''

    '''
    same as previous tutorial
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
    and we add the two avatars.
    they receive the output of the first job, msg_to_[name], as its input
    and response individually
    '''
    j = JobNode(id='Serious', desc="I'm Serious")
    j.need_input('msg_to_Serious')
    j.set_callback(Serious_avatar)
    wrapper.add_sub_job(j)

    j = JobNode(id='Kidding', desc="I'm Kidding")
    j.need_input('msg_to_Kidding')
    j.set_callback(Kidding_avatar)
    wrapper.add_sub_job(j)

    '''
    them you will see the conversation
    '''
    # ==
    job_id, state = wrapper.execute()
    #raw_input()
