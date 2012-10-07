#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
from optparse import OptionParser

# add path of project library into python path
current_filepath =  os.path.realpath(__file__)
current_dirpath  = os.path.dirname(current_filepath) + "/"
if current_dirpath +'../lib' not in sys.path:
    sys.path.append(current_dirpath +'../lib')

from job import Job
from job import JobNode
from job import JobBlock

import config

from log    import Logger

def foo_job(self):
    input_file_path = self.get_input('a very long path blah blah')
    some_file_path = self.get_input('composite path')
    output_file_path = self.get_output('output_path')

    self.log(Logger.INFO, 'execute the following command:')
    self.log(Logger.INFO, 'ls %s' % (some_file_path))
    self.log(Logger.INFO, 'cat %s | wc -l > %s' % (input_file_path, output_file_path))


    return Job.DONE


if __name__ == '__main__':

    '''
    in this tutorial, we want introduce config helper
    which allows you to load and pick up the configs in a better way
    '''
    process_id = __file__.lstrip('./')
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    '''
    before the tutorial, we introduce a python module, OptionParser, to load
    options from command line. with thus module, you could apply config more
    dynamically. for example, we set "-y" to enable dry run mode.
    '''
    parser = OptionParser()
    parser.add_option("-y", "--is_dry_run", help="show all the information of process", action='store_true')
    (options, args) = parser.parse_args()

    '''
    the option variable is an object; that means you have to understand the API
    of it. therefore, we make some conversion so that we could manipulator it
    with the other kind of config (say, file) in an unified interface. and after
    all kinds of config are loaded, we will convert the final config container
    into a simple python dictionary which is comprising of key-value pairs
    '''
    configs_command = config.convert_option(options)

    '''
    some config are rarely changed, we want them stay in the config file rather
    than list them in the command line every time.
    we provide two config loading helpers,
    "load_from_script_root" and "soft_load_from_script_root"
    the difference between "soft" loading and the normal loading is -
    soft loading will be ignored if the target file is not existed;
    normal loading does not.
    this mechanism give the flexibility to load some environment-dependent configs.
    '''
    configs     = config.load_from_script_root('./doc/test_config', id='base')
    configs_dev = config.load_from_script_root('./doc/test_config_dev', id='dev')

    '''
    now, we want the configs to be overrided by configs_dev
    just call the update method - the config with same key will be overrided by
    the latter one. (align with the behavior of python built-in update method
    for dictionary)
    '''
    configs.update(configs_dev)

    '''
    you may notice that we give an id to each config while loading them.
    the purpose is to trace the inheritance history.
    call pprint_dict() and check the output, you will understand what's going on.
    '''
    print '============= Inheritance Table of Configuration ==================='
    configs.pprint_dict()
    print '============= Inheritance Table of Configuration ==================='

    '''
    actually, the config object implements some common interfaces of python dictionary,
    so you could manipulator it as if it's a dictionary.
    but for purifying it, we finally convert it into a real dictionary
    '''
    is_dry_run_mode = configs['is_dry_run']
    CFG = configs.output_dict()

    '''
    now we have everything (from files and command line) in the CFG.
    (of course you could produce more configs and put them in to the CFG.)
    but this CFG is nothing connected to the job flow.

    we often need to concatenate a filepath by the directory in the configs.
    for example, some_file_path = CFG['dir1']+'/'+CFG['dir2']+'/'+CFG['file']
    this thing will be repeated over and over. scary, right.

    we could utilized the power of config mechanism we mentioned in the tutorial_04 -
    declare once, use anywhere. but this time we declare the picked config in
    Job singleton rather than specific JobNode or JobBlock object.

    choose the configs you need in the process and all the job in the process
    can easily access those configs.
    '''
    Job.set_global('a very long path blah blah', CFG['a very long path blah blah'])
    Job.set_global('another very long path blah blah', CFG['another very long path blah blah'])
    Job.set_global('yet another very long path blah blah', CFG['yet another very long path blah blah'])

    '''
    wait, do you notice that we are repeatedly typing the variable name?
    we don't like bad smell, even it is slightly.
    we introduce a helper in the config module, called pickup_from_dict
    applying it with the python build-in method, map, could ease your work
    '''
    configs_for_jobs = config.pickup_from_dict(CFG, [
        'a very long path blah blah',           # you can even
        'another very long path blah blah',     # make some tidy
        'yet another very long path blah blah', # comment here
    ])
    map(lambda key: Job.set_global(key, CFG[key]), configs_for_jobs.keys())


    wrapper = JobBlock('entry job', '''
        this job demonstrate how to use config management module
    ''')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'foo')
    wrapper.add_plan('foo', Job.DONE, Job.LAST_JOB)

    '''
    we could get the configs we just set as global by giving the key without value
    or, we could put it into some other input

    here we also introduce another usage of output:
    in the tutorial_04, we set the key of output without value; that's a kind of
    declaration to exclaim 'we will put some value with that key as the output.
    (and the later jobs could access it as input)
    this time, we do give value to output key because we want the job output
    something to the path we expected.
    '''
    j = JobNode(id='foo', desc=''' foo ''')
    j.need_input('a very long path blah blah')
    j.need_input('composite path', '[another very long path blah blah]/append_with_a_sub_directory')
    j.need_output('output_path','[yet another very long path blah blah]')
    j.set_callback(foo_job)
    wrapper.add_sub_job(j)


    job_id, state = wrapper.execute()
    #raw_input()
