#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import codecs
from pprint import pprint
from datetime import datetime


class ConfigObject(object):
    '''
    this class behaves like a dictionary but could record the inheritance record
    '''
    def __init__(self, dict, id):
        self.id = id
        self.set_conifg(dict)

    def set_conifg(self, dict):
        config = {}
        for k, v in dict.items():
            record = [{'value': v, 'source': self.id, 'stamp': datetime.now()}]
            config[k] = record
        self.config = config

    def get_config(self):
        return self.config

    def keys(self):
        return self.config.keys()

    def get(self, key):
        return self.config.get(key)

    def __getitem__(self, key):
        '''
        get last overrided item, which is placed at the end of list
        '''
        if self.config.get(key) is None:
            return None
        else:
            value = self.config[key][-1]['value']
            return value

    def __len__(self):
        return len(self.config)

    def update_record(self, prior_record, key):
        '''
        add key value pair if it's not show in the config
        otherwise, override the existed key
        '''
        self_record = self.config.get(key)
        last_record = prior_record.pop()
        last_record['stamp'] = datetime.now()
        mergerd_record = prior_record + self_record
        mergerd_record.sort(key=lambda l: l['stamp'], reverse=False)
        mergerd_record.append(last_record)
        self.config[key] = mergerd_record

    def update(self, prior_config_object):
        '''
        similar to the update_record, but update the whole input in batch mode
        '''
        prior_config = prior_config_object.get_config()
        self_config = self.config
        union = set(prior_config.keys() + self_config.keys())

        for key in union:
            prior_record = prior_config.get(key)
            self_record = self_config.get(key)
            # update self by prior
            if prior_record is not None and self_record is not None:
                self.update_record(prior_record, key)
            elif self_record is None:
                self_config[key] = prior_record
            elif prior_record is None:
                pass

    def output_history(self):
        '''
        output the config to dictionary (and keep the inheritance records)
        '''
        output_config = {}
        for k, records in self.config.items():
            snap_shots = []
            for record in records:
                snap_shots.append("%s(%s)" % (record['value'], record['source']))
            snap_shots.reverse()
            if len(snap_shots) > 1:
                prefix = "(!)"
            else:
                prefix = ''
            output_config[prefix + k] = ' <= '.join(snap_shots)
        return output_config

    def output_dict(self):
        '''
        output the config to dictionary (and discard the inheritance records)
        '''
        output_config = {}
        for k, records in self.config.items():
            output_config[k] = records[-1]['value']
        return output_config

    def pprint_dict(self):
        '''
        pretty print for output_history
        '''
        history_configs = self.output_history()
        max_key_length = max(map(lambda item: len(item), history_configs.keys()))
        history_configs = history_configs.items()
        history_configs.sort(key=lambda item: item[0])
        ptn = "%-" + str(max_key_length) + "s := %s"
        for k, v in history_configs:
            print ptn % (k, v)


def get_full_path(file_path_relative_to_project_root):
    script_filepath = os.path.realpath(__file__)
    script_path = os.path.dirname(script_filepath)
    script_root = script_path + '/../'
    full_path = script_root + file_path_relative_to_project_root
    return full_path


def get_full_path_from_importer(file_path_relative_to_importer):
    importer = sys._getframe(2).f_globals.get('__file__')
    importer_filepath = os.path.realpath(importer)
    importer_path = os.path.dirname(importer_filepath)
    full_file_path = os.path.join(importer_path, file_path_relative_to_importer)
    return full_file_path

    # script_filepath = os.path.realpath(__file__)
    # script_path = os.path.dirname(script_filepath)
    # script_root = script_path + '/../'
    # full_path = script_root + file_path_relative_to_importer
    # return full_path


def load(fn):
    """
    this method loads the given config filename as a dictionary
    however, the relative path depends on the execution directory.
    """
    f = codecs.open(fn, 'r', 'utf8')
    content = f.read()
    config = []
    try:
        config = eval(content)
    except Exception, e:
        print 'syntax error found in config file %s' % (fn)
        print e
        exit(1)
    return config


def load_from_importer(fn, id):
    """
    this method will load the config filename as a dictionary
    from the file import config.py no matter where you execute the script
    and bind the given id to the loaded config so that we can easily distinguish
    the configs from different files
    """
    # importer = sys._getframe(1).f_globals.get('__file__')
    # importer_filepath = os.path.realpath(importer)
    # importer_path = os.path.dirname(importer_filepath)
    full_file_path = get_full_path_from_importer(fn)
    result = load(full_file_path)
    co = ConfigObject(result, id)
    return co


def load_from_script_root(fn, id):
    """
    this method will load the config filename as a dictionary
    from the script root, no matter where you execute the script
    and bind the given id to the loaded config so that we can easily distinguish
    the configs from different files
    """
    full_file_path = get_full_path(fn)
    result = load(full_file_path)
    co = ConfigObject(result, id)
    return co


def soft_load_from_script_root(fn, id):
    """
    similar to load_from_script_root, but only load only if the file is existed
    """
    full_file_path = get_full_path(fn)
    result = {}
    if os.path.exists(full_file_path):
        result = load(full_file_path)
    co = ConfigObject(result, id)
    return co


def convert_option(option_obj):
    """
    * this method is bound to the build-in library, optparse,
    whose parser return an option object contains command line arguments.
    we hack it to be a dict by calling its stringify method (__str__)
    and convert it back to dict by eval method.
    """
    option = eval(str(option_obj))
    # filter the pair whose value is None
    # to prevent None value overrides the default config
    option = dict((k, v) for k, v in option.items() if v is not None)
    co = ConfigObject(option, 'command_line')
    return co


def pickup_from_dict(tar_dict, keys):
    '''
    get a subset of the given dictionary by the given IDs
    '''
    pickuped = dict((k, v) for k, v in tar_dict.items() if k in keys)
    return pickuped


def is_first_time_execution():
    '''
    a flag file, .cookie, will be set after this method excuted so that we could
    know whether the whole project is excueted on a new environment or not; and
    could provide some hint to the user.
    '''
    flag_file = get_full_path('conf/.cookie')
    try:
        open(flag_file, 'r').close()
        return False
    except IOError, e:
        f = open(flag_file, 'w')
        f.write('the existence of this file indicates that user has ran the' +
                'script before. therefor, we will not show the "config hint" anymore.')
        f.close()
        return True

if __name__ == '__main__':
    a = {'1': 1, '2': 2}
    b = {'2': 'x', '3': 3}
    c = {'2': 'y', '3': 3}
    ao = ConfigObject(a, 'a')
    bo = ConfigObject(b, 'b')
    co = ConfigObject(c, 'c')
    ao.update(bo)
    ao.update(co)
    pprint(ao.output_history())
    pprint(ao.output_dict())
