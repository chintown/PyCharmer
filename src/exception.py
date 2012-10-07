#!/usr/bin/python

class CommandError(Exception):
    '''
    mainly used for Delegatee class in hadoop.py
    '''
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return str(self.value)