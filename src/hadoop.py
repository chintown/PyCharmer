#!/usr/bin/python

import os
import re
import sys
import random
from commands import getstatusoutput
from subprocess import Popen
from subprocess import PIPE
from subprocess import STDOUT
from time import sleep
from datetime import datetime
from pprint import pprint


from log import Logger
from exception import CommandError
from dev import deprecated

class Delegatee(object):
    NORMAL = 0
    ABNORMAL = 1
    SUCCESS = True
    FAIL = False

    SECONDS_TO_SHOW_HINT = 5
    def __init__(self, is_dry_run=False):
        self.cmd = ''
        self.is_dry_run = is_dry_run
        self.logger = None
        self.raw_output = ''
        self.stamp_output = ''

        self.idx_symbol = 0

    @deprecated
    def _execute_subproc(self):
        """ here are some commands not used anymore, but could be references """
        result_system, output_script = getstatusoutput(cmd)

        (outs ,errs) = proc.communicate()
        status= os.waitpid(proc.pid, 0)[1]
        outs = proc.stdout.read()
        errs = proc.stderr.read()

        subproc_reuslt = os.waitpid(proc.pid, 0)
        status = subproc_reuslt[1]
    def _execute(self, cmd, is_dry_run=None):
        '''
        execute command by sub process and keep flushing the stdout/stderr output
        to access the output, use get_output; the return value may be capture by
        subclass and encapisulated into more simple result (success or fail)
        this also includes some helper:
        1. provide logger if the process take a long time
        2. while failed, echo the raw command if it's a long command so that we
           could easily do the copy
        '''
        # preliminary work
        if is_dry_run is not None:
            _is_dry_run = is_dry_run
        else:
            _is_dry_run = self.is_dry_run

        self._show_command(cmd)
        cmd_snippet = '%s ... %s' % (cmd[:20], cmd[-20:])
        raw_output = ''

        # execute the command by sub process
        if _is_dry_run:
            status = self._dry_run(cmd)
            raw_output = self.stamp_output = ''
        else:
            starting_time = datetime.now()
            is_hint_enable = False
            #cmd = 'echo '+cmd
            #cmd = ["date; sleep 1;"] * 7
            #cmd = ''.join(cmd)
            proc = Popen(args=cmd, shell=True, stdout=PIPE, stderr=STDOUT)

            # record the live log, and output it to file for tail monitoring
            stamp_output = '(' + self.logger.get_stamp() + ')'
            fn = self._get_tmp_logging_file(cmd_snippet)
            if not os.path.exists(fn):
                f = open(fn, 'w+')
                os.chmod(fn, 0666)
            else:
                f = open(fn, 'w+')
            while True:
                out = proc.stdout.read(1)
                if out == '' and proc.poll() != None:
                    break
                if out != '':
                    raw_output += out
                    stamp_output += out
                    if "\n" == out:
                        out += '(' + self.logger.get_stamp() + ') '
                    f.write(out)
                    f.flush()
                #print str(self)

                if not is_hint_enable:
                    current_time = datetime.now()
                    period = current_time - starting_time
                    if Delegatee.SECONDS_TO_SHOW_HINT < period.seconds:

                        hint = "\nlive log: %s" % ( \
                            self._get_tmp_logging_file(cmd_snippet),
                        )
                        self._log(Logger.INFO, hint)
                        print hint  # if just using logger, you need to wait until
                                    # it finished
                        is_hint_enable = True
            f.close()

            self.stamp_output = stamp_output
            status = proc.returncode
        raw_output = raw_output.rstrip("\n\r")

        # exception handler
        if Delegatee.NORMAL != status:
            if len(cmd) > (self.logger.MAX_LINE_WIDTH * 2):
                cmd = "[Rerun the long command]  "+cmd
            self.cmd = cmd
            self.raw_output = raw_output
            raise CommandError(self.stamp_output)

        self.cmd = cmd;
        self.raw_output = raw_output

        return self.raw_output
    def _show_command(self, cmd):
        msg = cmd
        self._log(Logger.INFO, msg)
    def _dry_run(self, cmd):
        return Delegatee.NORMAL
    def _log(self, trace_level, *list0):
        """ log one line by delegated Logger instance """
        self.logger.log(trace_level, *list0)

    """
    while running a hadoop process, we can not directly control the output
    information; we only can log the terminal output. we reuse the file which
    logger owned.
    """
    def _get_tmp_logging_file(self, stamp=None):
        if stamp is None:
            stamp = str(self).strip("<>").split(" ")[-1]
        else:
            valid_chars = "-_a-zA-z0-9"
            stamp = re.sub("[^"+valid_chars+"]", '_',  stamp)
        return "%s.%s" % (self.logger.fn, stamp)
    def _load_temp_logging_file(self):
        if self.is_dry_run:
            log_content = 'no abnormal found while hadoop script executed'
        else:
            fn = self._get_tmp_logging_file()
            f = open(fn, 'r')
            log_content = f.read()
            f.close()

        log_content = "\n" + "HADOOP SCRIPT STARTS\n" + \
                        log_content + "\n" + \
                        "END OF HADOOP SCRIPT "
        return log_content

    def _get_loading_animation_symbols(self):
        symbols = ['|','/','-','*','\\']
        symbols = reduce(lambda l1, l2: l1+l2,
                                map(lambda s: [s]*2000, loading_states) )
        return symbols
    def _show_loading_animation_symbol(self):
        state = loading_states[self.idx_symbol % len(loading_states)]
        sys.stdout.write('['+state+"]\r")
        sys.stdout.flush()
        self.idx_symbol += 1

    def get_cmd(self):
        return self.cmd
    def get_raw_output(self):
        return self.raw_output
    def get_output(self):
        return self.stamp_output
    def set_logger(self, logger):
        self.logger = logger
    def set_dry_run(self, is_dry_run):
        self.is_dry_run = is_dry_run

    def clear(self):
        '''
        because delegatee is shared for all jobs
        the specific configuration of any job could affect other jobs
        therefore, clear is set to clean the state/config of the delegatee
        it should be called before accessing the delegatee
        '''
        pass

    # interface
    def __deepcopy__(self, memo):
        pass


class Shell(Delegatee):
    def __deepcopy__(self, memo):
        shell = Shell(self.is_dry_run)
        return shell
    def run(self, cmd, is_dry_run=None):
        """ execute customized command """
        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result

    @classmethod
    def mkdir_p(self, path, is_dry_run=False):
        result = None
        if is_dry_run:
            result = Delegatee.SUCCESS
        else:
            if not os.path.exists(path):
                dir_name = os.path.dirname(path)
                cmd = 'mkdir -p "%s/"' % dir_name
                ret = os.system(cmd)
                # print ret
                # proc = Popen(['mkdir -p "' + dir_name + '/"'], shell=True, stdout=PIPE, stderr=PIPE)
                # print proc
                # print proc.stdout.read()
                # print proc.stderr.read()
                if 0 != ret:
                    result = Delegatee.FAIL
                else:
                    result = Delegatee.SUCCESS
            else:
                result = Delegatee.SUCCESS
        return result

    @classmethod
    def safe_open(self, path_w_file, mode, is_dry_run=False):
        path = os.path.dirname(path_w_file)
        if not os.path.exists(path):
            res = Shell.mkdir_p(path_w_file)
        return open(path_w_file, mode)

    @classmethod
    def safe_write_all(self, path, content, is_dry_run=False):
        f = Shell.safe_open(path, 'w')
        f.write(content)
        f.close()

    @classmethod
    def read_one_line(self, path, is_dry_run=False):
        f = open(path, 'r')
        result = "".join(f.readlines()).rstrip("\r\n")
        f.close()
        return result
class DFS(Delegatee):

    def __init__(self, base_filepath='', is_dry_run=False):
        """
        after setting a <base> path,
        all the commands executed by this class will
        apply the <base> as its working directory
        [USAGE] dfs = DFS('/Users/chintown')
                dfs.test_command('some_file')
        [HELP] the above command equals:
                "test_command /Users/chintown/some_file"
        """
        #self.base = base_filepath + "/"
        self.base = base_filepath
        self.is_dry_run = is_dry_run
    def __deepcopy__(self, memo):
        dfs = DFS(self.base, self.is_dry_run)
        return dfs
    def test(self, filepath, is_dry_run=None):
        """
        hadoop dfs -test -e file_path
        """
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -test -e %s" % (full_filepath)

        try:
            self._execute(cmd, is_dry_run)
            result = True # uri exists
        except CommandError, e:
            result = False
        return result
    def cat(self, filepath, is_dry_run=None):
        """
        hadoop dfs -cat file_path
        """
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -cat %s" % (full_filepath)

        try:
            result = self._execute(cmd, is_dry_run)
        except CommandError, e:
            result = Delegatee.FAIL

        if self.is_dry_run or is_dry_run:
            result = random.choice([100,200,300])
        return result
    def copyFromLocal(self, from_path, to_path, mode=664, is_dry_run=None):
        """
        hadoop dfs -copyFromLocal from_path to_path
        """
        full_to_path = self.base + to_path
        cmd = "hadoop dfs -copyFromLocal %s %s && hadoop dfs -chmod %s %s" % \
              (from_path, full_to_path, mode, full_to_path)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result
    def copyToLocal(self, from_path, to_path, is_dry_run=None):
        """
        hadoop dfs -copyToLocal from_path to_path
        """
        full_to_path = self.base + to_path

        Shell.mkdir_p(full_to_path, is_dry_run)

        cmd = "hadoop dfs -copyToLocal %s %s" % (from_path, full_to_path)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result
    def getmerge(self, from_path, to_path, is_dry_run=None):
        """
        hadoop dfs -getmerge from_path to_path
        """
        full_from_path = self.base + from_path
        cmd = "hadoop dfs -getmerge %s %s" % (from_path, full_to_path)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result
    def rmr(self, filepath, is_dry_run=None, is_skip_trash=False):
        """
        hadoop dfs -rmr filepath
        """
        if is_skip_trash: trash = '-skipTrash'
        else: trash  = ''
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -rmr %s %s" % (trash, filepath)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result
    def rm(self, filepath, is_dry_run=None, is_skip_trash=False):
        """
        hadoop dfs -rm filepath
        """
        if is_skip_trash: trash = '-skipTrash'
        else: trash  = ''
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -rm %s %s" % (trash, filepath)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result
    def mv(self, from_path , to_path, is_dry_run=None):
        """
        hadoop dfs -mv from_path, to_path
        """
        full_from = self.base + from_path
        full_to = self.base + to_path

        cmd = "hadoop dfs -mv %s %s" % (full_from, full_to)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result
    def chmod(self, filepath, mode, is_dry_run=None):
        """
        hadoop dfs -chmod mode filepath
        """
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -chmod %s %s" % (mode, full_filepath)

        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result

    def lswc(self, filepath, is_dry_run=None):
        """
        hadoop dfs -ls filepath | wc -l
        please be careful while you apply piped command
        the system error code may not refect the error
        you have to check the raw output to identify the status
        """
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -ls %s | wc -l" % (filepath)

        try:
            result = self._execute(cmd, is_dry_run)
            try:
                result = int(result)
            except ValueError, e:
                self.stamp_output = result
                result = Delegatee.FAIL
        except CommandError, e:
            result = Delegatee.FAIL
        if self.is_dry_run or is_dry_run:
            result = random.choice([0,100,200,300])
        return result

    def ls(self, filepath, is_dry_run=None):
        """
        hadoop dfs -ls filepath
        """
        full_filepath = self.base + filepath
        cmd = "hadoop dfs -ls %s" % (filepath)

        try:
            result = self._execute(cmd, is_dry_run)
        except CommandError, e:
            result = Delegatee.FAIL

        return result


    def freehand(self, cmd, is_dry_run=None):
        """
        execute customized command
        """
        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        if self.is_dry_run:
            result = random.choice([True, False])
        return result
class Pig(Delegatee):
    # all the pib who needed configarable parallel numbel
    # should use value of VAR_NAME_FOR_PARALLEL_NUM
    VAR_NAME_FOR_NUM_PARALLEL = 'num_parallel'

    def __init__(self, queue, num_parallel, is_dry_run=False):
        self.is_profile = False
        self.is_substitute = False
        self.script = ''
        self.queue = ''
        self.log_file = ''
        self.class_path = ''
        self.params = {}
        self.extra_params = []
        self.num_parallel = num_parallel
        self.set_num_parallel(num_parallel)
        self.set_queue(queue)
        self.is_dry_run = is_dry_run
    def __deepcopy__(self, memo):
        pig = Pig(self.queue, self.num_parallel)
        pig.is_dry_run = self.is_dry_run
        pig.is_profile = self.is_profile
        pig.is_substitute = self.is_substitute
        return pig
    def _show_command(self, cmd):
        msg = cmd
        msg = msg.replace('-param', '\n-param')
        tokens = msg.split("\n")
        map(lambda t: self._log(Logger.INFO, t), tokens)
    def _output_pig_script(self, cmd):
        dry_run_cmd = cmd.replace('pig ','pig -dryrun ')
        try:
            self._execute(dry_run_cmd, is_dry_run=False)
        except CommandError, e:
            pass
            # pig -dryrun will return error code to terminate the process,
            # it is the normal case

    def clear(self):
        self.cmd = ''
        self.script = ''
        self.params = {}
        self.extra_params = []
        self.set_num_parallel(self.num_parallel)
    def set_profiling(self, is_profile):
        self.is_profile = is_profile
    def set_substitute(self, is_substitute):
        self.is_substitute = is_substitute
    def set_script(self, script):
        self.script = script
    def set_queue(self, queue):
        self.queue = queue
    def set_log_file(self, log_file):
        self.log_file = log_file
    def set_class_path(self, class_path):
        self.class_path = class_path
    def set_num_parallel(self, num_parallel):
        self.params[Pig.VAR_NAME_FOR_NUM_PARALLEL] = num_parallel
    def set_params(self, param_dict):
        # some setting (e.g. parallel_num) may be set before this
        self.params.update(param_dict)
    def set_extra_param_string(self, param_string):
        self.extra_params.append(param_string)

    def run(self, is_dry_run=None):
        cmd_tokens = []

        executable = "pig " # /grid/0/gs/pig/bin/pig
        if self.is_profile:
            executable = 'time '+ executable
        cmd_tokens.append(executable)

        queue = '-Dmapred.job.queue.name='+self.queue
        cmd_tokens.append(queue)

        if self.class_path != '':
            class_path = '-cp '+ self.class_path
            cmd_tokens.append(class_path)

        params = map(lambda (k,v): ' -param '+k+'='+str(v), self.params.items())
        params = ''.join(params)
        cmd_tokens.append(params)

        if not '-Dmapred.job.reduce.memory.mb=4096' in self.extra_params:
            self.extra_params.append('-Dmapred.job.reduce.memory.mb=4096')
        cmd_tokens.append(' '.join(self.extra_params))

        cmd_tokens.append(self.script)

        #log = '>& '+self.log_file
        #cmd_tokens.append(log)

        cmd = ' '.join(cmd_tokens)

        if self.is_substitute:
            self._output_pig_script(cmd)
        try:
            self._execute(cmd, is_dry_run)
            result = Delegatee.SUCCESS
        except CommandError, e:
            result = Delegatee.FAIL
        return result

if __name__ == '__main__':
    dfs = DFS('/Users/chintown')
    dfs.test('/test_path', is_dry_run=True)
