#!/usr/bin/python
import sys
import re
import copy
import inspect
from sys import exit
from new import instancemethod
from threading import Thread
from pprint import pprint

from log import Logger
from word_distance import cliff
import graph
from dev import deprecated

"""
this is the interface for JobNode and JobBlock
provide a common triggering method, execute()

this interface maintain a set of state, including: START, DONE and etc.
all the callback methods should carefully arrange the state in its implementation

this interface also maintain two pseudo job, including: INIT_JOB and LAST_JOB.
applying these two JOB in the job plan to indicates the starting/ending point.

each Job could maintain a configuration dictionary to store job-dependent information
say, file path, time stamp or some threshold numbers

sometimes latter job needs to check the result from previous job, in this case,
they could store/access the "global" information in the variable, <GLOBALS>
"""
class Job(object):
    # the destination of state should be decided by job plan (designer)    (iii)
    # rather than program. program could move on even meet ERROR state
    # there is a one and only one exception, TERMINATE.
    # once job meet TERMINATE from its subjob, it should stop process and   (i)
    # report TERMINATE to its parent. top level job should exit the process.(ii)
    START = 'START'
    SKIP  = 'SKIP'
    DONE  = 'DONE'
    LOOP_DONE = 'LOOP_DONE'
    TERMINATE = 'TERMINATE'
    ERROR = 'ERROR'

    # last job indicates the end of job block
    # exit job indicates the end of program, however, sub job could not end (iv)
    # it's life. it should report to the top level job through TERMINATE state
    # for thread-safe terminating the whole process.
    INIT_JOB = 'INIT_JOB'
    LAST_JOB = 'LAST_JOB'
    EXIT_JOB = 'EXIT_JOB'

    _PSEUDO_JOB = [INIT_JOB, LAST_JOB, EXIT_JOB]
    _BLACK_INPUTS = ['is_dry_run'] # don't check this internal-used input

    GLOBALS = {} # TODO is there a better way
    LOGGER = None
    DELEGATEES = {}
    def __init__(self, id, desc):
        if id in Job._PSEUDO_JOB:
            msg = "[ERROR] '%s' is not a valid job id" % (id)
            reason = "'%s' is a preserved id" % (Job.INIT_JOB)
            if Job.LOGGER is not None:
                Job.LOGGER.log(Logger.ERRO, msg)
                Job.LOGGER.log(Logger.ERRO_WHY, reason)
                Job.LOGGER.log(Logger.EXIT, '')
            else:
                print msg
                print reason
                print 'process '
            exit(1)
        self.id = id
        self.desc = desc
        self.plannable = None
        self.config = {}
        self.inputs = {}
        self.outputs = {}
        self.logger = None
        self.delegatees = None
        self.is_visualized = False
        self.interactive = False

        self._user_defined_pre_hook = None
        self._user_defined_post_hook = None
    def pre_hook(self):
        """
        tracking the job information
        """
        #self.log(Logger.INFO, self.id, self.is_dry_run())
        #self.log(Logger.INFO, self.id, self.logger)
        #self.log(Logger.INFO, self.id, self.delegatees)
        #if self.delegatees is None : return
        #for id, delegatee in self.delegatees.items():
        #    d = str(delegatee).replace('object at ', '')
        #    l = str(delegatee.logger).replace('object at ', '')
        #    self.log(Logger.INFO, self.id, d, l)
        if self._get_logger().tracelevel > int(Logger.DUMP):
            t = ('>'*5+' job dump: ').upper()+self.id+' '+'>'*5
            print '>' * len(t)
            print t
            pprint (self.__dict__)
            t = ('<'*5+' job dump: ').upper()+self.id+' '+'<'*5
            print t
            print '<' * len(t)
        if self._user_defined_pre_hook is not None:
            self._user_defined_pre_hook(self)
        pass
    def post_hook(self, state):
        """
        remember the result state, then ParallelJobBlock could track the children
        results and make reaction (decide state)
        """
        self.state = state
        if self._user_defined_post_hook is not None:
            self._user_defined_post_hook(self)

    @classmethod
    def is_pseudo_job(self, job_id):
        """ the job w/o content or plan is pseudo job. check the members in Job """
        return job_id in Job._PSEUDO_JOB
    @classmethod
    def set_global(self, key, value):
        """
        used in the Job planning
        for repeated references in the whole process (say, path root)
        if some global needed variable is only accessed once, then just use
        global variable
        """
        Job.GLOBALS[key] = value
    @classmethod
    def get_global(self, key):
        """  """
        return Job.GLOBALS.get(key)
    @classmethod
    def encode_plan_key(self, job_id, state):
        """
        generate a dictionary key for plan decision
        currently we set "<state>@<job_id>" as the key of plan dictionary
        [USAGE] Job.encode_plan_key("foo job", Jobs.FAILED)
        """
        return "%s@%s" % (state, job_id)
    @classmethod
    def decode_plan_key(self, plan_key):
        """
        extract (state, job_id) from plan_key
        """
        return plan_key.split('@')


    # private
    def _show_desc_comment(self, depth):
        ## CONTEXT OF DESCRIPTION
        block_indent = "\t"*(depth)
        for desc in self.desc.lstrip("\n").rstrip(" \n").split("\n"):
            desc = desc.strip(" ")
            msg = "%s  %s" % (block_indent, desc)
            self.log(Logger.INFO, msg)
        ## CONTEXT OF DESCRIPTION

    """
    any delegatee should implement the followeing interface:
    set_dry_run(is_dry_run), set_logger(logger)
    """
    def _set_delegatees(self, delegatees):
        self.delegatees = delegatees
    def _get_delegatees(self):
        if self.delegatees is None:
            return Job.DELEGATEES
        else:
            return self.delegatees
    def _inherit_delegatees(self, source_job):
        self.delegatees = source_job.delegatees
    def _refresh_delegatees(self):
        delegatees = self._get_delegatees()
        #delegatees = Job.DELEGATEES
        for delegatee in delegatees.values():
            delegatee.set_dry_run(self.is_dry_run())
            delegatee.set_logger(self._get_logger())

    """
    about the logger:
    in normal cases, after assign a logger in the top level Job, you don't need
    to manipulate it anymore - all the job delegate the log task to a singleton
    logger (so the default member will remain None. however, things changed when
    we introduce threaded Job. we need to replicate logger for each ParallelJob,
    then collect the history in each logger after all the thread are done. and
    finally dump them sequentially. at this time, we need assign new logger into
    individual Job as class member. then the log task is no longer delegated to
    the singleton.
    """
    def _is_logger_valid(self):
        """
        because the actual functionality is delegated to the Logger class.
        and we need to assign the file path of output log to the Logger instance;
        then assign the Logger instance to the Job.
        checking is needed before using.
        """
        if Job.LOGGER is None:
            print "[WARN] log failed, please set Job.LOGGER = Logger(path, [open_mode])"
            return False
        else:
            return True
    def _get_logger(self):
        if self.logger is None:
            return Job.LOGGER
        else:
            return self.logger
    def _set_logger(self, logger):
        self.logger = logger
    def _inherit_logger(self, source_job):
        self.logger = source_job.logger
    """
    about the config:
    [usage]
    config is used inside the Job. user should only aware of input/output which
    utilizes the power of configs

    [inherit]
    configs are tightly relied on inherit to ease the suffer of Job design.
    (think of keeping assigning a path root to each Job) we provide two config
    containers, singleton of Job class and local member variable in each Job instance.

    common things like path root, "enabler" should be stored once in globally;
    per job-dependent things like offset, directory name should be stored locally.

    """
    def _set_config(self, key, value, set_as_local=False):
        """ the default setting container is global one (singleton). """
        if set_as_local:
            self.config[key] = value
        else:
            Job.set_global(key, value)
    def _get_config(self, key):
        """
        local container priors to the global one (i.e. check local first,
        if not found, then check global)
        """
        value = self.config.get(key)
        if value is None:
            value = Job.GLOBALS.get(key)
        return value
    def _get_completed(self, stack, is_soft=False):
        if isinstance(stack, str):
            # skip checking, start to process
            pass
        elif isinstance(stack, int): return stack
        elif isinstance(stack, list):
            comp_stack = map(lambda sub: self._get_completed(sub, is_soft=is_soft), stack)
            return comp_stack
        elif isinstance(stack, dict):
            comp_stack = self._complete_config(stack, is_soft=is_soft)
            return comp_stack
        else: return stack

        complete_stack = stack
        searches = re.findall(r'\[(.+?)\]', stack)
        for search in searches:
            replace = self._get_config(search)
            if replace is None and not is_soft:
                self.log(Logger.ERRO, 'config completion failed')
                self.log(Logger.ERRO,
                    'no matched item to complete the config, %s' % (stack))
                self.log(Logger.EXIT, self.id)
                exit(1)                                         # may exit from here
            if replace is None and is_soft:
                pass
            else:
                if complete_stack == ('[%s]' % (search)):
                    complete_stack = replace
                else:
                    try:
                        complete_stack = complete_stack.replace('[%s]' % (search), str(replace))
                    except UnicodeEncodeError:
                        complete_stack = complete_stack.replace('[%s]' % (search), str(replace.encode('utf8')))
        return complete_stack

    def _complete_config(self, config=None, is_soft=True):
        """
        [side-effective function]
        {some_key: some_value_w_[var_id], var_id: foo}
        -> some_key: some_value_w_foo, var_id: foo}
        variable is allowed in the config value with the form of [var_name]
        search the existed config key equals to the var_name and replace the var
        by the corresponding value
        """

        if config is None:
            config = self.config
        complete_config = config
        for (k , v) in config.items():
            if k.startswith('[') and k.endswith(']'):
                self.log(Logger.ERRO, 'config key, %s, could not be a variable.' % k)
                self.log(Logger.ERRO, 'partial is allowed')
                self.log(Logger.EXIT, self.id)
                exit(1)                                         # may exit from here
            comp_k = self._get_completed(k)
            comp_v = self._get_completed(v, is_soft=is_soft)

            complete_config[comp_k] = comp_v
            if comp_k != k:
                del complete_config[k]
        return complete_config
    def _inherit_config(self, source_job):
        """
        job could be organized into tree structure and encapisulated into bigger
        block. information may needed to be shared in the block. so we apply
        config inherit just before execution to keep the parent config fresh.
        """
        for k, v in source_job.config.items():
            # skip the global configuration item if it's already set in local
            # inherit it, if not
            if self.config.get(k) is not None:
                continue
            self._set_config(k, v, set_as_local=True)

    # public
    def p(self, *argv):
        """ print one line by delegated Logger instance """
        if self._is_logger_valid():
            self._get_logger().p(*argv)
    def log(self, trace_level, *list0):
        """ log one line by delegated Logger instance """
        if self._is_logger_valid():
            self._get_logger().log(trace_level, *list0)
    def log_float(self, trace_level, *list0):
        """ log one line w/o newline character by delegated Logger instance """
        if self._is_logger_valid():
            self._get_logger().log_float(trace_level, *list0)
    def set_dry_run(self, is_dry_run, set_as_local=False):
        self._set_config('is_dry_run', is_dry_run, set_as_local)
        #self.is_dry_run = is_dry_run
    def is_dry_run(self):
        """
        we promote this as a method beacuse we want to do late bindign
        (i.e. get the most fresh setting before execution)
        """
        return self._get_config('is_dry_run')
    def set_delegatee(self, id, delegatee):
        #Job.DELEGATEES[id] = delegatee
        delegatees = self._get_delegatees()
        delegatees[id] = delegatee
    def get_delegatee(self, id):
        #delegatee = Job.DELEGATEES.get(id)
        #delegatee.clear()
        delegatees = self._get_delegatees()
        delegatee = delegatees.get(id)
        delegatee.clear()
        return delegatee

    """
    about input/output

    to force interface be more clear, we ask user to explicitly assign inputs and
    output while they design the job. (then we could apply document generation and
    flow validation.) input should be bound with value, otherwise, it indicates the
    value comes from global configs or output by previous jobs.

    output does not only represent the item you need, but also brings the information
    into job callbacker, say a destination output file path. we apply the strategy
    to decouple the data and the logic, and, again, let the code be more self-
    documented and traceable,

    we utilized the power of config mechanism to store input/output information
    """
    def need_input(self, key, value=None):
        """
        explicitly assign the input items for job.
        the object will also maintain the input items for documentation
        and verification before execution
        """
        if value is not None:
            self._set_config(key, value, set_as_local=True)
        self.inputs[key] = value
    def get_input(self, key):
        """
        get the assigned the input value by corresponding key.
        this will search local first, then global configs
        use this in the callbacker
        """
        return self._get_config(key)
    def set_input(self, key, value):
        """
        store the job result globally for further using
        use this in the callbacker
        """
        self._set_config(key, value, set_as_local=False)
    def need_output(self, key, value=None):
        """
        explicitly assign the output items in job when planning to make interface
        more clear.
        the object will also maintain the input items for documentation
        and verification
        """
        if value is not None:
            self._set_config(key, value, set_as_local=True)
        self.outputs[key] = value
    def get_output(self, key):
        """
        get the assigned the output value by corresponding key.
        this will search local first, then global configs
        """
        return self._get_config(key)
    def set_output(self, key, value):
        """
        store the job result globally for further using
        use this in the callbacker
        """
        self._set_config(key, value, set_as_local=False)
    def pickup_inputs(self, keys):
        """
        pickup_inputs(keys) -> {k1: v1, k2: v2, ...}
        get list of values from the config by the given keys
        and pack them into a dictionary with the given keys
        use this in the callbacker
        """
        result = {}
        for k in keys:
            result[k] = self._get_config(k)
        return result
    def pickup_outputs(self, keys):
        """
        pickup_outputs(keys) -> {k1: v1, k2: v2, ...}
        get list of values from the config by the given keys
        and pack them into a dictionary with the given keys
        use this in the callbacker
        """
        result = {}
        for k in keys:
            result[k] = self._get_config(k)
        return result

    # interface
    def _show_plan(self, depth, symbol_table):
        raise NotImplementedError('_show_plan should be implemented for Job object')
    def _show_desc(self, depth, symbol_table):
        raise NotImplementedError('_show_desc should be implemented for Job object')
    def _is_valid(self):
        raise NotImplementedError('_is_valid should be implemented for Job object')
    def execute(self):
        raise NotImplementedError('execute should be implemented for Job object')


    def _get_symbol(self, lookup_table, key):
        if self.is_pseudo_job(key):
            return key

        if lookup_table.get(key) is None:
            if len(lookup_table) == 0:
                symbol = 1
            else:
                symbol = max(lookup_table.values()) + 1
        else:
            symbol = lookup_table[key]
        lookup_table[key] = symbol
        return symbol

    def set_pre_hook(self, callback):
        """
        any function matches with the following structure
        def exmple_callback (self):
            pass
        could be set as a callback
        """
        self._user_defined_pre_hook = callback
    def set_post_hook(self, callback):
        """
        any function matches with the following structure
        def exmple_callback (self):
            pass
        could be set as a callback
        """
        self._user_defined_post_hook = callback

"""
JobNode is a concrete job with a specified callback method
for executing the actual commands
"""
class JobNode(Job):
    def __init__(self, id, desc):
        super(JobNode, self).__init__(id, desc)
        self.plannable = False
        self.is_ready = False
        self._user_defined_method = None
    def __deepcopy__(self, memo):
        j = JobNode(self.id, self.desc)
        j.plannable = copy.deepcopy(self.plannable)
        j.inputs = copy.deepcopy(self.inputs)
        j.outputs = copy.deepcopy(self.outputs)
        j.config = copy.deepcopy(self.config)
        j.logger = copy.deepcopy(self.logger)

        j.is_ready = copy.deepcopy(self.is_ready)
        j._user_defined_method = self._user_defined_method
        return j
    # private
    def _callback(self):
        """
        every JobNode must override this method
        this is the place to tigger UDM
        """
        return self._user_defined_method(self)

    # public
    def set_callback(self, callback):
        """
        any function matches with the following structure
        def exmple_callback (self):
            pass
        could be set as a callback
        """
        self.is_ready = True
        #self._callback = instancemethod(callback, self, JobNode)
        self._user_defined_method = callback

        return self

    # override
    def _show_plan_(self, depth=0):
        block_indent = "\t"*depth

        # show node
        msg = "%s%s" % (block_indent, self.id)
        self.log(Logger.INFO, msg)
        return
    def _show_desc(self, depth, symbol_table={}):
        block_indent = "\t"*depth

        # show node id
        symbol = self._get_symbol(symbol_table, self.id)
        msg = "%s%s.%s" % (block_indent, symbol, self.id.upper()) # MARK
        self.log(Logger.INFO, msg)

        # show node profile in multi-lines
        self._show_desc_comment(depth)

        ## CONTEXT OF DESCRIPTION
        io = {'input': self.inputs.items(), 'output': self.outputs.items()}
        for io_type, items in io.items():
            items.sort(key=lambda l: l[0])
            for key, value in items:
                assignment_type = ''
                if value is None:
                    value_ = self._get_config(key)
                    if value_ is None:
                        value_ = ''
                        assignment_type = '(runtime assignment)'
                    else:
                        assignment_type = ' (global setting)'
                else:
                    value_ = value
                key = self._get_completed(key,)
                value_ = self._get_completed(value_, is_soft=True)
                msg = "%s  * %s - %s := %s%s" % (block_indent, io_type,
                                              key, value_, assignment_type)
                self.log(Logger.INFO, msg)
         ## CONTEXT OF DESCRIPTION
        return True
    def _is_valid(self):
        """
        check whether the execution content is implemented in the job.
        1. user_defined_method (UDM) should be override
        2. UDMshould follow the practice
           - must return at least one Job state.
           - all planned inputs (key in need_input) should be used in the UDM
             (get_input). vice versa
        """
        is_valid = True
        if not self.is_ready:
            msg = "'%s' is not executable (overriding is needed)" % (self.id)
            self.log(Logger.ERRO, msg)
            is_valid = False
        if self._user_defined_method.__class__.__name__ != 'function':
            msg = "callback method, %s, is not a function" % \
                (self._user_defined_method)
            self.log(Logger.ERRO, msg)
            return False
        source = inspect.getsource(self._user_defined_method)
        if 'return' not in source:
            msg = "'return' is not found in '%s' (not properly exit) " % (self.id)
            self.log(Logger.ERRO, msg)
            is_valid = False

        # check whether the planned inputs match with used inputs

        # part1. extract inputs from
        #   self.pickup_outputs('used_inputs_1', "used_inputs_2",)
        used_inputs_ = []
        used_input_group = re.findall(r'self\.pickup_inputs\(\[(.+?)\]\)', source.replace("\n",""))
        if len(used_input_group) > 0:
            used_input_group = used_input_group.pop()
            used_inputs_ = used_input_group.split(',')
            used_inputs_ = map(lambda s: s.strip(r" '\""), used_inputs_)
            used_inputs_ = filter(lambda s: s != '', used_inputs_)

        # part2. extract inputs from
        # self.get_input('key_1', "key_2", )
        used_inputs = re.findall(r'self\.get_input\((.+?)\)', source.replace("\n",""))
        used_inputs = map(lambda s: s.strip(r"'\""), used_inputs)
        used_inputs = filter(lambda s: s not in Job._BLACK_INPUTS, used_inputs)

        # merge part1 and part2
        used_inputs += used_inputs_

        planned_inputs = self.inputs.keys()
        planned_inputs = filter(lambda s: s not in Job._BLACK_INPUTS, planned_inputs)

        if 0 == len(used_inputs) and 0 == len(planned_inputs):
            msg = "no input in %s" % (self.id)
            self.log(Logger.WARN, msg)
        elif 0 == len(used_inputs):
            msg = "all planned inputs are not use in %s's callbacker" % (self.id)
            self.log(Logger.ERRO, msg)
            return False
        elif 0 == len(planned_inputs):
            msg = "all inputs are not planned for %s" % (self.id)
            self.log(Logger.ERRO, msg)
            return False

        # exame whether un-planned input exists
        for used_input in used_inputs:
            if used_input not in planned_inputs:
                if self._get_config(used_input) is None:
                    method_name = self._user_defined_method.__name__
                    msg = "required input, '%s', used in %s is not define:" \
                        % (used_input, method_name)
                    self.log(Logger.ERRO, msg)
                    msg = "\tcheck the plans of %s" % (self.id)
                    self.log(Logger.ERRO, msg)
                    msg = "\tplanned inputs: %s" % (self.inputs)
                    self.log(Logger.ERRO, msg)
                    is_valid = False
        for planned_input in planned_inputs:
            if planned_input not in used_inputs:
                # some configs are prepared for replace other variablized config
                # they could be not appear in the planned inputs

                # take a boolean survey on all the other planned inputs
                # to see whether this planned input has the replacing purpose
                is_planned_input_for_variablized_configs = \
                    map(lambda v:
                        "[%s]" % (planned_input) in str(v),
                        filter(lambda val: val is not None, self.inputs.values())
                        )
                if 0 == len(is_planned_input_for_variablized_configs):
                    is_input_for_config_var = False
                else:
                    is_input_for_config_var = reduce(
                        lambda for_conf1, for_conf2: for_conf1 or for_conf2,
                        is_planned_input_for_variablized_configs, False
                    )
                # we only check the config w/o replacing purpose
                if not is_input_for_config_var:
                    msg = "planned input, '%s', is not use in %s" % (planned_input,
                                                                     self.id)
                    self.log(Logger.WARN, msg)
                    method_name = self._user_defined_method.__name__
                    msg = "\tcheck the callback %s" % (method_name)
                    self.log(Logger.WARN, msg)
                    msg = "\tused inputs: %s" % (used_inputs)
                    self.log(Logger.WARN, msg)
                    is_valid = False

        return is_valid
    def execute(self, is_init_job=True):
        """ the actual execute "flow" """
        self.pre_hook()
        is_dry = self.is_dry_run()
        if is_dry: prefix = '(dry run) '
        else: prefix  = ''
        self.log(Logger.INFO_SUBHEAD_S, "%s%s" % (prefix, self.id))
        state = self._callback()
        self.log(Logger.INFO_SUBHEAD_E, "[%s] %s%s" % (state, prefix, self.id))
        self.post_hook(state)
        return self.id, state

"""
JobBlock contains a set of JobNode with a state machine.
Each JobNode is a state, with a certain execution result,
a corresponding next job is recorded.

We implement a dictionary, plan, as the state machine.
The key is the combination of previous job id and the execution result. (*1)
And the value is the id of the next job
"""
class JobBlock(Job):
    def __init__(self, id, desc):
        super(JobBlock, self).__init__(id, desc)
        self.jobs = {}
        self.plan = {}
        self.plannable = True
        self.graph_filepath = None
        self.is_mute_health_check = False
    def __deepcopy__(self, memo):
        j = JobBlock(self.id, self.desc)
        j.plannable = copy.deepcopy(self.plannable)
        j.inputs = copy.deepcopy(self.inputs)
        j.outputs = copy.deepcopy(self.outputs)
        j.config = copy.deepcopy(self.config)
        j.logger = copy.deepcopy(self.logger)

        j.jobs= copy.deepcopy(self.jobs)
        j.plan= copy.deepcopy(self.plan)
        j.graph_filepath = copy.deepcopy(self.graph_filepath)
        return j
    # private
    def _get_next(self, prev_job_id, state):
        """
        get next job id by previous job id and its executing result
        multiple results is possible in ParallelJobBlock
        e.g. ParaJ -> [SubJ1, SubJ2]
        """
        plan_key = Job.encode_plan_key(prev_job_id, state)
        job_id = self.plan.get(plan_key)
        return job_id
    def _get_neighbors(self, target_job_id):
        """
        get a list of all the possible state and corresponding next job id
        by prev_job_id.
        return a list of {'state': '', 'next_job_id': ''}
        mainly used for self._show_plan
        """
        plans = []
        for plan_key, next_job_id in self.plan.items():
            state, job_id = Job.decode_plan_key(plan_key)
            if job_id == target_job_id:
                plans.append({'state': state, 'next_job_id': next_job_id})
        return plans
    def _get_sorted_children(self):
        """
        get all children job id in the planning order (the original form is dict)
        this method contains tricks:
        1. record all the non-skipped path will get complete list
           so we could skip pair with SKIP state
        """
        # convert plan to lookup table
        plans = {}
        for plan_key, to_job in self.plan.items():
            state, from_job = Job.decode_plan_key(plan_key)
            if Job.SKIP == state: continue                             #continue
            if not plans.has_key(from_job):
                plans[from_job] = []
            plans[from_job].append(to_job)

        # fill job list in sequence
        sorted_plans = []
        from_job = Job.INIT_JOB
        from_job_history = {} # record for loop detection
        is_reach_end = False

        from_job = plans[from_job][0]
        #print from_job
        unvisited_jobs = self.jobs.keys()
        #print unvisited_jobs
        def visit(from_job):
            if from_job in unvisited_jobs:
                unvisited_jobs.remove(from_job)
                sorted_plans.append(from_job)
                if plans.get(from_job) is None:
                    # node may exit to other job which is not in this block
                    pass
                else:
                    to_jobs = plans[from_job]
                    for to_job in to_jobs:
                        visit(to_job)

        visit(from_job)
        #print '<<<<<<<<<<'
        #print self.id
        #pprint(sorted_plans)
        #print '>>>>>>>>>>'
        #raw_input()
        '''
        while(1):
            from_job_history[from_job] = True

            to_jobs = plans[from_job]

            next_job = None
            print '[from]', from_job, '[tos]', to_jobs
            to_job_taceback = [] # job w/ multiple to may have EXIT dead end
            for to_job in to_jobs:
                print ' [to]', to_job
                # escap from loop
                if from_job_history.get(to_job):
                    new_to_job = self._get_next(to_job, Job.LOOP_DONE)
                    if new_to_job is None:
                        self.log(Logger.ERRO,
                                 'you need to prepare a route: %s @ %s -> somewhere' % \
                                    (Job.LOOP_DONE, to_job)
                            )
                        exit(1)
                    to_job = new_to_job


                if Job.LAST_JOB == to_job:
                    is_reach_end = True
                    break                                                 #break
                elif Job.is_pseudo_job(to_job):
                    # currently, it's just EXIT
                    continue                                           #continue
                else:
                    sorted_plans.append(to_job)
                    next_job = to_job
            if is_reach_end: break                                        #break

            #if next_job is None:
            #    self.log(Logger.ERRO, 'can not find next job.')
            #    self.log(Logger.ERRO, 'from %s to %s.' % (from_job, to_jobs))
            #    exit(1)
            print '[from]',from_job, '[tos]', to_jobs, '[plan]', sorted_plans
            from_job = next_job
        '''
        return sorted_plans

    def _normalize_edge(self, from_job_id, to_job_id):
        """
        normalize the id of given Job nodes, including
        converting spaces to underscores
        differentiate the pseudo job name (by adding job id as prefix)
        """
        self_id = self.id.replace(' ', '_')

        norm_ids = []
        for id in [from_job_id, to_job_id]:
            id = id.replace(' ','_')
            # make INIT_JOB and LAST_JOB be unique (concatenate to job id)
            if id == Job.INIT_JOB or id == Job.LAST_JOB:
                norm_id = '%s_%s' % (self_id, id)
            else:
                norm_id = id
            norm_ids.append(norm_id)
        return norm_ids
    def _get_plan_edges(self, depth=0):
        """
        recursively parse the job plan
        and record the unique flow edges (pair-wise job node)
        this method is the body of recursive edge collection.
        for this collection task, sequence is not important; just make sure we
        could go through all the plan edge.
        """
        edges = []
        if 0 != depth:
            from_job_id_, to_job_id_ = self._normalize_edge(Job.INIT_JOB, self.id)
            edges.append([to_job_id_, from_job_id_, '->START'])
        for plan_key, to_job_id in self.plan.items():
            # get edges for this JobBlock
            state, from_job_id = Job.decode_plan_key(plan_key)
            from_job_id_, to_job_id_ = self._normalize_edge(from_job_id, to_job_id)
            edge = [from_job_id_, to_job_id_, state]
            edges.append(edge)

            # bridge the gap of self.id -> self.id_START and
            #                   self.id_END -> seld.id
            if from_job_id == Job.INIT_JOB:
                edge = [self.id, from_job_id_, '->START']
                edges.append(edge)
            if to_job_id == Job.LAST_JOB and 0 != depth:
                edge = [to_job_id_, self.id, '->END']
                edges.append(edge)

            # get edges for inner JobBlock
            if not Job.is_pseudo_job(from_job_id):
                job = self.get_job(from_job_id)
                if (job.plannable):
                    sub_edges = job._get_plan_edges(depth+1)
                    edges += sub_edges
        if 0 != depth:
            from_job_id_, to_job_id_ = self._normalize_edge(self.id, Job.LAST_JOB)
            edges.append([to_job_id_, from_job_id_, '->END'])
        return edges

    @deprecated
    def _show_flow_chart(self):
        # deprecated. due to the limited length of GET parameter
        """
        this method is the entry point to
        collect the edges in the state machine for Google chart rendering
        """
        edges = self._get_plan_edges()
        edges = map(lambda e: "%s--%s" % (list(e)[0], list(e)[1]) , edges)
        url = "https://chart.googleapis.com/chart?cht=gv&chl=graph{%s}" % (
            ';'.join(edges)
        )
        self.log(Logger.INFO, url)
    def _show_graph(self, filepath):
        """
        this method is the entry point to
        collect the edges in the state machine for networkx graph rendering
        """
        edges = self._get_plan_edges()
        g = graph.NetGraph(filepath)
        if g.is_graph_lib_available:
            for edge in edges:
                u = edge[0]
                v = edge[1]
                if len(edge) > 2:
                    m = edge[2]
                else:
                    m = ''

                if m == Job.DONE: weight = 5
                else: weight = 1

                if m == Job.ERROR: edge_color='red'
                elif m == Job.SKIP: edge_color='grey'
                elif m.startswith('FORK_'): edge_color='purple'
                elif m.startswith('END_'): edge_color='purple'
                elif m == '->START':  edge_color='green'
                elif m == '->END':  edge_color='blue'
                else: edge_color='black'


                def get_node_color(n):
                    if n.endswith('_%s' % Job.INIT_JOB):
                        return 'green'
                    elif n.endswith('_%s' % Job.LAST_JOB):
                        return 'blue'
                    elif n == Job.EXIT_JOB:
                        return 'red'
                    else:
                        return 'black'

                from_color = get_node_color(u)
                to_color = get_node_color(v)

                meta = {
                    'msg': m,
                    'from_color': from_color,
                    'to_color': to_color,
                    'edge_color': edge_color,
                    'weight': weight ,
                }
                g.add_edge(u, v, meta)
            g.show()
        else:
            print graph.GRAPH_LIB_NOT_FOUND
        self.log(Logger.INFO, '')
    def _health_check(self):
        """ check and visualize the job plan before execution """
        self.log(Logger.INFO, "")
        self.log(Logger.INFO_SUBHEAD_S, "plan validation")
        if not self._is_valid():
            msg = "the plan is not valid"
            self.log(Logger.ERRO, msg)
            self.log(Logger.EXIT, self.id)
            exit(1)                                         # may exit from here
        self.log(Logger.INFO_SUBHEAD_E, "plan validation")
        self.log(Logger.INFO, "")
        if self.interactive: raw_input()

        symbol_table = {}

        self.log(Logger.INFO_SUBHEAD_S, "plan illustration")
        self._show_plan_wrapper(0, symbol_table)
        self.log(Logger.INFO_SUBHEAD_E, "plan illustration")
        self.log(Logger.INFO, '')
        if self.interactive: raw_input()

        self.log(Logger.INFO_SUBHEAD_S, "Global configuration for all jobs")
        self._complete_config(Job.GLOBALS)
        gvars = Job.GLOBALS.items()
        gvars.sort(key=lambda l:l[0])
        if 0 == len(gvars): max_key_length = 0
        else: max_key_length = max(map(lambda item: len(item[0]), gvars))
        ptn = "[config] %"+str(max_key_length)+"s := %s"
        for input, value in gvars:
            msg = ptn % (input, value)
            self.log(Logger.INFO, msg)
        self.log(Logger.INFO_SUBHEAD_E, "Global configuration for all jobs")
        self.log(Logger.INFO, '')
        if self.interactive: raw_input()

        self.log(Logger.INFO_SUBHEAD_S, "description and input/output listing")
        self._show_desc(0, symbol_table)
        self.log(Logger.INFO_SUBHEAD_E, "description and input/output listing")
        self.log(Logger.INFO, "")
        if self.interactive: raw_input()

        # deprecated
        #self.log(Logger.INFO, "check the illustration of the flow: ")
        #self._show_flow_chart()


        if self.is_visualized:
            self.log(Logger.INFO, "chart is saved at "+self.graph_filepath+".")
            self._show_graph(self.graph_filepath)

    # public
    def get_job(self, job_id):
        """
        get job (JobNode or JobBlock) by id only its own job
        """
        return self.jobs.get(job_id)
    def find_job(self, target_job_id):
        """
        get job (JobNode or JobBlock) by id, recursively find from children
        """
        result = None
        for job_id, job in self.jobs.items():
            if job_id == target_job_id:
                result = job
                return result
            if not job.plannable: continue                            # continue
            result = job.find_job(target_job_id)
            if result is not None:
                return result
        return result
    def add_sub_job(self, job):
        """
        we add JobNode or JobBlock as sub-job into JobBlock
        all the sub-job are store in a dictionary with its id as key
        """
        job_id = job.id
        self.jobs[job_id] = job
    def add_plan(self, from_job_id, state, to_job_id):
        """
        we add a "path", which comprises of
            starting point(job),
            executing result and
            destination point(job),
        as one decision in the whole flow.
        multiple path starts from a same point is supported.
        """

        # dummy-prove
        if Job.INIT_JOB == from_job_id and state != Job.START:
            state = Job.START
            self.log(Logger.WARN,
                     "the first state of '%s' should be START" % (self.id))

        # calulate the key for storing the given plan
        plan_key = Job.encode_plan_key(from_job_id, state)

        # the destinations of a plan key could be existed in two type:
        # string or list of string
        if isinstance(self.plan.get(plan_key), str):
            self.plan[plan_key] = [self.plan[plan_key], to_job_id]
        elif isinstance(self.plan.get(plan_key), list):
            self.plan[plan_key].append(to_job_id)
        else:
            self.plan[plan_key] = to_job_id
    def set_visualization(self, is_visualized, filepath):
        """ output path for the graph of job plan """
        self.is_visualized = is_visualized
        self.graph_filepath = filepath
    def set_interactive(self, interactive):
        self.interactive = interactive
    def set_mute_health_check_msg(self, is_mute_health_check):
        self.is_mute_health_check = is_mute_health_check
    # override
    def _show_plan_(self, depth=0):
        """
        visualize the job flow in plain text.
        """
        block_indent = "\t"*depth
        plan_indent = '--'

        child_job_ids = self._get_sorted_children()
        child_jobs = map(lambda id: self.get_job(id), child_job_ids)

        # show INIT_JOB plan
        self.log(Logger.INFO, "%s%s" % (block_indent, Job.INIT_JOB))
        self.log(Logger.INFO, "%sif %s\t->\t%s" %
            (block_indent+plan_indent, Job.START, child_jobs[0].id)
        )

        # get Job plan
        # if JobNode, Done -> next_job_id
        # else,       START ->
        #                 if STATE  -> xxx
        #                 ...
        #             DONE  -> next_job_id
        for child_job in child_jobs:

            self.log(Logger.INFO, "%s%s" % (block_indent, child_job.id))

            if child_job.plannable:
                # children prefix --if START      ->
                self.log(Logger.INFO, "%sif %s\t->" % \
                    (block_indent+plan_indent, Job.START)
                )

            plans = self._get_neighbors(child_job.id)
            for plan in plans:
                state = plan['state']
                next_job_id = plan['next_job_id']
                msg = "%sif %s\t->\t%s" % \
                    (block_indent+plan_indent, state, next_job_id)
                if Job.DONE == state:
                    done_msg = msg
                else:
                    self.log(Logger.INFO, msg)

            if child_job.plannable:
                child_job._show_plan(depth+1)
                pass

            # children suffix  --if DONE       -> xxx
            self.log(Logger.INFO, done_msg)


        # show LAST_JOB plan (empty)
        self.log(Logger.INFO, "%s%s" % (block_indent, Job.LAST_JOB))
        return
    def _show_plan(self, depth, child_jobs, symbol_table):
        for child_job in child_jobs:
            if child_job.plannable:
                start_to_jobs = child_job.plan[self.encode_plan_key(Job.INIT_JOB, Job.START)]
                if isinstance(start_to_jobs, str): start_to_jobs = [start_to_jobs]
                start_plan = map(lambda to_job: {'state': 'START', 'next_job_id': to_job},
                    start_to_jobs)
                #print start_plan;raw_input()
                serialized_plan = map(
                    lambda plan:
                    "%s:%s" % (plan['state'], self._get_symbol(symbol_table, plan['next_job_id'])),
                    self._get_neighbors(child_job.id) + start_plan
                )
                serialized_plan = ', '.join(serialized_plan)

                symbol = self._get_symbol(symbol_table, child_job.id)
                msg = "%s%s.[%s] -> %s" % ("\t"*(depth+1), symbol, child_job.id, serialized_plan) # MARK
                self.log(Logger.INFO, msg)

                child_job._show_plan_wrapper(depth+1, symbol_table)
            else:
                symbol = self._get_symbol(symbol_table, child_job.id)
                serialized_plan = map(
                    lambda plan:
                    "%s:%s" % (plan['state'], self._get_symbol(symbol_table, plan['next_job_id'])),
                    self._get_neighbors(child_job.id)
                )
                serialized_plan = ', '.join(serialized_plan)
                msg = "%s%s.%s -> %s" % ("\t"*(depth+1), symbol, child_job.id, serialized_plan) # MARK
                self.log(Logger.INFO, msg)
    def _show_plan_wrapper(self, depth=0, symbol_table={}):
        """
        list the detail description including input/output for the plan.
        it traverse all the job (no pseudo job) and print profile;
        recursively handle the children if block job is found
        """

        block_indent = "\t"*depth

        if depth == 0:
            # show node id
            symbol = self._get_symbol(symbol_table, self.id)
            msg = "%s%s.[%s]" % (block_indent, symbol, self.id) # MARK
            self.log(Logger.INFO, msg)

        # show plan of children nodes
        child_job_ids = self._get_sorted_children()
        child_jobs = map(lambda id: self.get_job(id), child_job_ids)
        self._show_plan(depth, child_jobs, symbol_table)

        return
    def _show_desc(self, depth=0, symbol_table={}):
        """
        list the detail description including input/output for the plan.
        it traverse all the job (no pseudo job) and print profile;
        recursively handle the children if block job is found
        """
        block_indent = "\t"*depth

        # show node id
        symbol = self._get_symbol(symbol_table, self.id)
        msg = "%s%s.[%s]" % (block_indent, symbol, self.id.upper()) # MARK
        self.log(Logger.INFO, msg)

        # show node profile in multi-lines
        self._show_desc_comment(depth)

        # show plan of children nodes
        child_job_ids = self._get_sorted_children()
        #print child_job_ids ; raw_input()
        child_jobs = map(lambda id: self.get_job(id), child_job_ids)
        ##child_jobs = self.jobs.values()
        for child_job in child_jobs:
            child_job._inherit_config(self); child_job._complete_config()
            child_job._show_desc(depth+1, symbol_table)

        return
    def _is_valid(self):
        """
        before starting the whole process, rather than during the runtime,
        we want to verify the job plan to check whether all the job id is
        correctly registered with a job instance. (for JobBlock)
        and whether it is correctly overrided. (for JobNode)
        _is_valid is a common interface of JobNode and JobBlock.

        this method scan the member variable, plan, without specific sequence;
        a list, checked_list, will record the checked jobs to prevent duplicated
        checking
        """
        checked_list = [] # prevent duplicated checking
        is_process_valid = True

        max_len = 50

        # check whether plan is properly set
        if 0 == len(self.plan):
            # empty plan, give warning
            self.log(Logger.INFO, "%s%s[%s]" % (self.id,
                                                ' '*(max_len-len(self.id)),
                                                'x'))
            mgs = "no plan found in '%s'" % (self.id)
            self.log(Logger.ERRO, mgs)
            is_process_valid = False
        else:
            # check whether Job.LAST is set
            to_job_list = self.plan.values()
            if not Job.LAST_JOB in to_job_list:
                self.log(Logger.INFO, "%s%s[%s]" % (self.id,
                                                       ' '*(max_len-len(self.id)),
                                                        'x'))
                mgs = "at least one Job.LAST_JOB should be set in '%s' " % (self.id)
                self.log(Logger.ERRO, mgs)
                is_process_valid = False


        for plan_key, to_job_id in self.plan.items():
            state, from_job_id = Job.decode_plan_key(plan_key)

            if isinstance(to_job_id, list):
                self.log(Logger.INFO, "%s%s[%s]" % (self.id,
                                                   ' '*(max_len-len(self.id)),
                                                    'x'))
                mgs = "multiple destinations is not allowed here"
                self.log(Logger.ERRO, mgs)
                mgs = "\t%s -> %s" % (from_job_id, to_job_id)
                self.log(Logger.ERRO, mgs)
                is_process_valid = False
                continue                                        # continue point


            for job_id in [from_job_id, to_job_id]:
                # skip the pseudo job
                if Job.is_pseudo_job(job_id): continue          # continue point
                # skip checked job
                if job_id in checked_list: continue

                checked_list.append(job_id)

                # check self-loop plan
                if job_id == self.id:
                    self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                       ' '*(max_len-len(job_id)),
                                                        'x'))
                    mgs = "self-loop found in '%s'" % (job_id)
                    self.log(Logger.ERRO, mgs)
                    is_process_valid = False
                    continue                                    # continue point

                job = self.get_job(job_id)

                # check whether job is registered
                if job is None:
                    self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                       ' '*(max_len-len(job_id)),
                                                        'x'))
                    mgs = "'%s' is not registered" % (job_id)
                    self.log(Logger.ERRO, mgs)

                    # give recommendation
                    distances = map(lambda j: [cliff(job_id, j),j] , self.jobs.keys())
                    distances.sort(cmp=None, key=None, reverse=True)
                    try:
                        most_similar = distances.pop()[1]
                        msg = "'%s', do you mean it?" % (most_similar)
                        self.log(Logger.ERRO, msg)
                    except IndexError:
                        msg = "no Job registered in %s" % (self.id)
                        self.log(Logger.ERRO, msg)

                    is_process_valid = False
                    continue                                    # continue point


                # check whether job is workable:
                # for JobNode, callback overriding is needed
                # for JobBlock, recursively call its _is_valid method
                job._inherit_config(self)
                job._complete_config()
                job._inherit_logger(self)

                is_valid = job._is_valid()
                if is_valid:
                    self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                       ' '*(max_len-len(job_id)),
                                                        'o'))
                else:
                    self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                    ' '*(max_len-len(job_id)),
                                                    'x'))
                    mgs = "\terror(s) found in '%s' (JobBlock)" % (job_id)
                    self.log(Logger.ERRO, mgs)
                    is_process_valid = False
        return is_process_valid
    def execute(self, is_init_job=True):
        """
        start the state machine for triggering sub jobs (JobNode)
        """
        self.pre_hook()
        if is_init_job:
            if self.is_mute_health_check:
                self._get_logger().force_stdout_set(False)
                self._get_logger().force_fileout_set(False)
            self.log(Logger.INFO_HEAD, 'start plan health-checking')
            self._health_check()
            self.log(Logger.INFO_HEAD, 'start plan execution')
            self.log(Logger.INFO, """
    _____  _______         _____  _______       _    _  ______  _____   ______
   / ____||__   __| /\    |  __ \|__   __|     | |  | ||  ____||  __ \ |  ____|
  | (___     | |   /  \   | |__) |  | |        | |__| || |__   | |__) || |__
   \___ \    | |  / /\ \  |  _  /   | |        |  __  ||  __|  |  _  / |  __|
   ____) |   | | / ____ \ | | \ \   | |        | |  | || |____ | | \ \ | |____
  |_____/    |_|/_/    \_\|_|  \_\  |_|        |_|  |_||______||_|  \_\|______|
""")

            if self.is_mute_health_check:
                self._get_logger().force_stdout_set(True)
                self._get_logger().force_fileout_set(True)
            self.log(Logger.INFO, "%s. log: %s" % (self.id, self._get_logger().fn))
        #return None, None # XXX open this for testing, remember to close it
        if self.interactive: raw_input()

        self.log(Logger.INFO_HEAD_S, "%s BLOCK" % self.id)

        prev_job = Job.INIT_JOB
        state = Job.START
        while(1):
            next_job = self._get_next(prev_job, state)

            if Job.LAST_JOB == next_job:
                break                                               # break here
            # ERROR state should be continue by plan logic
            # see comment of Job (iii)
            ##if Job.ERROR == state:##
            ##    break             ##

            # EXIT job should stop the loop
            if (Job.EXIT_JOB == next_job):
                # see comment of Job (iv)
                state = Job.TERMINATE
                break                                               # break here

            job = self.get_job(next_job)

            if job is None:
                self.log(Logger.ERRO, "can NOT find next job, '%s', in %s" %
                        (next_job, self.id))
                self.log(Logger.ERRO, "from: %s" % prev_job)
                self.log(Logger.ERRO, "state: %s" % state)
                self.log(Logger.EXIT, self.id)
                exit(1)                                         # may exit from here

            job._inherit_config(self)   # inherit the config
                                        # must be set before execution
            job._complete_config(is_soft=False)
            job._inherit_logger(self)

            job._inherit_delegatees(self)
            job._refresh_delegatees()

            prev_job, state = job.execute(is_init_job=False)
            if Job.TERMINATE == state:
                # see comment of Job (i)
                break


        self.log(Logger.INFO_HEAD_E, "[%s] %s BLOCK" % (state, self.id))
        self.log(Logger.INFO,'')
        self.log(Logger.INFO,'')

        self.post_hook(state)
        if is_init_job and Job.TERMINATE == state:
            # see comment of Job (ii)
            self.log(Logger.EXIT, '')
            exit(1)                                              # exit here
        return self.id, state

class ParallelJobBlock(JobBlock):
    def __init__(self, id, desc):
        super(ParallelJobBlock, self).__init__(id, desc)
    def __deepcopy__(self, memo):
        j = ParallelJobBlock(self.id, self.desc)
        j.plannable = copy.deepcopy(self.plannable)
        j.inputs = copy.deepcopy(self.inputs)
        j.outputs = copy.deepcopy(self.outputs)
        j.config = copy.deepcopy(self.config)
        j.logger = copy.deepcopy(self.logger)

        j.jobs= copy.deepcopy(self.jobs)
        j.plan= copy.deepcopy(self.plan)
        j.graph_filepath = copy.deepcopy(self.graph_filepath)
        return j
    # public
    def add_papallel_plan(self, *job_ids):
        """ add the given job ids in the plan in a parallel way """
        # create parallel job plan for documentation not for execution
        for job_id in job_ids:
            self.add_plan(Job.INIT_JOB, Job.START, job_id)
            self.add_plan(job_id, Job.DONE, Job.LAST_JOB)

    # override
    def _get_sorted_children(self):
        return self.jobs.keys()
    def _get_plan_edges(self, depth=0):
        """
        this method is the body of recursive edge collection.
        for this collection task, sequence is not important; just make sure we
        could go through all the plan edge.
        """

        prev_job = Job.INIT_JOB
        state = Job.START
        next_jobs = self._get_next(prev_job, state)
        edges = []

        i = 1
        from_job_id_, to_job_id_ = self._normalize_edge(Job.INIT_JOB, self.id)
        edges.append([to_job_id_, from_job_id_, '->START'])
        for job_id in next_jobs:
            edges.append(self._normalize_edge(Job.INIT_JOB, job_id)+['FORK_'+str(i)])
            job = self.get_job(job_id)
            if (job.plannable):
                sub_edges = job._get_plan_edges(depth+1)
            edges += sub_edges
            edges.append(self._normalize_edge(job_id, Job.LAST_JOB)+['END_'+str(i)])
            i += 1
        from_job_id_, to_job_id_ = self._normalize_edge(self.id, Job.LAST_JOB)
        edges.append([to_job_id_, from_job_id_, '->END'])
        return edges
    def _show_plan_(self, depth=0):
        """
        this method is the entry point to
        traverse the graph of state machine
        """
        block_indent = "\t"*depth
        plan_indent = '--'

        for job in self.jobs.values():
            self.log(Logger.INFO, "%s%s" % (block_indent, job.id))

            # children prefix --if START      ->
            self.log(Logger.INFO, "%sif %s\t->" % \
                (block_indent+plan_indent, Job.START)
            )

            job._show_plan(depth+1)

            # children suffix  --if DONE       -> oxox
            self.log(Logger.INFO, "%sif %s\t->\t%s" %\
                (block_indent+plan_indent, Job.DONE, Job.LAST_JOB)
            )

            # show LAST_JOB plan (empty)
            self.log(Logger.INFO, "%s%s" % (block_indent, Job.LAST_JOB))
        return
    def _show_plan_wrapper(self, depth=0, symbol_table={}):
        block_indent = "\t"*depth

        if depth == 0:
            # show node id
            symbol = self._get_symbol(symbol_table, self.id)
            msg = "%s%s[%s]" % (block_indent, symbol, self.id) # MARK
            self.log(Logger.INFO, msg)

        prev_job = Job.INIT_JOB
        state = Job.START
        next_job_ids = self._get_next(prev_job, state)
        next_jobs = map(lambda id: self.get_job(id),next_job_ids)
        self._show_plan(depth, next_jobs, symbol_table)

        return
    def _show_desc(self, depth=0, symbol_table={}):
        block_indent = "\t"*depth

        # show node id
        symbol = self._get_symbol(symbol_table, self.id)
        msg = "%s%s.[%s]" % (block_indent, symbol, self.id.upper())
        self.log(Logger.INFO, msg)

        # show node profile in multi-lines
        self._show_desc_comment(depth)

        prev_job = Job.INIT_JOB
        state = Job.START
        next_job_ids = self._get_next(prev_job, state)
        next_jobs = map(lambda id: self.get_job(id),next_job_ids)
        for job in next_jobs:
            job._inherit_config(self)
            job._complete_config()

            job._show_desc(depth+1, symbol_table)

        return
    def _is_valid(self):
        """
        similar to the method of JobBlock, the difference is that the initial
        job to be checked.
        """
        is_process_valid = True

        max_len = 50

        # check whether plan is properly set
        if 0 == len(self.plan):
            self.log(Logger.INFO, "%s%s[%s]" % (self.id,
                                                ' '*(max_len-len(self.id)),
                                                'x'))
            mgs = "no plan found in '%s'" % (self.id)
            self.log(Logger.ERRO, mgs)
            is_process_valid = False

        # TODO re factor this common logic with different logic
        prev_job = Job.INIT_JOB
        state = Job.START
        next_jobs = self._get_next(prev_job, state)
        for job_id in next_jobs:

            # check self-loop plan
            if job_id == self.id:
                self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                   ' '*(max_len-len(job_id)),
                                                    'x'))
                mgs = "self-loop found in '%s'" % (job_id)
                self.log(Logger.ERRO, mgs)
                is_process_valid = False
                continue                                        # continue point

            job = self.get_job(job_id)

            # check whether job is registered
            if job is None:
                self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                   ' '*(max_len-len(job_id)),
                                                    'x'))
                mgs = "'%s' is not registered" % (job_id)
                self.log(Logger.ERRO, mgs)

                # give recommendation
                distances = map(lambda j: [cliff(job_id, j),j] , self.jobs.keys())
                distances.sort(cmp=None, key=None, reverse=True)
                most_similar = distances.pop()[1]
                msg = "'%s', do you mean it?" % (most_similar)
                self.log(Logger.ERRO, msg)

                is_process_valid = False
                continue

            # check whether job is workable:
            # for JobNode, callback overriding is needed
            # for JobBlock, recursively call its _is_valid method
            job._inherit_config(self)
            job._complete_config()
            job._inherit_logger(self)

            is_valid = job._is_valid()
            if is_valid:
                self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                   ' '*(max_len-len(job_id)),
                                                    'o'))
            else:
                self.log(Logger.INFO, "%s%s[%s]" % (job_id,
                                                ' '*(max_len-len(job_id)),
                                                'x'))
                mgs = "\terror(s) found in '%s' (ParaJobBlock)" % (job_id)
                self.log(Logger.ERRO, mgs)
                is_process_valid = False

        return is_process_valid
    def execute(self, is_init_job=False):
        self.log(Logger.INFO_HEAD_S, "%s PARA BLOCK" % self.id)
        self.pre_hook()
        logger = self._get_logger()

        # fire the sub-jobs in parallel
        jobs = self.jobs.values()
        threads = []
        thread_alive_status = []
        for job in jobs:
            job._inherit_config(self) # inherit the config
            job._complete_config(is_soft=False)

            job._inherit_logger(self)
            cpy_logger = Logger(logger.fn)
            cpy_logger.force_stdout_set(False)
            cpy_logger.force_fileout_set(False)
            cpy_logger.clear_cache()
            job._set_logger(cpy_logger)

            job._inherit_delegatees(self)
            delegatees = job._get_delegatees()
            cpy_delegatees = {}
            for id, delegatee in delegatees.items():
                cpy_delegatee = copy.deepcopy(delegatee)
                cpy_delegatee.set_logger(cpy_logger)
                cpy_delegatees[id] = cpy_delegatee
            job._set_delegatees(cpy_delegatees)
            job._refresh_delegatees()

            Thread()
            t = Thread(target=job.execute, kwargs={'is_init_job': False})
            t.start()
            threads.append(t)
            thread_alive_status.append(True)

        # first end, first dump (process log)
        states = []
        while (1):
            is_someone_alive = reduce(lambda t1, t2: t1 or t2, thread_alive_status)
            if not is_someone_alive :
                break
            for i in xrange(len(jobs)):
                t = threads[i]
                j = jobs[i]
                if (not t.isAlive()) and thread_alive_status[i] == True:
                    j.logger.restore_stdout_set()
                    j.logger.restore_fileout_set()
                    child_log = j.logger.get_cache()
                    j.logger.close()
                    logger.handover_cache(child_log)
                    states.append(j.state)
                    thread_alive_status[i] = False

        '''
        for t in threads:
            t.join()
        states = []
        for job in jobs:
            #print '<'*10,job.id
            job.logger.restore_stdout_set()
            job.logger.restore_fileout_set()
            job.logger.dump_cache()
            #print '>'*10,job.id
            states.append(job.state)
        '''

        if Job.ERROR in states:
            state = Job.ERROR
        elif Job.TERMINATE in states:
            state = Job.TERMINATE
        else:
            state = Job.DONE
        self.post_hook(state)
        self.log(Logger.INFO_HEAD_E, "[%s] %s PARA BLOCK" % (state, self.id))
        return self.id, state

if __name__ == '__main__':
    from time import sleep
    from datetime import datetime

    process_id = 'test'
    Job.LOGGER = Logger("%s/log/%s.log" % ('/tmp', process_id), 'w')

    wrapper = JobBlock('test job', 'this is a testing job block')
    wrapper.add_plan(Job.INIT_JOB, Job.START, 'job0')
    wrapper.add_plan('job0', Job.DONE, 'job1')
    wrapper.add_plan('job1', Job.DONE, 'block1')
    wrapper.add_plan('block1', Job.DONE, 'para1')
    wrapper.add_plan('para1', Job.DONE, Job.LAST_JOB)
    print 'finish planing'

    # need self as first parameter
    # dont print, log
    def exe_job0(self):
        self.log(Logger.INFO, 'do something')
        return Job.DONE
    def exe_job1(self):
        for i in xrange(3):
            self.log(Logger.INFO, 'wake up at %s' % datetime.now())
            sleep(1)
        return Job.DONE
    # ==
    j = JobNode(id='job0',desc='desc0')
    j.set_callback(exe_job0)
    wrapper.add_sub_job(j)
    # ==
    j = JobNode(id='job1',desc='desc1')
    j.set_callback(exe_job0)
    wrapper.add_sub_job(j)
    # ==
    j = JobBlock(id='block1', desc='block1')
    j.add_plan(Job.INIT_JOB, Job.DONE, 'inner1')
    j.add_plan('inner1', Job.DONE, 'inner2')
    j.add_plan('inner2', Job.DONE, Job.LAST_JOB)
    wrapper.add_sub_job(j)
    # ==
    j = ParallelJobBlock(id='para1', desc='para1')
    j.add_papallel_plan('inner3','inner4')
    wrapper.add_sub_job(j)

    # ==
    parent_j = wrapper.get_job('block1')
    # --
    j = JobNode(id='inner1',desc='inner1')
    j.set_callback(exe_job0)
    parent_j.add_sub_job(j)
    # --
    j = JobNode(id='inner2',desc='inner2')
    j.set_callback(exe_job0)
    parent_j.add_sub_job(j)
    # ==
    parent_j = wrapper.get_job('para1')
    # --
    j = JobNode(id='inner3',desc='inner3')
    j.set_callback(exe_job1)
    parent_j.add_sub_job(j)
    # --
    j = JobNode(id='inner4',desc='inner4')
    j.set_callback(exe_job1)
    parent_j.add_sub_job(j)


    # ==
    job_id, state = wrapper.execute()
    #raw_input()
