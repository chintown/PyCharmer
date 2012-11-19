#!/usr/bin/python

import os
import sys
import codecs
import inspect
from time import gmtime
from time import strftime
from pprint import pprint


class Logger(object):
    '''
    this logger provide functionalities like "tee" command
    and improve the readibility.
    it could be mute for some code block and store the message in the buffer for
    further usage.
    '''
    INFO_SUBHEAD    = '-2'
    INFO_SUBHEAD_S  = '-4'
    INFO_SUBHEAD_E  = '-6'
    INFO_HEAD       = '-1'
    INFO_HEAD_S     = '-3'
    INFO_HEAD_E     = '-5'
    RAW        = '0'
    INFO       = '1'
    INFO_MORE  = '2'
    INFO_WHY   = '3'
    WARN       = '4'
    WARN_MORE  = '5'
    WARN_WHY   = '6'
    ERRO       = '7'
    ERRO_MORE  = '8'
    ERRO_WHY   = '9'
    EXIT       = '99'
    DUMP       = '999'
    _LEVEL = {
        '-99':{'type': 'DUMP'},
        '-1':{'type': '    '},'-3':{'type': '    '},'-5':{'type': '    '},
        '-2':{'type': '    '},'-4':{'type': '    '},'-6':{'type': '    '},
        '0': {'type': 'RAW '},
        '1': {'type': '    '}, '2': {'type': '    '}, '3': {'type': '    '},
        '4': {'type': 'WARN'}, '5': {'type': 'WARN'}, '6': {'type': 'WARN'},
        '7': {'type': 'ERRO'}, '8': {'type': 'ERRO'}, '9': {'type': 'ERRO'},
        '99': {'type': 'EXIT'},
    }

    MAX_LINE_WIDTH = 75  # of log message
    TITLE_OFFSET = 3

    def __init__(self, fn, mode='a'):
        """
        [PARA]
        fn: fullpath of log file
        mode: mode for opening the log file
        [HINT]
        remeber to close the logger
        """
        self.fn = self._set_ext(fn)
        self.mode = mode
        self.f = None
        self.is_mute = 0
        self.last_is_stdout = self.is_stdout = 1
        self.last_is_fileout = self.is_fileout = 1
        self.tag = ''
        self.cache = ''  # for thread
        self.tracelevel = 100

    def _get_file_handler(self):
        if self.f is None:
            try:
                self.f = codecs.open(self.fn, self.mode, 'utf8')
            except IOError, e:
                print "[WARN] Logger can not access the target file:"
                print e
                direcotry = os.path.dirname(self.fn)
                try:
                    ret = os.system("mkdir -p %s" % direcotry)
                except e:
                    print "[ERROR] Logger can not mkdir: %s." % (direcotry)
                    print e

        return self.f

    def _set_ext(self, fn):
        if not fn.endswith('.log'):
            fn += '.log'
        return fn

    def _trace_back(self):
        '''
        return the trace back message in reorganized structure w/
        file content inspector
        '''
        num_line_to_trace = 3
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, num_line_to_trace)
        res = "\n"
        for record in calframe:
            #pprint(record )
            frame_instance, filepath, tar_lineno, func, list_line, idx_of_line = list(record)
            file = filepath.split('/')[-1]
            res = "%s %s\n" % (file, func)
            i = 0
            indicator = '==> '
            for line in list_line:
                if i == idx_of_line:
                    indent = indicator
                else:
                    indent = ' ' * len(indicator)
                offset = i - idx_of_line
                lineno = tar_lineno + offset
                res += "%s%s %s" % (indent, lineno, line)
                i += 1
            #print res
        return res

    def get_stamp(self):
        stamp = strftime("%m-%d %H:%M:%S", gmtime())
        return stamp

    def set_file(self, fn):
        self.fn = self._set_ext(fn)
    def set_mute(self, is_mute):
        self.is_mute = is_mute
    def set_stdout(self, is_stdout):
        self.is_stdout = is_stdout

    def force_stdout_set(self, is_stdout):
        self.last_is_stdout = self.is_stdout
        self.is_stdout = is_stdout

    def restore_stdout_set(self):
        self.is_stdout = self.last_is_stdout

    def force_fileout_set(self, is_fileout):
        self.last_is_fileout = self.is_fileout
        self.is_fileout = is_fileout

    def restore_fileout_set(self):
        self.is_fileout = self.last_is_fileout

    def clear_cache(self):
        self.cache = ''

    def get_cache(self):
        return self.cache

    def dump_cache(self):
        sys.stdout.write(self.cache)

    def handover_cache(self, cache):
        sys.stdout.write(cache)
        self.f.write(cache)
        self.cache += cache

    def set_tag(self, tag):
        """
        [USAGE] l.set_tag('INFO')
        [HELP]  for MM-DD hh:mm:ss [INFO] log message
        """
        self.tag = "[%s]\t" % (tag)

    def p(self, *argv):
        pprint(argv)

    def log(self, trace_level, *list0):
        """ log input data in a blocked line (w/ new line code) """
        self._log(True, trace_level, *list0)

    def log_float(self, trace_level, *list0):
        """ log input data in a floating line (w/o new line code) """
        self._log(False, trace_level, *list0)

    def _log(self, is_new_line, trace_level, *list0):
        """
        [USAGE] l.log(1, 'a', ['x', 'y'])
        [HELP] for MM-DD hh:mm:ss [INFO] (a, 'a', ['x', 'y'])
        """
        # preprare file
        if self.is_fileout:
            f = self._get_file_handler()
            if f is None: return                            # may exit from here

        # preprare time stamp
        stamp = self.get_stamp()

        # preprare tag
        if '' != self.tag:
            tag = self.tag
        else:
            tag = Logger._LEVEL[trace_level]['type']

        # preprare content
        content = ''
        if trace_level == Logger.EXIT:
            content = "process interrupted at %s! : (\
                     \nlog: %s " % (list0[0], self.fn)
        elif len(list0) == 1:
            content = unicode(list0[0])
        else:
            content = unicode(list0)

        if trace_level == Logger.INFO_HEAD:
            content = content.upper()

        # prepare prepare
        prefix = ''
        title_prefix_len = Logger.MAX_LINE_WIDTH - len(content) + Logger.TITLE_OFFSET
        if trace_level == Logger.INFO_HEAD:
            prefix = '['+'='*title_prefix_len
        elif trace_level == Logger.INFO_HEAD_S:
            prefix = r'/'+'='*title_prefix_len
        elif trace_level == Logger.INFO_HEAD_E:
            prefix = '\\'+'='*title_prefix_len
        elif trace_level == Logger.INFO_SUBHEAD_S:
            prefix = r'/'+'`'*title_prefix_len
        elif trace_level == Logger.INFO_SUBHEAD_E:
            prefix = '\\'+'_'*title_prefix_len

        # append suffix
        suffix = ''
        title_suffix_len = 3
        if trace_level == Logger.INFO_HEAD:
            suffix+= '='*title_suffix_len + ']'
        elif trace_level == Logger.INFO_HEAD_S:
            suffix+= '='*(title_suffix_len-1) + '\\\\'
        elif trace_level == Logger.INFO_HEAD_E:
            suffix+= '='*(title_suffix_len-1) + '//'
        elif trace_level == Logger.INFO_SUBHEAD:
            suffix+= '-'*(Logger.MAX_LINE_WIDTH-len(prefix+content))
        elif trace_level == Logger.INFO_SUBHEAD_S:
            suffix+= '`'*title_suffix_len + '\\'
        elif trace_level == Logger.INFO_SUBHEAD_E:
            suffix+= '_'*title_suffix_len + '/'

        if is_new_line:
            suffix+= ""

        # compose log
        if trace_level == Logger.RAW:
            tokens = [content]
        else:
            tokens = content.split("\n")
            tokens = reduce(lambda tok1, tok2:
                            tok1 + tok2,
                            map(lambda t:
                                self.smart_tokenizer(t, Logger.MAX_LINE_WIDTH),
                                tokens)
                            )
        #tokens = self.smart_tokenizer(content, Logger.MAX_LINE_WIDTH)
        for token in tokens:
            ##record = "[%2s]\t%s\t[%s] %s %s %s \n" % (trace_level, stamp, tag,
            record = "%s [%s] %s %s %s \n" % ( stamp, tag,
                                                prefix, token, suffix)

            #record = record + suffix

            ##record = str(self)[-10:] + record # thread debugging # XXX

            # write log !
            if self.is_fileout:
                self.f.write(record)

            # print log
            if not self.is_mute and self.is_stdout:
                sys.stdout.write(record)

            # cache log
            self.cache += record

        # more handling
        # TODO think when open trace back
        #if '' == self.tag and int(trace_level) == int(Logger.ERRO):
        #    print self._trace_back()
    def smart_tokenizer(self, target, len_token):
        '''
        split long message into tokens in MAX_LINE_WIDTH
        but keep the word/segment not broken
        '''
        result = [] # store the tokenized message in list of content lines w/ \n
        len_extend = 10 # a flexible extend range of the given length
        len_token_extend = len_token + len_extend # max length of expected tokened line
        len_target = len(target)

        # no need to tokenize
        if len_target <= len_token_extend:
            result.append(target)
            return result

        # get all possible space(or other char) position in the given string
        # these will become the candidates position for tokenizing
        newline_candidates = [' ']
        newline_pos_candidates = filter(lambda idx:
                                        target[idx] in newline_candidates,
                                        xrange(len_target))

        # debug message
        #print newline_pos_candidates
        #print target, len_target
        #for i in xrange(0,100):
        #    sys.stdout.write(str(i % 10))
        #    pass
        #print
        #for i in xrange(0,100):
        #    if int(i%10) == 0:
        #        sys.stdout.write(str(i/10))
        #        pass
        #    else:
        #        sys.stdout.write(' ')
        #        pass
        #print

        # last_epos [sliding window] epos + extra
        last_epos = spos = epos = 0
        while(1):
            len_left_target = len_target - last_epos + 1

            # no need to tokenize
            if len_left_target  < len_token_extend:
                result.append(target[last_epos:])
                break

            # epos_curr_token is the ideal position to tokenize
            # to make the word/segment complete we may choose
            # lower(more left) or higher(more right) postion
            epos_curr_token = last_epos + len_token
            ##print 'last_epos, epos_curr_token', last_epos, epos_curr_token
            lower_pos_candidates = filter(lambda pos:
                                    last_epos<pos and pos<=epos_curr_token,
                                    newline_pos_candidates)
            higher_pos_candidates = filter(lambda pos:
                                    pos>epos_curr_token,
                                    newline_pos_candidates)
            ##print 'window', lower_pos_candidates, higher_pos_candidates

            # there may be different candidates
            # the main idea is to choose the nearest one to the ideal position
            if 0 == len(lower_pos_candidates) and 0 == len(higher_pos_candidates):
                # no option to tokenize
                result.append(target[last_epos:])
                ##print 'token',target[last_epos:]
                break
            elif 0 == len(higher_pos_candidates):
                # only short candidate position
                pos = lower_pos_candidates.pop()
            elif 0 == len(lower_pos_candidates):
                # only large candidate position
                pos = higher_pos_candidates.pop(0)
            else:
                # has both lower and higher candidates
                pos_w_diffs = map(lambda pos: (abs(pos-epos_curr_token), pos), newline_pos_candidates)
                pos_w_diffs.sort()
                higher_pos_w_diffs = filter(lambda p: p[1]>epos_curr_token,pos_w_diffs)
                lower_pos_w_diffs = filter(lambda p: p[1]<=epos_curr_token,pos_w_diffs)

                ##print pos_w_diffs
                ##print lower_pos_w_diffs , higher_pos_w_diffs

                # find closest pos (len_token +/- len_extend)
                # proity:
                # higher but still in max extend range >
                # lower  but still in minextend range >
                # the nearest lower one to the ideal position
                pos = None
                for (diff, pos_) in higher_pos_w_diffs:
                    if diff <= len_extend: pos = pos_
                if pos is None:
                    for (diff, pos_) in lower_pos_w_diffs:
                        if diff <= len_extend: pos = pos_
                if pos is None:
                    pos = lower_pos_candidates.pop()

            spos = last_epos
            epos = pos
            token = target[spos: epos]
            result.append(token)
            last_epos = epos

            #print 'select pos', spos, epos
            #print 'token',token
        #print 'result', result, len(result)
        #if len(result)> 3:
        #    raw_input()
        return result
    def close(self):
        if self.f is not None:
            self.f.close()

if __name__ == '__main__':
    l = Logger('/Users/chintown/tmp/testlog')
    l.set_stdout(1)
    l.set_tag('XX')
    l.log('aaaaaaaaaaaaa')
    l.log(1,2,3 )
    l.close()
