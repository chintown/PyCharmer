#!/usr/bin/python

import sys
import time
from datetime import datetime
from datetime import timedelta


class DateGenerator(object):
    def __init__(self, date, formate, is_not_string=False):
        """
        create base date by formated time string
        [USAGE] dg = DateGenerator('20110401', '%Y%m%d')
        [HELP] date = DateGenerator("20110401", "%Y%m%d")
            %Y(4), %y(2), %m(2), %b(Jan), %B(January), %d(2),
            %H(24), %I(12), %M(2), %S(2)
        """
        if is_not_string:
            self.date = date
        else:
            try:
                self.date = datetime.strptime(date, formate)
            except AttributeError:
                # older python version
                target_time = time.strptime(date, formate)
                (ye, mo, da, ho, mi, se, wday, yday, is_dist) = list(target_time)
                # http://docs.python.org/release/2.4.3/lib/module-time.html
                self.date = datetime(ye, mo, da, ho, mi, se)
            except ValueError:
                print """
                [ERROR] incorrect formate, '%s', for date string, '%s'
                [HELP]
                create base date by formated time string
                date = DateGenerator("20110401", "%%Y%%m%%d")
                    %%Y(4), %%y(2), %%m(2), %%b(Jan), %%B(January), %%d(2),
                    %%H(24), %%I(12), %%M(2), %%S(2)
                """ % (formate, date)
                sys.exit(1)
        self.formate = formate

    def _convert_time_to_date_time(self, time):
        epoch = mktime(time)
        return fromtimestamp(epoch)

    def get_offset_date(self, offset_days):
        """
        get date by a offset volume
        [USAGE]
        create the date of yesterday by dg.get_offset_date(1)
        create the date of last week by dg.get_offset_date(7)
        """
        offset_date = self.date - timedelta(days=offset_days)
        offset_date_str = datetime.strftime(offset_date, self.formate)
        offset_dg = DateGenerator(offset_date_str, self.formate)
        return offset_dg

    def sub(self, sinuend_object):
        """
        return diff days of slef - sinuend_object
        """
        offset_time = self.date - sinuend_object.date
        return offset_time.days

    def __str__(self):
        return datetime.strftime(self.date, self.formate)

    def __lt__(self, other):
        return self.date < other.date

    def __le__(self, other):
        return self.date <= other.date

    def __eq__(self, other):
        return self.date == other.date

    def __ne__(self, other):
        return self.date != other.date

    def __gt__(self, other):
        return self.date > other.date

    def __ge__(self, other):
        return self.date >= other.date

    def __sub__(self, other):
        return self.date - other.date

if __name__ == '__main__':
    today = DateGenerator('2011040110', '%Y%m%d%H')
    print today
    last_week = today.get_offset_date(7)
    print last_week
    one_day = last_week.get_offset_date(1)
    print one_day
    print '%s' % (today - one_day).days
