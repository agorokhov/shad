#!/usr/bin/env python
"""
"""

import sys
import re
import datetime
import argparse
from collections import defaultdict


URL_SIGNUP = '/signup'

def main():
    objects = globals().copy()
    objects.update(locals())
    methods = dict([(n, m) for n, m in objects.iteritems() if hasattr(m, 'func_name')])
    methods = dict([(n, m) for n, m in methods.iteritems() if (n.startswith('reducer_') or
                                      n.startswith('mapper_') or
                                      n.startswith('count_') or
                                      n.startswith('sort_'))])

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--function', choices=methods.keys(),
                        default=None, required=True, help='Function to call')
    parser.add_argument('--day', help='Date for new/lost users')
    args = parser.parse_args()

    methods[args.function](args)


def reducer_day_users(_):
    current_key = None
    day = None
    first_referer = None
    was_signed = False
    for line in sys.stdin:
        fields = line.strip().split()
        key = fields[0]
        if current_key != key:
            if current_key:
                print '\t'.join([current_key, day.strftime("%F"), first_referer, str(was_signed)])
            current_key = key
            day = datetime.datetime.fromtimestamp(int(fields[1]))
            first_referer = fields[3]
        if fields[2] == URL_SIGNUP:
            was_signed = True
    if current_key:
        print '\t'.join([current_key, day.strftime("%F"), first_referer, str(was_signed)])


def reducer_new_users(args):
    new_users = 0
    lost_users = 0
    last_day = args.day
    first_day = (datetime.datetime.strptime(args.day, "%Y-%m-%d") - datetime.timedelta(days=13)).strftime("%F")

    current_key = None
    visit_last_day, visit_first_day = False, False
    days_visited = 0

    for line in sys.stdin:
        fields = line.strip().split()
        key, day = fields[0], fields[1]
        if current_key != key:
            if current_key:
                if days_visited == 1:
                    if visit_last_day:
                        new_users += 1
                    if visit_first_day:
                        lost_users += 1
            current_key = key
            visit_last_day, visit_first_day = False, False
            days_visited = 0
        days_visited += 1
        if day == last_day:
            visit_last_day = True
        if day == first_day:
            visit_first_day = True
    if current_key:
        if days_visited == 1:
            if visit_last_day:
                new_users += 1
            if visit_first_day:
                lost_users += 1

    print "%d\t%d" % (new_users, lost_users)


def count_new_users(_):
    sum_fields = [0, 0]
    for line in sys.stdin:
        fields = line.strip().split('\t')
        fields = [int(f) for f in fields]
        for i in range(len(sum_fields)):
            sum_fields[i] += fields[i]
    print "%d\t%d" % (sum_fields[0], sum_fields[1])


if __name__ == '__main__':
    main()
