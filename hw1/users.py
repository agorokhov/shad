#!/usr/bin/env python
"""
"""

import sys
import os
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
    parser.add_argument('--get-new-facebook-users', action='store_true', help='Emit new users')
    parser.add_argument('--get-users-stat', action='store_true', help='Emit new/lost users statistics')
    parser.add_argument('--dataset', action='append',
                        help='Substring in dataset path to mark as 1 in mapper_mark_dataset')
    parser.add_argument('--tag', '--dataset-mark', action='append', dest='dataset_mark',
                        help='Mark for dataset')

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
            was_signed = False
        if fields[2] == URL_SIGNUP:
            was_signed = True
    if current_key:
        print '\t'.join([current_key, day.strftime("%F"), first_referer, str(was_signed)])


def reducer_new_users(args):
    def is_facebook_user(line):
        try:
            line.split()[2].index('facebook')
        except ValueError:
            return False
        return True

    new_users = 0
    lost_users = 0
    last_day = args.day
    first_day = (datetime.datetime.strptime(args.day, "%Y-%m-%d") - datetime.timedelta(days=13)).strftime("%F")

    current_key = None
    current_line = None
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
                        if args.get_new_facebook_users and is_facebook_user(current_line):
                            print current_line
                    if visit_first_day:
                        lost_users += 1
            current_key = key
            current_line = line.strip()
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
                if args.get_new_facebook_users and is_facebook_user(current_line):
                    print current_line
            if visit_first_day:
                lost_users += 1

    if args.get_users_stat:
        print "%d\t%d" % (new_users, lost_users)


def count_new_users(_):
    sum_fields = [0, 0]
    for line in sys.stdin:
        fields = line.strip().split('\t')
        fields = [int(f) for f in fields]
        for i in range(len(sum_fields)):
            sum_fields[i] += fields[i]
    print "%d\t%d" % (sum_fields[0], sum_fields[1])


def mapper_mark_dataset(args):
    """
    Mark dataset with args.dataset in path with tag=1
    """
    tag = None
    for dataset, tag_ in zip(args.dataset, args.dataset_mark):
        try:
            os.getenv('mapreduce_map_input_file').index(dataset)
            tag = tag_
            break
        except ValueError:
            pass
    if tag is None:
        tag = 0
    print >> sys.stderr, "Mark %s with tag %s" % (os.getenv('mapreduce_map_input_file'), tag)

    for line in sys.stdin:
        key, value = line.strip().split('\t', 1)
        print "%s\t%s\t%s" % (key, tag, value)


def reducer_converted_users(args):
    """
    Join datasets with:
    - tag=0 - new users
    - tag=1 - users from current day (day X)
    - tag=2 - users from day X-1 and X-2
    Select users with tag=1 and and 1 and select with tag=1 and 4th field==True
    (i.e. users, visited /signup), but without tag=2
    """
    new_users = 0
    converted_users = 0
    current_key = None
    tags = set()
    current_was_signup = False
    for line in sys.stdin:
        key, tag, _, _, was_signup = line.strip().split()
        tag = int(tag)
        was_signup = True if was_signup == 'True' else False
        if current_key != key:
            if len(tags) == 2 and current_was_signup:
                converted_users += 1
            current_key = key
            tags = set()
            current_was_signup = False
        if tag == 0:
            new_users += 1
            tags.add(tag)
        elif tag == 1:
            if was_signup:
                current_was_signup = True
            tags.add(tag)
        elif tag == 2:
            if was_signup and current_was_signup:
                if 0 in tags:
                    print >> sys.stderr, "Turn off current_was_signup for key=%s" % key
                current_was_signup = False
    if len(tags) == 2 and current_was_signup:
        converted_users += 1
    print "%d\t%d" % (new_users, converted_users)


def count_converted_users(_):
    sum_fields = [0, 0]
    for line in sys.stdin:
        fields = line.strip().split('\t')
        fields = [int(f) for f in fields]
        for i in range(len(sum_fields)):
            sum_fields[i] += fields[i]
    print "%d\t%d\t%f" % (sum_fields[0], sum_fields[1],
                          sum_fields[1]/float(sum_fields[0]) if sum_fields[0] > 0 else 0)


if __name__ == '__main__':
    main()
