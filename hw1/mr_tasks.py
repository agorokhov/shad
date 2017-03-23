#!/usr/bin/env python
"""
Script to run functions, useful for MR streaming.
Runs methods by name, name can starts with:
- 'mapper_' - mapper function
- 'reducer_' - reducer function
- 'sort_' - sort in-memory
- 'count_' - locally sum (count) values from several files

Usage:
$ mr_tasks.py FUNCTION [PARAMETERS ...]

Examples:

- list all possible functions:
$ mr_tasks.py -h

- run function 'reducer_new_users' with default parameters:
$ mr_tasks.py reducer_new_users

- help for all function 'reducer_new_users' parameters:
$ mr_tasks.py reducer_new_users -h

"""

import sys
import os
import re
import datetime
import inspect
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
    subparsers = parser.add_subparsers(help='sub-command help')

    for name, func in methods.iteritems():
        subparser = subparsers.add_parser(name, help='Function %s' % name)
        subparser.set_defaults(func=func)
        args = inspect.getargspec(func)
        args_default = args.defaults if args.defaults is not None else ()
        if len(args_default) < len(args.args):
            args_default = tuple([None for _ in range(len(args.args)-len(args_default))]) + args_default
        for dest, arg_default in zip(args.args, args_default):
            arg_params = {
                'type': str,
                'action': 'store',
                'dest': dest,
                'default': arg_default,
            }
            arg = '--' + dest.replace('_', '-')
            if arg_default is not None:
                arg_params['type'] = type(arg_default)
            if isinstance(arg_default, list):
                arg_params['action'] = 'append'
                arg_params['type'] = str
            elif isinstance(arg_default, bool):
                arg_params['action'] = 'store_true'
                subparser.add_argument(arg.replace('--', '--no-'), dest=dest,
                                       default=arg_default,
                                       action='store_false')
                del arg_params['type']
            else:
                arg_params['action'] = 'store'
            subparser.add_argument(arg, **arg_params)
    args = parser.parse_args()

    func_args = dict(vars(args))
    func_args.pop("func")
    args.func(**func_args)


def sum_int_fields(length):
    sum_fields = [0 for _ in range(length)]
    for line in sys.stdin:
        fields = line.strip().split('\t')
        fields = [int(f) for f in fields]
        for i in range(len(sum_fields)):
            sum_fields[i] += fields[i]
    return sum_fields


def reducer_day_users():
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


def reducer_new_users(day, get_users_stat=True, get_new_facebook_users=False):
    def is_facebook_user(line):
        try:
            line.split()[2].index('facebook')
        except ValueError:
            return False
        return True

    new_users = 0
    lost_users = 0
    last_day = day
    first_day = (datetime.datetime.strptime(day, "%Y-%m-%d") - datetime.timedelta(days=13)).strftime("%F")

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
                        if get_new_facebook_users and is_facebook_user(current_line):
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
                if get_new_facebook_users and is_facebook_user(current_line):
                    print current_line
            if visit_first_day:
                lost_users += 1

    if get_users_stat:
        print "%d\t%d" % (new_users, lost_users)


def count_new_users():
    sum_fields = sum_int_fields(2)
    print "%d\t%d" % (sum_fields[0], sum_fields[1])


def mapper_mark_dataset(dataset=[], dataset_mark=[]):
    """
    Mark dataset with args.dataset in path with tag=1
    """
    print str(dataset)
    print str(dataset_mark)
    tag = None
    for dataset, tag_ in zip(dataset, dataset_mark):
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


def reducer_converted_users():
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
    converted_users_v2 = 0
    current_key = None
    tags = set()
    current_was_signup = False
    current_was_signup_v2 = False
    for line in sys.stdin:
        key, tag, _, _, was_signup = line.strip().split()
        tag = int(tag)
        was_signup = True if was_signup == 'True' else False
        if current_key != key:
            if len(tags) == 2 and current_was_signup:
                converted_users += 1
            if len(tags) == 2 and current_was_signup_v2:
                converted_users_v2 += 1
            current_key = key
            tags = set()
            current_was_signup = False
            current_was_signup_v2 = False
        if tag == 0:
            new_users += 1
            tags.add(tag)
        elif tag == 1:
            if was_signup:
                current_was_signup = True
                current_was_signup_v2 = True
            tags.add(tag)
        elif tag == 2:
            if was_signup and current_was_signup:
                if 0 in tags:
                    print >> sys.stderr, "Turn off current_was_signup for key=%s" % key
                current_was_signup = False
    if len(tags) == 2 and current_was_signup:
        converted_users += 1
    if len(tags) == 2 and current_was_signup_v2:
        converted_users_v2 += 1
    print "%d\t%d\t%d" % (new_users, converted_users, converted_users_v2)


def count_converted_users():
    sum_fields = sum_int_fields(3)
    print "%d\t%d\t%f\t%d\t%f" % (
        sum_fields[0], sum_fields[1],
        sum_fields[1]/float(sum_fields[0]) if sum_fields[0] > 0 else 0,
        sum_fields[2],
        sum_fields[2]/float(sum_fields[0]) if sum_fields[0] > 0 else 0
    )


def mapper_profile_stat():
    re_profile = re.compile(r'/(id\d+)$')
    for line in sys.stdin:
        fields = line.strip().split('\t')
        ip, timestamp, url = fields[0], int(fields[1]), fields[2]
        match = re_profile.match(url)
        if not match:
            continue
        print "\t".join([
            match.group(1),
            ip,
            datetime.datetime.fromtimestamp(timestamp).strftime("%H"),
            str(1)
        ])


def reducer_combine_profile_stat():
    current = (None, None, None)
    current_count = 0
    for line in sys.stdin:
        profile, ip, date, count = line.strip().split('\t')
        count = int(count)
        if current != (profile, ip, date):
            if current[0]:
                print "\t".join(list(current) + [str(current_count)])
            current = (profile, ip, date)
            current_count = 0
        current_count += count
    if current[0]:
         print "\t".join(list(current) + [str(current_count)])


def reducer_profile_stat():
    def format_hours(hours):
        return ";".join(["%s:%d,%d" % (h, s[0], s[1])
                         for h, s in enumerate(hours)
                         if s[0]])

    HOURS = 24
    current_profile = None
    current_ip = None
    current_hours = [[0, 0] for _ in range(HOURS)]
    current_hour = None

    for line in sys.stdin:
        profile, ip, hour, count = line.strip().split('\t')
        hour = int(hour)
        count = int(count)
        if current_profile != profile:
            if current_profile:
                print "%s\t%s" % (current_profile, format_hours(current_hours))
                current_hours = [[0, 0] for _ in range(HOURS)]
            current_profile = profile
            current_ip = None
        if current_ip != ip:
            current_ip = ip
            current_hour = None
        if current_hour != hour:
            current_hour = hour
            current_hours[hour][1] += 1 # increment users
        current_hours[hour][0] += count
    if current_profile:
        print "%s\t%s" % (current_profile, format_hours(current_hours))


def mapper_liked_profiles():
    """
    For HW3 - select liked profiles, i.e. url=/id1234?like=1
    Input: extended log (IP, ts, url, ...)
    Output: profile, day
    """

    re_liked_profile = re.compile(r'/(id\d+).*[&?]like=1')
    for line in sys.stdin:
        fields = line.strip().split('\t')
        ip, timestamp, url = fields[0], int(fields[1]), fields[2]
        match = re_liked_profile.match(url)
        if not match:
            continue
        print "\t".join([
            match.group(1),
            datetime.datetime.fromtimestamp(timestamp).strftime("%F"),
        ])


def reducer_uniq_value():
    """
    For HW3 - combiner to unique days for profile; so in the input days are not unique,
    in the output - are unique.
    Input: profile, day
    Output: profile, day
    """
    current_key = None
    values_set = set()
    for line in sys.stdin:
        key, value = line.strip().split('\t', 1)
        if key != current_key:
            if values_set:
                print '\n'.join(['%s\t%s' % (current_key, v) for v in values_set])
            current_key = key
            values_set = set()
        values_set.add(value)
    if values_set:
        print '\n'.join(['%s\t%s' % (current_key, v) for v in values_set])


def reducer_profile_liked_three_days():
    """
    For HW3 - reducer to count number of liked profiles for 3 continuous days.
    Input: profile, day
    Output: number_of_profiles
    """
    LIKED_PROFILE_PERIOD = 3 # days
    liked_profiles = 0
    current_key = None
    values_set = set()
    for line in sys.stdin:
        key, value = line.strip().split('\t', 1)
        if key != current_key:
            if len(values_set) == LIKED_PROFILE_PERIOD:
                liked_profiles += 1
            current_key = key
            values_set = set()
        values_set.add(value)
    if len(values_set) == LIKED_PROFILE_PERIOD:
        liked_profiles += 1
    print "%d" % liked_profiles



def count_profiles():
    sum_fields = sum_int_fields(1)
    print "%d" % sum_fields[0]


if __name__ == '__main__':
    main()
