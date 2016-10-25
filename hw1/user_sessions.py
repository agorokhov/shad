#!/usr/bin/env python
"""
Reducer, counts users, hits, session, session length, session time
"""

import sys
import re
import datetime
import argparse
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sum', action='store_true', help='Do sum sessions stats')
    args = parser.parse_args()

    if args.sum:
        count_sessions_stat(args)
    else:
        reducer_user_sessions()


def count_sessions_stat(args):
    sum_fields = [0, 0, 0, 0, 0]
    country_users = defaultdict(int)
    for line in sys.stdin:
        fields = line.strip().split('\t')
        if fields[0] == "COUNTRY":
            country_users[fields[1]] += int(fields[2])
        else:
            fields = [int(f) for f in fields]
            for i in range(5):
                sum_fields[i] += fields[i]
    print "%d\t%d\t%d\t%f\t%f\t%f" % (
        sum_fields[0],
        sum_fields[1],
        sum_fields[2],
        sum_fields[0]/float(sum_fields[2]),
        sum_fields[3]/float(sum_fields[2]),
        sum_fields[4]/float(sum_fields[2])
    )
    for country, count in country_users.iteritems():
        print "COUNTRY\t%s\t%d" % (country, count)


def reducer_user_sessions():
    current_user = None
    start_timestamp = None
    last_timestamp = None
    users = 0
    hits = 0
    sessions = 0
    sessions_bounce = 0
    sessions_length = 0
    country_users = defaultdict(int)

    for line in sys.stdin:
        fields = line.strip().split('\t')
        user, timestamp, country = fields[0], int(fields[1]), fields[5]
        if user != current_user:
            if current_user:
                sessions += 1
                sessions_length += last_timestamp - start_timestamp
                if last_timestamp == start_timestamp:
                    sessions_bounce += 1
            start_timestamp = timestamp
            last_timestamp = timestamp
            current_user = user
            users += 1
            country_users[country] += 1
        if timestamp - last_timestamp >  30*60:
            sessions += 1
            sessions_length += last_timestamp - start_timestamp
            if last_timestamp == start_timestamp:
                sessions_bounce += 1
            start_timestamp = timestamp
        last_timestamp = timestamp
        hits += 1
    sessions += 1
    sessions_length += last_timestamp - start_timestamp
    if last_timestamp == start_timestamp:
        sessions_bounce += 1

    print "%d\t%d\t%d\t%d\t%d" % (hits, users, sessions, sessions_length, sessions_bounce)
    for country, count in country_users.iteritems():
        print "COUNTRY\t%s\t%d" % (country, count)


if __name__ == '__main__':
    main()

