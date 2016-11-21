#!/usr/bin/env python


import argparse
import calendar
import getpass
import happybase
import logging
import random
import sys

logging.basicConfig(level="DEBUG")

TABLE = "bigdatashad_" + getpass.getuser() + "_"
HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]


def connect():
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)

    logging.debug("Connecting to HBase Thrift Server on %s", host)
    conn.open()
    return conn


def loader_profile_stat(connection, day):
    table_name = TABLE + 'profile_stat'
    if table_name not in connection.tables():
        connection.create_table(table_name, {"hits": dict(), "users": dict()})
        logging.debug("Created table %s", table_name)
    else:
        logging.debug("Using table %s", table_name)
    table = happybase.Table(table_name, connection)

    with table.batch(batch_size=1000) as b:
        for line in sys.stdin:
            profile, stat = line.strip().split()
            for one_stat in stat.split(';'):
                hour = one_stat.split(':')[0]
                hits, users = one_stat.split(':')[1].split(',')
                logging.debug("Put %s %s %s %s %s", profile, day, hour, hits, users)
                b.put(
                    '%s/%s' % (profile, day),
                    {
                        'hits:%s' % hour: hits,
                        'users:%s' % hour: users,
                    },
                )


LOADERS = [
    loader_profile_stat,
]

def main():
    objects = globals().copy()
    objects.update(locals())
    methods = dict([(n, m) for n, m in objects.iteritems() if hasattr(m, 'func_name')])
    methods = dict([(n, m) for n, m in methods.iteritems() if n.startswith('loader_')])

    parser = argparse.ArgumentParser()
    parser.add_argument("--loader", required=True, choices=methods.keys())
    parser.add_argument("--day", required=True)

    args = parser.parse_args()
    connection = connect()

    methods[args.loader](connection, args.day)


if __name__ == '__main__':
    main()

