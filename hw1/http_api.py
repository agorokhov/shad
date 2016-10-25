#!/usr/bin/env python

import argparse
import datetime
import getpass
import hashlib
import random
import struct
import os.path

from flask import Flask, request, abort, jsonify, g

from parse_access_log import load_regions

LOCAL_DATA_DIR = "/home/agorokhov/devel/stat"
LOCAL_CONFIG_DIR = "/home/agorokhov/devel/conf"

app = Flask(__name__)
app.secret_key = "gorokhov"


def iterate_between_dates(start_date, end_date):
    span = end_date - start_date
    for i in xrange(span.days + 1):
        yield start_date + datetime.timedelta(days=i)


def get_geo_names():
    geo_names = getattr(g, '_geo_names', None)
    if geo_names is None:
        regions = load_regions(os.path.join(LOCAL_CONFIG_DIR, 'IP2LOCATION-LITE-DB1.CSV'))
        geo_names = g._geo_names = dict([(r['code'], r['name']) for r in  regions])
    return geo_names


@app.teardown_appcontext
def teardown_geo_names(exception):
        delattr(g, '_geo_names')


@app.route("/")
def index():
    return "OK!"


@app.route("/api/hw1")
def api_hw1():
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)
    if start_date is None or end_date is None:
        abort(400)
    start_date = datetime.datetime(*map(int, start_date.split("-")))
    end_date = datetime.datetime(*map(int, end_date.split("-")))

    result = {}
    for date in iterate_between_dates(start_date, end_date):
        day = date.strftime("%F")
        result[day] = get_day_stats(day)

    return jsonify(result)


def get_day_stats(day):
    stat = {}
    try:
        geo_names = get_geo_names()
        with open(os.path.join(LOCAL_DATA_DIR, 'sessions-%s.txt' % day)) as fd:
            for line in fd:
                fields = line.strip().split()
                if fields[0] == 'COUNTRY':
                    if 'users_by_country' not in stat:
                        stat['users_by_country'] = {}
                    country = geo_names.get(fields[1], fields[1])
                    stat['users_by_country'][country] = int(fields[2])
                else:
                    stat['total_hits'] = int(fields[0])
                    stat['total_users'] = int(fields[1])
                    stat['average_session_length'] = float(fields[3])
                    stat['average_session_time'] = float(fields[4])
                    stat['bounce_rate'] = float(fields[5])
    except IOError:
        pass

    try:
        with open(os.path.join(LOCAL_DATA_DIR, 'pages-%s.txt' % day)) as fd:
            stat['top_10_pages'] = []
            for line in fd:
                fields = line.strip().split()
                stat['top_10_pages'].append(fields[1])
    except IOError:
        pass

    try:
        with open(os.path.join(LOCAL_DATA_DIR, 'new_users-%s.txt' % day)) as fd:
            for line in fd:
                fields = line.strip().split()
                stat['new_users'] = int(fields[0])
                stat['lost_users'] = int(fields[1])
                break
    except IOError:
        pass

    try:
        with open(os.path.join(LOCAL_DATA_DIR, 'conv_fb_users-%s.txt' % day)) as fd:
            for line in fd:
                fields = line.strip().split()
                stat['facebook_signup_conversion_3'] = float(fields[2])
                break
    except IOError:
        pass

    return stat


def login_to_port(login):
    """
    We believe this method works as a perfect hash function
    for all course participants. :)
    """
    hasher = hashlib.new("sha1")
    hasher.update(login)
    values = struct.unpack("IIIII", hasher.digest())
    folder = lambda a, x: a ^ x + 0x9e3779b9 + (a << 6) + (a >> 2)
    return 10000 + reduce(folder, values) % 20000


def main():
    parser = argparse.ArgumentParser(description="HW 1 Example")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=login_to_port(getpass.getuser()))
    parser.add_argument("--debug", action="store_true", dest="debug")
    parser.add_argument("--no-debug", action="store_false", dest="debug")
    parser.set_defaults(debug=False)

    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
