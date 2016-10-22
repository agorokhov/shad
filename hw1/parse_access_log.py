#!/usr/bin/env python


import sys
import re
import datetime
import argparse
from bisect import bisect_left

# 196.223.28.31 - - [16/Nov/2015:00:00:00 +0400] "GET /photo/manage.cgi HTTP/1.1" 404 0 "-" "Mozilla/6.66"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--geobase', required=True, help='Geobase file')
    args = parser.parse_args()
    mainloop(args)


def mainloop(args):
    current_key = None
    record_re = re.compile('([\d\.:]+) - - \[(\S+) [^"]+\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) \d+ "([^"]+)" "([^"]+)"')
    regions = load_regions(args.geobase)
    region_keys = [e['lower'] for e in regions]
    for line in sys.stdin:
        match = record_re.search(line)
        if not match:
            continue
        if match.group(6) != "200":
            continue
        ip = match.group(1)
        ua = parse_ua(match.group(8))
        date = datetime.datetime.strptime(match.group(2), "%d/%b/%Y:%H:%M:%S")
        referer = match.group(7)
        url = match.group(4)
        print '\t'.join([
            ip,
            date.strftime("%s"),
            url,
            referer,
            ua,
            ip_to_country_code(regions, region_keys, ip)
        ])


UA_RULES = [
    ("Chrome", "Chrome"),
    ("YaBrowser", "Yandex Browser"),
    ("Firefox", "Firefox"),
    ("Safari", "Safari"),
    ("MSIE", "Internet Explorer"),
]

def parse_ua(ua_string):
    for substring, name in UA_RULES:
        try:
            if ua_string.index(substring) >= 0:
                return name
        except ValueError:
            pass
    return "Other"


def ip_string_to_number(ip):
    byte_0, byte_1, byte_2, byte_3 = map(int, ip.split("."))
    number = byte_0 << 24 | byte_1 << 16 | byte_2 << 8 | byte_3 << 0
    return number


def ip_to_country_code(regions, region_keys, ip):
    ip_number = ip_string_to_number(ip)
    index = bisect_left(region_keys, ip_number)
    if index:
        return regions[index]['code']
    raise ValueError


def load_regions(regions_file):
    regions = []
    with open(regions_file, 'r') as fd:
        for line in fd:
            fields = line.strip().split(',')
            fields = [s.replace('"', '') for s in fields]
            regions.append({
                'lower': int(fields[0]),
                'upper': int(fields[1]),
                'code': fields[2],
                'name': fields[3]
            })
    regions = sorted(regions, key=lambda e: e['lower'])
    return regions


if __name__ == '__main__':
    main()

