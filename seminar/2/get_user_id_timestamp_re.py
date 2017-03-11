#!/usr/bin/env python


import sys
import re
import datetime

# 196.223.28.31 - - [16/Nov/2015:00:00:00 +0400] "GET /photo/manage.cgi HTTP/1.1" 404 0 "-" "Mozilla/6.66"

def main():
    current_key = None
    record_re = re.compile('([\d\.:]+) - - \[(\S+) [^"]+\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) \d+ "([^"]+)" "([^"]+)"')
    for line in sys.stdin:
        match = record_re.search(line)
        if not match:
            continue
        if match.group(6) != "200":
            continue
        try:
            date = datetime.datetime.strptime(match.group(2), "%d/%b/%Y:%H:%M:%S")
        except ValueError:
            continue
        print "%s\t%s" % (match.group(1), date.strftime("%s"))


if __name__ == '__main__':
    main()

