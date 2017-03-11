#!/usr/bin/env python


import sys

def main():
    current_key = None
    for line in sys.stdin:
        line = line.strip()
        try:
            key, value = line.split('\t', 1)
        except ValueError:
            (key, value) = (line, 1)
        if current_key != key:
            print "%s\t%d" % (key, value)
            current_key = key


if __name__ == '__main__':
    main()

