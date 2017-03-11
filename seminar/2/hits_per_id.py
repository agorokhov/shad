#!/usr/bin/env python


import sys

def main():
    current_key = None
    hits = 0

    for line in sys.stdin:
        line = line.strip()
        try:
            key, value = line.split('\t', 1)
            value = int(value)
        except ValueError:
            (key, value) = line, 1

        if current_key != key:
            if current_key:
                print "%s\t%d" % (current_key, hits)
            hits = 0
            current_key = key
        hits += value

    if current_key:
        print "%s\t%d" % (current_key, hits)


if __name__ == '__main__':
    main()

