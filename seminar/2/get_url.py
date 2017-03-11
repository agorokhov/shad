#!/usr/bin/env python


import sys

def main():
    current_key = None
    for line in sys.stdin:
        fields = line.strip().split(' ')
        print fields[6]


if __name__ == '__main__':
    main()

