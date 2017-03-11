#!/usr/bin/env python


import sys

def main():
    current_key = None
    for line in sys.stdin:
        ip, _ = line.strip().split(' ', 1)
        print ip


if __name__ == '__main__':
    main()

