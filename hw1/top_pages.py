#!/usr/bin/env python
"""
"""

import sys
import re
import datetime
import argparse
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--function', choices=['mapper_top', 'reducer_sum', 'sort_pages'],
                        default='reducer_sum', help='Function to call')
    args = parser.parse_args()

    if args.function == 'reducer_sum':
        reducer_sum()
    elif args.function == 'mapper_top':
        mapper_top()
    elif args.function == 'sort_pages':
        sort_pages()


class Heap:
    def __init__(self, ntop=10, fcmp=lambda x, y: x[0] < y[0]):
        self.data = []
        self.ntop = ntop
        self.fcmp = fcmp

    def push(self, elem):
        index = None
        for i in range(len(self.data)):
            if self.fcmp(self.data[i], elem) < 0:
                index = i
                break
        if index is not None and index < self.ntop:
            self.data.insert(index, elem)
            if len(self.data) > self.ntop:
                self.data.pop(self.ntop)
        elif index is None and len(self.data) < self.ntop:
            self.data.append(elem)


def cmp_pages(x, y):
    if x[0] < y[0]:
        return -1
    elif x[0] == y[0]:
        if x[1] > y[1]:
            return -1
        elif x[1] == y[1]:
            return 0
    return 1


def mapper_top():
    top_keys = Heap(fcmp=cmp_pages)
    for line in sys.stdin:
        key, count = line.strip().split()
        count = int(count)
        top_keys.push((count, key))
    for elem in top_keys.data:
        print "%d\t%s" % (elem[0], elem[1])


def reducer_sum():
    current_key = None
    summa = 0

    for line in sys.stdin:
        fields = line.strip().split()
        key = fields[0]
        count = 1 if len(fields) < 2 else int(fields[1])
        if key != current_key:
            if summa:
                print "%s\t%d" % (current_key, summa)
            summa = 0
            current_key = key
        summa += count
    if summa:
        print "%s\t%d" % (current_key, summa)


def sort_pages():
    pages = []
    for line in sys.stdin:
        count, page = line.strip().split()
        count = int(count)
        pages.append((count, page))
    pages = sorted(pages, cmp=cmp_pages, reverse=True)
    for elem in pages:
        print "%d\t%s" % (elem[0], elem[1])


if __name__ == '__main__':
    main()

