#!/usr/bin/env python

import sys
import argparse
import random
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('task_files', metavar='T', type=str, nargs='+',
                        help='files with tasks')
    parser.add_argument('--count', '-n', type=int, default=1,
                        help='number of tests')
    parser.add_argument('--output-file', default='tests_out.md',
                        help='output file name')
    args = parser.parse_args()

    print args.task_files

    tasks = {}
    for task_file in args.task_files:
        if task_file in tasks:
            tasks[task_file]['count'] += 1
            continue
        tasks[task_file] = {
            'count': 1,
            'content': read_file_tasks(task_file)
        }

    compose_tests(tasks, args.count, args.output_file)


def read_file_tasks(filename):
    result = []
    with open(filename) as fd:
        current_id = None
        current_task = ''

        for line in fd:
            line = line.rstrip()
            if line.startswith('====='):
                if current_task:
                    result.append(current_id + ' ' + current_task)
                    current_id = None
                    current_task = ''
                continue
            if not line:
                if current_id:
                    current_task += '\n'
                continue
            if not current_id:
                current_id, current_task = line.split(' ', 1)
            else:
                current_task += '\n' + line

    result.append(current_id + ' ' + current_task)
    return result


def compose_tests(tasks, count, output_file):
    with open(output_file, 'w') as fd:
        stats = {}
        for _ in range(count):
            print >> fd, compose_one_test(tasks, stats)
            print >> fd, '----------'
            print >> fd, '<P style="page-break-before: always">'
        print '\n'.join(['%s - %s' % (k, v) for k, v in stats.iteritems()])


def compose_one_test(tasks, stats):
    result = ''
    for filename in sorted(tasks.keys()):
        if filename not in stats:
            stats[filename] = defaultdict(int)
        tasks_info = tasks[filename]
        task_numbers = set()
        while len(task_numbers) < tasks_info['count']:
            task_numbers.add(random.randint(0, len(tasks_info['content']) - 1))

        for task_index in task_numbers:
            stats[filename][task_index] += 1
            result += tasks_info['content'][task_index] + '\n\n'
    return result

if __name__ == '__main__':
    main()
