from __future__ import print_function
import requests
import argparse
import json
import time
import sys
import os
import curses
import re

try:
    from terminaltables import AsciiTable
except ImportError:
    AsciiTable = None


def _get_bench_types(url):
    response = requests.get('{}/v1/bench/types'.format(url))
    if response.status_code != 200:
        print("Failed to fetch benchmark types")
        return
    return response.json()


def _get_bench_options(type, url):
    response = requests.get('{}/v1/bench/types/{}'.format(url, type))
    if response.status_code != 200:
        print("Failed to fetch benchmark options")
        return
    return response.json()


def start_bench(config, url):
    response = requests.post("{}/v1/bench".format(url), data=json.dumps(config), headers={'content-type': 'application/json'})
    if response.status_code != 200:
        print("Failed to start test")
        return
    return response.text


def _create_table(data):
    """Creates a table from the given data."""
    table = AsciiTable(data)
    table.inner_column_border = False
    table.inner_row_border = False
    table.outer_border = False
    table.inner_heading_row_border = False
    table.padding_right = 4
    return str(table.table)


def _report_processes(processes):
    def to_snake_case(option):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', option)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    columns = ['PROCESS']
    fields = []
    data = []
    data.append(columns)
    for id, stats in processes.items():
        for stat, value in stats.items():
            if stat not in fields:
                fields.append(stat)
                columns.append(to_snake_case(stat).upper())

    for id, stats in processes.items():
        row = []
        row.append(id)
        for field in fields:
            row.append(stats[field])
        data.append(row)

    text = _create_table(data)
    stdscr.addstr(0, 0, text)
    stdscr.refresh()


def report_progress(bench_id, url):
    response = requests.get("{}/v1/bench/{}/progress".format(url, bench_id))
    if response.status_code != 200:
        print("Failed to fetch test progress")
        return False
    else:
        progress = response.json()
        _report_processes(progress['processes'])

        if progress['status'] == 'COMPLETE':
            return False
    return True


def report_result(bench_id, url):
    response = requests.get("{}/v1/bench/{}/result".format(url, bench_id))
    if response.status_code != 200:
        print("Failed to fetch test result")
    else:
        result = response.json()
        print(_report_processes(result['processes']))


def stop_bench(bench_id, url):
    requests.delete("{}/v1/bench/{}".format(url, bench_id))


def run_bench(args, unknown, url):
    """Runs the benchmark."""
    def to_kebab_case(name):
        return name.replace('_', '-')

    config = {}
    for key, value in vars(args).items():
        if value is not None:
            config[to_kebab_case(key)] = value

    i = 0
    while i < len(unknown):
        arg = unknown[i]
        if '=' in arg:
            arg, value = arg.split('=')
            i += 1
        else:
            value = unknown[i+1]
            i += 2

        if arg.startswith('--'):
            arg = arg[2:]
        if '.' in arg:
            parts = arg.split('.')
            obj = config
            for part in parts[:-1]:
                if part not in obj:
                    obj[part] = {}
                obj = obj[part]
            obj[to_kebab_case(parts[-1])] = value
        else:
            config[to_kebab_case(arg)] = value

    bench_id = start_bench(config, url)
    if bench_id is None:
        print("Failed to start benchmark")
        sys.exit(1)

    curses.noecho()
    curses.cbreak()

    try:
        while True:
            if not report_progress(bench_id, url):
                break
            time.sleep(1)
    except KeyboardInterrupt:
        stop_bench(bench_id, url)
        sys.exit(1)
    finally:
        curses.nocbreak()
        curses.echo()
        curses.endwin()

    report_result(bench_id, url)


if __name__ == '__main__':
    def percentage(value):
        if value[-1] != '%':
            raise argparse.ArgumentTypeError(str(value) + " is not a valid percentage value")
        try:
            return int(value[:-1])
        except ValueError:
            raise argparse.ArgumentTypeError(str(value) + " is not a valid percentage value")

    host = os.getenv('ATOMIX_HOST', 'localhost')
    port = int(os.getenv('ATOMIX_PORT', '5678'))

    address = "{}:{}".format(host, port)
    url = "http://{}".format(address)

    types = _get_bench_types(url)

    def to_kebab_case(option):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', option)
        return re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()

    parser = argparse.ArgumentParser(description="Run an Atomix benchmark")
    subparsers = parser.add_subparsers(dest='type', help="The type of benchmark to run")
    for type in _get_bench_types(url):
        subparser = subparsers.add_parser(type, help="{} benchmark".format(type))
        for optname, opttype in _get_bench_options(type, url).items():
            if optname in ('type', 'benchId'):
                continue
            if opttype == 'boolean':
                subparser.add_argument('--{}'.format(to_kebab_case(optname)), action='store_true', default=False)
            elif opttype != 'object':
                subparser.add_argument('--{}'.format(to_kebab_case(optname)), type=str, default=None)

    args, unknown = parser.parse_known_args()

    stdscr = curses.initscr()
    run_bench(args, unknown, url)
