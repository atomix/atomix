from __future__ import print_function
import requests
import argparse
import json
import time
import sys
import curses

try:
    from terminaltables import AsciiTable
except ImportError:
    AsciiTable = None


def start_bench(config, url):
    response = requests.post("{}/v1/bench".format(url), data=json.dumps(config), headers={'content-type': 'application/json'})
    if response.status_code != 200:
        print("Failed to start test")
        return
    return response.text


def _report_text(processes):
    lines = []
    for id, stats in processes.items():
        if stats['time'] > 0:
            time = round(stats['time'] / float(1000), 2)
            ops = round(stats['operations'] / (stats['time'] / float(1000)), 2)
            rps = round(stats['reads'] / (stats['time'] / float(1000)), 2)
            wps = round(stats['writes'] / (stats['time'] / float(1000)), 2)

            lines.append("process: {}, ops: {}, reads: {}, writes: {}, events: {}, time: {}s, ops/sec: {}, reads/sec: {}, writes/sec: {}".format(
                id, stats['operations'], stats['reads'], stats['writes'], stats['events'], time, ops, rps, wps))
    return '\n'.join(lines)


def _create_table(data):
    """Creates a table from the given data."""
    table = AsciiTable(data)
    table.inner_column_border = False
    table.inner_row_border = False
    table.outer_border = False
    table.inner_heading_row_border = False
    table.padding_right = 4
    return str(table.table)

def _report_table(processes):
    data = [['PROCESS', 'OPS', 'READS', 'WRITES', 'EVENTS', 'TIME', 'OPS/S', 'READS/S', 'WRITES/S']]
    for id, stats in processes.items():
        if stats['time'] > 0:
            time = round(stats['time'] / float(1000), 2)
            ops = round(stats['operations'] / (stats['time'] / float(1000)), 2)
            rps = round(stats['reads'] / (stats['time'] / float(1000)), 2)
            wps = round(stats['writes'] / (stats['time'] / float(1000)), 2)

            data.append([
                id,
                stats['operations'],
                stats['reads'],
                stats['writes'],
                stats['events'],
                time,
                ops,
                rps,
                wps
            ])

    return _create_table(data)


def _report_processes(processes):
    if AsciiTable is not None:
        text = _report_table(processes)
    else:
        text = _report_text(processes)
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

        if progress['state'] == 'COMPLETE':
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


def run_bench(args):
    """Runs the benchmark."""
    address = "{}:{}".format(args.host, args.port)
    url = "http://{}".format(address)

    config = {
        'type': 'map',
        'operations': args.ops,
        'concurrency': args.concurrency,
        'write-percentage': args.writes,
        'num-keys': args.keys,
        'key-length': args.key_length,
        'num-values': args.values,
        'value-length': args.value_length,
        'include-events': args.include_events,
        'deterministic': not args.non_deterministic
    }
    if args.protocol is not None:
        config['protocol'] = {
            'type': args.protocol,
            'group': args.group
        }

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

    parser = argparse.ArgumentParser(description="Run an Atomix benchmark")
    parser.add_argument('--host', type=str, default='localhost', help="The bench host through which to run the test")
    parser.add_argument('--port', type=int, default=5678, help="The HTTP port to use to control the test")
    parser.add_argument('--protocol', '-p', default=None, choices=['multi-raft', 'multi-primary', 'multi-log'], help="The protocol on which to run the test")
    parser.add_argument('--group', '-g', type=str, default=None, help="The partition group on which to run the test")
    parser.add_argument('--concurrency', '-c', type=int, default=1, help="The number of concurrent operations to execute from a single process")
    parser.add_argument('--ops', '-o', type=int, default=10000, help="The number of operations to perform")
    parser.add_argument('--writes', '-w', type=percentage, default='100%', help="The percentage of operations to be writes")
    parser.add_argument('--keys', type=int, default=1000, help="The total number of unique keys to write")
    parser.add_argument('--key-length', type=int, default=64, help="The length of each unique key")
    parser.add_argument('--values', type=int, default=1000, help="The total number of unique values to write")
    parser.add_argument('--value-length', type=int, default=1024, help="The length of each unique value")
    parser.add_argument('--include-events', action='store_true', default=False, help="Whether to include events in the test")
    parser.add_argument('--non-deterministic', action='store_true', default=False, help="Whether to partition operations non-deterministically")

    args = parser.parse_args()

    stdscr = curses.initscr()
    run_bench(args)
