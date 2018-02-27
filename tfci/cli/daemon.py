# daemon interface.
import argparse
import sys
import os
from typing import ClassVar, Type

from tfci.daemon import Daemon
from tfci.settings import Settings


def load_settings():
    return Settings.from_module(os.environ.get('TF_OPTS', 'tfci_opts'))


def main():
    settings = load_settings()

    parser = argparse.ArgumentParser(prog='tfci')

    daemons_list = settings.get_daemons()

    daemon_args = parser.add_subparsers(
        title="daemon",
        dest="daemon",
        help="select the daemon to start",
        metavar="DAEMON",
    )
    daemon_args.required = True

    for name, item in daemons_list:
        daemon = daemon_args.add_parser(name, help=f'{item.description} (version: {item.version})')
        item.arguments(daemon)

    options = parser.parse_args(sys.argv[1:])

    options = vars(options)

    daemon = options['daemon']
    del options['daemon']

    daemon_cls = [z for y, z in daemons_list if y == daemon][0]

    inst = daemon_cls(settings=settings, **options)  # type: Daemon
    inst.startup()
    try:
        settings.get_logger().getChild('cli.daemon').debug('Startup')
        inst.run()
        settings.get_logger().getChild('cli.daemon').debug('Stopped')
    finally:
        settings.get_logger().getChild('cli.daemon').debug('Tearing down')
        inst.teardown()


if __name__ == '__main__':
    main()
