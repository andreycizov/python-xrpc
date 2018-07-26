import argparse

from xrpc.actor import run_server
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.logging import logging_parser, cli_main


def main(bind, **kwargs):
    rpc = ExemplaryRPC()
    run_server(rpc, bind)


def parser():
    parser = argparse.ArgumentParser()

    logging_parser(parser)

    parser.add_argument(
        '-b',
        '--bind',
        dest='bind',
        action='append',
        default=[],
        help='Bind to these addresses'
    )

    return parser


if __name__ == '__main__':
    cli_main(main, parser())
