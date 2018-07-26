from multiprocessing import Pool

import argparse

from xrpc.actor import run_server
from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc.logging import logging_parser, cli_main


def run_server_a(b_addr):
    run_server(BroadcastClientRPC(b_addr), ['udp://127.0.0.1:7483'])


def run_server_b(c_addr):
    run_server(BroadcastRPC(), ['udp://127.0.0.1:7482'])


def main(bind, **kwargs):
    with Pool(2) as p:
        a = p.apply_async(run_server_a, args=(('127.0.0.1', 7482),))
        b = p.apply_async(run_server_b, args=(('127.0.0.1', 7482),))

        a.get()
        b.get()


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
