import argparse
import unittest
from typing import Optional

from dataclasses import dataclass

from xrpc.cli import Parsable


@dataclass
class B(Parsable):
    field_unit: Optional[int] = 0


class TestCli(unittest.TestCase):
    def get_parser(self):
        return argparse.ArgumentParser()

    def parse(self, parser: argparse.ArgumentParser, *args):
        return vars(parser.parse_args(args))

    def test_simple(self):
        p = self.get_parser()

        B.parser('x', p)

        # self.assertEqual({'field_unit': 5}, self.parse(p, '-h'))
        self.assertEqual(({}, B(5)), B.from_parser('x', self.parse(p, '--x_field_unit', '5')))
