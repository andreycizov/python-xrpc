import argparse
import unittest
from typing import Optional, Dict, List

from dataclasses import dataclass, field

from xrpc.cli import Parsable, ParsableConf


@dataclass
class A(Parsable):
    field_unit: Optional[int] = 0
    field_unit_b: Optional[int] = 0
    field_unit_c: Optional[int] = 0

    @classmethod
    def overrides(cls) -> Dict[str, ParsableConf]:
        return {
            'field_unit_b': ParsableConf(['-fu', '-fu3']),
            'field_unit_c': ParsableConf(['-fu2'], type=int),
        }


@dataclass
class B(Parsable):
    field_unit: Optional[int] = 0
    flag: bool = False
    items: Optional[List[int]] = field(default_factory=list)


class TestCli(unittest.TestCase):
    def get_parser(self):
        return argparse.ArgumentParser()

    def parse(self, parser: argparse.ArgumentParser, *args):
        return vars(parser.parse_args(args))

    def test_simple_a(self):
        p = self.get_parser()

        A.parser('x', p)

        # self.assertEqual({'field_unit': 5}, self.parse(p, '-h'))
        self.assertEqual(A(5, '5'), A.from_parser('x', self.parse(p, '--x_field_unit', '5', '-fu', '5')))
        self.assertEqual(({}, A(5, field_unit_c=5)), A.from_parser('x', self.parse(p, '--x_field_unit', '5', '-fu2', '5'), forget_other=False))

    def test_simple_b(self):
        p = self.get_parser()

        B.parser('x', p)

        # self.assertEqual({'field_unit': 5}, self.parse(p, '-h'))
        self.assertEqual(B(5), B.from_parser('x', self.parse(p, '--x_field_unit', '5')))
        self.assertEqual(B(5, flag=True), B.from_parser('x', self.parse(p, '--x_field_unit', '5', '--x_flag')))
        self.assertEqual(({}, B(5)), B.from_parser('x', self.parse(p, '--x_field_unit', '5'), forget_other=False))
