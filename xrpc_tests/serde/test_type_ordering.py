import unittest
from typing import Generic, TypeVar, Dict, List, Union

from dataclasses import dataclass

T = TypeVar('T')


@dataclass
class GenericDC(Generic[T]):
    a: T


class OrderingTest(unittest.TestCase):
    def test_order(self):
        pass #sorted([Dict[int, int], List[int], GenericDC[int]])
