from typing import NamedTuple


class ObjR3(NamedTuple):
    """This is kept in another module to be sure that ForwardRef to another module works correctly"""
    i: int
