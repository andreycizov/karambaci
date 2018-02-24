import inspect
from pprint import pprint
from typing import Any

from tfci.opcode import OpArg, RefOpArg

if __name__ == '__main__':
    def a(a: int, b, c, d=3, f=4, *args: RefOpArg[int], f1: RefOpArg[Any] = 66, **kwargs: OpArg[Any]):
        pass

    # we check if we can run this function with the given parameters ?


    pprint(inspect.getfullargspec(a))
