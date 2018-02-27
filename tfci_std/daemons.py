from argparse import ArgumentParser
from uuid import uuid4

from tfci.daemon import Daemon
from tfci.db.manager import EntityManager
from tfci.dsm.struct import StackFrame, ThreadContext
from tfci_std.struct import FrozenThreadContext


class StackManager(EntityManager):
    entity = StackFrame.__name__
    model = StackFrame

    @classmethod
    def arguments_add(cls, args: ArgumentParser):
        super(StackManager, cls).arguments_add(args)

        def stack_var(v):
            a, b = str(v).split('=', 1)

            return a, b

        args.add_argument(
            '-v',
            dest='stack_const',
            action='append',
            default=[],
            type=stack_var,
            help="Add a constant value on the stack"
        )

        args.add_argument(
            '-e',
            dest='stack_exec',
            action='append',
            default=[],
            type=stack_var,
            help="Add a result value of a python expression on the stack",
        )

    def action_add(self, id, stack_const, stack_exec, **kwargs):
        if id is None:
            id = uuid4().hex
            print(f'Generated ID {id}')

        def check_in_stack(k):
            if k in stack:
                raise KeyError(f'{k} defined twice')

        stack = {}

        for k, v in stack_const:
            check_in_stack(k)
            stack[k] = v

        for k, v in stack_exec:
            check_in_stack(k)

            try:
                v = eval(compile(v, filename='<string>', mode='eval'))
                stack[k] = v
            except:
                print(f'While evaluating {k}={v}')
                raise

        a, b = StackFrame.new(id, stack).create(self.db)

        return [a], [b]


class ThreadContextManager(EntityManager):
    entity = ThreadContext.__name__
    model = ThreadContext


class FrozenThreadContextManager(EntityManager):
    entity = FrozenThreadContext.__name__
    model = FrozenThreadContext

    @classmethod
    def arguments_add(cls, args: ArgumentParser):
        super(FrozenThreadContextManager, cls).arguments_add(args)

        args.add_argument(
            '-S',
            dest='stacks',
            action='append',
            default=[],
            help="Add a SP on the stack"
        )

        args.add_argument(
            dest='ep',
            help="entry point",
        )

    def action_add(self, id, stacks, ep, **kwargs):
        a, b = self.model.new(id, ep, stacks).create(self.db)

        return [a] + [StackFrame.exists(self.db, s) for s in stacks], [b]


class ConfDaemon(Daemon):
    name = 'conf'
    description = 'stdlib shell utilities'
    is_daemon = False

    mgrs = {
        'stack': StackManager,
        'thread': ThreadContextManager,
        'frz': FrozenThreadContextManager,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = kwargs

    @classmethod
    def arguments(cls, args: ArgumentParser):
        utility_args = args.add_subparsers(
            title="entity",
            dest="entity",
            help="select the utility to run",
            metavar="ENTITY",
        )
        utility_args.required = True

        for k, v in cls.mgrs.items():
            v.arguments(utility_args.add_parser(k, help=k))

    def run(self):
        e = self.kwargs['entity']
        cls = self.mgrs[e]

        mgr = cls(self.db)

        mgr.action(**self.kwargs)
