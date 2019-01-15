import os
from typing import Tuple

import tfci_core.opcodes
import tfci_std.opcodes
from test_tfci.compiler.test_compiler import load_opcodes
from test_tfci.db.fixtures import Etcd3ServerFixture, callback_fixture
from tfci.dsl.compiler import compiler_compile_text
from tfci.dsl.struct import ProgramDefinition
from tfci.dsm.struct import ThreadContext, StackFrame
from tfci.dsm.rt import ProgramPages, OpcodeDefinition
from tfci.db.ops import Transaction
from tfci_core.daemons.worker.worker import ExecutionEngine

TEST_IDENT = 'test_ident'


def load_test_program(filename: str, supported_opcodes: OpcodeDefinition) -> Tuple[str, str, ProgramDefinition]:
    filename = os.path.join(os.path.dirname(__file__), filename)
    with open(filename) as f_in:
        text = f_in.read()

        return filename, text, compiler_compile_text(filename, text, supported_opcodes)


class TestProgramPages(ProgramPages):

    def __init__(self, filename, db, opcodes: OpcodeDefinition):
        super().__init__(db, opcodes)
        self.filename = filename

    def __getitem__(self, item):
        if not self._cache:
            self.p_filename, self.p_text, self._cache = load_test_program(self.filename, self.opcodes)

        return self._cache[item]


class TestExecutionEngine(Etcd3ServerFixture):
    def test_nonexistent_stack_arg(self):
        db, lease = self._db_lease()
        opcodes = load_opcodes(tfci_core.opcodes, tfci_std.opcodes)
        eng = ExecutionEngine(
            TEST_IDENT,
            lease,
            opcodes,
            TestProgramPages('dasm_ep_2_plus_2.txt', db, opcodes),
            db
        )

        sf1 = StackFrame.new(
            'sf1'
        )

        t1 = ThreadContext.new(
            't1',
            'ep_2_plus_2',
            [sf1.id]
        )

        tx = sf1.create().merge(t1.create())

        ok, _, _ = tx.exec(db)

        self.assertTrue(ok, "Transaction must execute successfully")

        ok, upd = eng.step(t1)

        self.assertTrue(ok, "Step must execute succ I")

        ok, upd = eng.step(t1)

        self.assertFalse(ok, "Step must execute succ II")

    def test_ok(self):
        db, lease = self._db_lease()
        opcodes = load_opcodes(tfci_core.opcodes, tfci_std.opcodes)

        eng = ExecutionEngine(
            TEST_IDENT,
            lease,
            opcodes,
            TestProgramPages('dasm_ep_2_plus_2.txt', db, opcodes),
            db
        )

        sf1 = StackFrame.new(
            'sf1',
            {'x': 1}
        )

        t1 = ThreadContext.new(
            't1',
            'ep_2_plus_2',
            [sf1.id]
        )

        ok, _, _ = sf1.create().merge(t1.create()).exec(db)

        self.assertTrue(ok, "Transaction must execute successfully")

        ok, upd = eng.step(t1)

        self.assertTrue(ok, "Step must execute succ I")

        ok, upd = eng.step(t1)

        self.assertTrue(ok, "Step must execute succ II")

        ok, upd = eng.step(t1)

        self.assertTrue(ok, "Step must execute succ III")

        tx1 = StackFrame.load_exists('sf1')

        sf: StackFrame

        ok, (sf, ), _ = tx1.exec(
            db
        )

        self.assertTrue(ok, "StackFrame must exist")

        self.assertEqual(sf.vals['x'], 3, "Return value must be equal")

        tx2 = ThreadContext.load_exists('t1')

        ok, (tctx,), _ = tx2.exec(
            db
        )

        self.assertFalse(ok, "ThreadContext must not exist")

    def test_multi_proc(self):
        db, lease = self._db_lease()

        opcodes = load_opcodes(tfci_core.opcodes, tfci_std.opcodes)

        returns = []

        def callback(*args, **kwargs):
            returns.append((args, kwargs))

        cb_fix = callback_fixture('callback_fix', callback)

        opcodes[cb_fix.name] = cb_fix

        eng = ExecutionEngine(
            TEST_IDENT,
            lease,
            opcodes,
            TestProgramPages('dasm_multi_proc.txt', db, opcodes),
            db
        )

        PAYLOAD = list(range(5))

        sf1 = StackFrame.new(
            'sf1',
            {
                'v': PAYLOAD,
                'x': 'callback_fn',
                '_ret': 'ret_fn',
            }
        )

        t1 = ThreadContext.new(
            't1',
            'fork_loop',
            [sf1.id]
        )

        ok, _, _ = sf1.create().merge(t1.create()).exec(db)

        tcxs = [t1]

        while len(tcxs):
            for t in tcxs:
                ok, upd = eng.step(t)

                self.assertTrue(ok, 'Must be OK')
                tcxs = ThreadContext.load_all(db).values()

        self.assertEqual(sorted([y['v'] for _, y in returns]), PAYLOAD)

    def tearDown(self):
        db = self._settings().get_db()
        for x, kv in db.get_all():
            print(kv.key, x)
            db.delete(kv.key)
