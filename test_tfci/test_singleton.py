import unittest
from pprint import pprint

from tfci.settings import Settings

PLUGINS = [
    'tfci.plugin.core',
    'tfci.plugin.docker',
    'tfci.plugin.http',
    'tfci.plugin.time',
]


class TestSingleton(unittest.TestCase):
    # def test_tables(self):
    #     s = Settings(PLUGINS)
    #
    #     tables = s.get_schema()
    #
    #     pprint(tables)

    def test_daemons(self):
        s = Settings(PLUGINS)

        tables = s.get_daemons()

        pprint(tables)

