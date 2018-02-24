from tfci.daemon import Daemon


class TimeDaemon(Daemon):
    name = 'time'

    @classmethod
    def arguments(cls, args):
        pass

    def run(self):
        # look at schedule changes; fire up the required tasks when their time has come.
        pass
