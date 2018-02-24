from tfci_core.daemons2 import logger


def trace(f):
    trace_depth = 0

    import sys
    import inspect
    import linecache
    import os

    def log_frame(frame, why, arg):
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        bname = os.path.basename(filename)

        line = linecache.getline(filename, lineno).rstrip()

        logger.info(f'{why.upper()} {bname}:{lineno} {line}')

    def globaltrace(frame, why, arg):
        nonlocal trace_depth
        # print(frame, dir(frame))
        if why in ["call", 'c_call']:

            # Parent frame details
            p_func = frame.f_back.f_code.co_name
            p_file = frame.f_back.f_code.co_filename
            p_lineinfo = frame.f_back.f_lineno
            p_class = ''
            p_module = ''
            if 'self' in frame.f_back.f_locals:
                p_class = frame.f_back.f_locals['self'].__class__.__name__
                p_module = frame.f_back.f_locals['self'].__class__.__module__

            # Current frame details
            c_func = frame.f_code.co_name
            c_file = frame.f_code.co_filename
            c_lineinfo = frame.f_lineno
            c_class = ''
            c_module = ''
            if 'self' in frame.f_locals:
                c_class = frame.f_locals['self'].__class__.__name__
                c_module = frame.f_locals['self'].__class__.__module__
            # Order is Caller -> Callee

            logger.info(
                f'{why.upper()} {c_file}:{c_lineinfo}\n {p_module}.{p_class}.{p_func} -> {c_module}.{c_class}.{c_func}')
            trace_depth = trace_depth + 1

            # if why == 'call':
            #     return localtrace
        elif why == "line":
            log_frame(frame, why, arg)
        elif why in ["return", 'c_return']:
            trace_depth = trace_depth - 1
            # function return event
            log_frame(frame, why, arg)
        else:
            print('UNKNOWN', why)
        return globaltrace

    def _f(*args, **kwds):
        sys.settrace(globaltrace)
        result = f(*args, **kwds)
        sys.settrace(None)
        return result

    return _f