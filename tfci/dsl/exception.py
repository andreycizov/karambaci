from typing import Optional

from pyparsing import lineno, col

from tfci.dsl.ast import Location


class CompilerException(Exception):
    def __init__(self, loc, reason, filename='<inmem>', text=None, subsequent: Optional['CompilerException'] = None):
        if isinstance(loc, Location):
            loc = loc.loc

        self.loc = loc
        self.reason = reason
        self.filename = filename
        self.text = text
        self.subsequent = subsequent

    @property
    def lineno(self):
        assert self.text is not None
        return lineno(self.loc, self.text)

    @property
    def col(self):
        assert self.text is not None
        return col(self.loc, self.text)

    @property
    def pos_str(self):
        if self.text:
            lineno_ = lineno(self.loc, self.text)
            col_ = col(self.loc, self.text)

            pos = f'{lineno_}:{col_}'
        else:
            pos = self.loc

        return f'{self.filename}:{pos}'

    def build_excerpt(self):
        # lineno = None
        # col = None

        if self.text:
            lineno_str = f'{self.lineno:d}: '
            excerpt_str = lineno_str + self.text.split('\n')[self.lineno - 1]
            ptr_str = (' ' * len(lineno_str)) + (' ' * (self.col - 1)) + '^'

            exc = f"""    {excerpt_str}
    {ptr_str}"""
        else:
            exc = """
<text unavailable>"""

        r = f"""    {self.pos_str}
{exc}"""
        return r

    def __str__(self):
        r = f"""
{self.reason}
{self.build_excerpt()}"""

        if self.subsequent:
            r += f"""{str(self.subsequent)}"""

        return r

    def with_filename(self, filename='<inmem>'):
        return CompilerException(
            self.loc, self.reason, filename, self.text,
            None if self.subsequent is None else self.subsequent.with_filename(filename)
        )

    def with_text(self, text):
        return CompilerException(
            self.loc, self.reason, self.filename, text,
            None if self.subsequent is None else self.subsequent.with_text(text)
        )