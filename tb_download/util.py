
from rich.console import Console
from rich.highlighter import RegexHighlighter
from rich.theme import Theme

from rich.traceback import install

class MyHighlighter(RegexHighlighter):
    """Style!."""

    base_style = "base."
    highlights = [r"(?P<tag>^\s?[^:\s]+:)",
                  r"(?P<true>True)",
                  r"(?P<false>False)",
                  r"(?P<date>\d+-\d+-\d+.\d+:\d+:\d+.[\d:]*)",
                  r"(?P<path>`.+`)",
                  ]


install(show_locals=True)
theme = Theme({"base.tag": "bold yellow",
               "base.true": "bold green",
               "base.false": "bold red",
               "base.date": "italic cyan",
               "base.path": "bold italic orange1"
               })
console = Console(highlighter=MyHighlighter(), theme=theme, markup=True)


def print_exception(*args, **kargs): # noqa: D103
    return console.print_exception(*args, **kwargs)


def info(*args, **kwargs):  # noqa: D103
    return console.print(*args, **kwargs)


def warning(*args, **kwargs):  # noqa: D103
    return console.print(*args, **kwargs, style="orange1")


def error(*args, **kwargs):  # noqa: D103
    return console.print(*args, **kwargs, style="red")


def rule(*args, **kwargs):  # noqa: D103
    return console.rule(*args, **kwargs, style="purple")
