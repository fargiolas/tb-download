"""Utils."""

import argparse
from rich.console import Console
from rich.highlighter import RegexHighlighter
from rich.theme import Theme
from rich.traceback import install


class TBDownloadHighlighter(RegexHighlighter):
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
console = Console(highlighter=TBDownloadHighlighter(), theme=theme, markup=True)


def print_exception(*args, **kwargs):  # noqa: D103
    return console.print_exception(*args, **kwargs)


def info(*args, **kwargs):  # noqa: D103
    return console.print(*args, **kwargs)


def warning(*args, **kwargs):  # noqa: D103
    return console.print(*args, **kwargs, style="orange1")


def error(*args, **kwargs):  # noqa: D103
    return console.print(*args, **kwargs, style="red")


def rule(*args, **kwargs):  # noqa: D103
    return console.rule(*args, **kwargs, style="purple")


class ArgParseHighlighter(RegexHighlighter):
    """Style for argparse help message."""

    base_style = "base."
    highlights = [r"(?P<short>-\w)[^\w]",
                  r"(?P<long>--[\w-]+)[^\w]",
                  r"--[\w-]+\s(?P<longarg>[A-Z_]+)",
                  r"-[\w]\s(?P<shortarg>[A-Z_]+)",
                  r"(?P<usage>usage:)",
                  r"(?P<default>default:)",
                  r"\(default:\s(?P<value>.*)\)",
                  r"^usage:\s(?P<cmd>[\w_-]+)"
                  ]


argparse_theme = Theme({"base.short": "bold yellow",
                        "base.long": "bold yellow",
                        "base.shortarg": "bold magenta",
                        "base.longarg": "bold purple",
                        "base.usage": "bold",
                        "base.cmd": "underline bold cyan",
                        "base.default": "orange1",
                        "base.value": "italic magenta",
                        })
argparse_console = Console(highlighter=ArgParseHighlighter(),
                           theme=argparse_theme, markup=True)


class RichArgumentParser(argparse.ArgumentParser):
    """Apply rich styling to argparse messages."""
    def _print_message(self, message: str, file=None):
        argparse_console.print(message)
