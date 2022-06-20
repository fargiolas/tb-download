# Copyright (c) 2022 Filippo Argiolas <filippo.argiolas@ca.infn.it>.
#
# a simple script to download timeseries data from ThingsBoard
# barely tested, poor error checking, ugly code, use at your own risk
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
"""Colored debug messages, rich styles, etc."""

import argparse
from rich.console import Console
from rich.highlighter import RegexHighlighter
from rich.theme import Theme


class TBDownloadHighlighter(RegexHighlighter):
    """Style!."""

    base_style = "base."
    highlights = [r"(?P<tag>^\s?[^:\s]+:)",
                  r"(?P<true>True)",
                  r"(?P<false>False)",
                  r"(?P<date>\d+-\d+-\d+.\d+:\d+:\d+.[\d:]*)",
                  r"(?P<path>`.+`)",
                  ]


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
