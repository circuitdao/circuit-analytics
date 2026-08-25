"""Options reaching the code that acts on them.

Threading a flag from argparse down to a renderer is exactly the kind of plumbing that fails
silently: -f/--full was accepted, stored, and passed into the block path, which then called
the renderer without it. The output looked like the flag did nothing.
"""

import inspect

from circuit_analytics.scanner import cli


def _source(func) -> str:
    return inspect.getsource(func)


def test_bundle_mode_passes_the_render_options():
    src = _source(cli._parse)
    assert "render_bundle(" in src
    for option in ("full=full", "details=details"):
        assert option in src, option


def test_block_mode_passes_the_render_options():
    src = _source(cli._parse_block)
    assert "render_block(" in src
    for option in ("full=full", "details=details"):
        assert option in src, option


def test_both_modes_pass_verbose_and_colour_too():
    for func in (cli._parse, cli._parse_block):
        src = _source(func)
        assert "verbose=verbose" in src, func.__name__
        assert "colour=colour" in src, func.__name__


def test_every_renderer_option_is_threaded_from_the_command():
    """Whatever `parse` accepts should reach a renderer, or it is a dead flag."""
    src = _source(cli.main)
    for option in ("args.full", "args.details", "args.verbose", "args.no_color"):
        assert option in src, option
