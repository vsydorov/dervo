#!/usr/bin/env python3
"""
Enter point for dervo 0.2

Runs an experiment defined by <path> (and its parents, when inherited)

Usage:
    run.py <path> [-c <hash>] [options] [<args_add> ...]

Options:

    -c <hash, --commit <hash>  Experiment hash to check out.

    --compat <version>         Compatability with old dervo version

    Default stream logging:
        --loglvl <level>       Level of logging (str/int). [default: INFO]
        --logform <name>       Formatter preset. [default: shorter]
        --logstream <name>     Stream to log to (stderr/stdout). [default: stderr]

    Convention for <args_add>:
        Some tools (like Lightning DDP) rerun the experiment with additional
        ARGV args. We extend the argv with ['--', '---guard'] and add special
        processing for arguments (if any) detected after.
"""

from docopt import docopt

from dervo.experiment import run_experiment
from dervo.logging import logging_init, parse_loglevel


def main(args):
    loglevel_int, loglevel_str = parse_loglevel(args.get("--loglvl"))
    log = logging_init(loglevel_int, args["--logform"], args["--logstream"])
    log.info("STDOUT loglevel: {}/{}".format(loglevel_int, loglevel_str))
    log.info("|||-------------------------------------------------------|||")
    log.info("    Start of Dervo experiment")
    run_experiment(
        args["<path>"], args["--commit"], args["--compat"], args["<args_add>"]
    )
    log.info("    End of Dervo experiment")
    log.info("|||-------------------------------------------------------|||")


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)
