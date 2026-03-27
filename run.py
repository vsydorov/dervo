#!/usr/bin/env python3
"""
Enter point for dervo 0.2

Runs an experiment defined by <path> (and its parents, when inherited)

Usage:
    run.py <path> [<hash>] [options] [--] [<add_args> ...]

Options:
    Default stream logging:
        --loglvl <level>     Level of logging (str/int). [default: INFO]
        --logform <name>     Formatter preset. [default: shorter]
        --logstream <name>   Stream to log to (stderr/stdout). [default: stderr]
"""

from docopt import docopt

from dervo.experiment import run_experiment
from dervo.logging import parse_loglevel, logging_init


def main(args: docopt.Dict):
    loglevel_int, loglevel_str = parse_loglevel(args.get("--loglvl"))
    log = logging_init(loglevel_int, args["--logform"], args["--logstream"])
    log.info("STDOUT loglevel: {}/{}".format(loglevel_int, loglevel_str))
    log.info("|||-------------------------------------------------------|||")
    log.info("    Start of Dervo experiment")
    run_experiment(args["<path>"], args["<hash>"], args["<add_args>"])
    log.info("    End of Dervo experiment")
    log.info("|||-------------------------------------------------------|||")


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)
