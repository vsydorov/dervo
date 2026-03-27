#!/usr/bin/env python3
"""
Enter point for dervo 0.2

Runs one of the experiments in the exp folder

Usage:
    run.py <path> [<hash>] [options] [--] [<add_args> ...]

Options:
    Logging:
        --log <level>       Level of stdout logging. [default: INFO]
        --lformat <level>   Which formatter to use. [default: shorter]
"""

from docopt import docopt

from dervo.experiment import run_experiment
from dervo.logging import docopt_loglevel, loglevel_int_to_str, reasonable_logging_setup


def main(args):
    loglevel_int: int = docopt_loglevel(args.get("--log"))
    log = reasonable_logging_setup(loglevel_int, args["--lformat"])
    log.info("STDOUT loglevel: {}".format(loglevel_int_to_str(loglevel_int)))
    log.info("|||-------------------------------------------------------|||")
    log.info("    Start of Dervo experiment")
    run_experiment(args["<path>"], args["<hash>"], args["<add_args>"])
    log.info("    End of Dervo experiment")
    log.info("|||-------------------------------------------------------|||")


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)
