#!/usr/bin/env python3
"""
Runs one of the experiments in the exp folder

Usage:
    run_exp.py <path> [options] [--] [<add_args> ...]

Options:
    Logging:
        --log <level>           Level of stdout logging [default: INFO]
        --lformat <level>       Which formatter to use [default: extended]

    Commit:
        --commit <hash>         Check out this commit. [default: RAW]
"""
from docopt import docopt

import vst

from dervo.experiment import run_experiment
from dervo.snippets import (docopt_loglevel, loglevel_int_to_str)


def main(args):
    # Define proper formatter right away
    loglevel_int: int = docopt_loglevel(args.get('--log'))
    log = vst.reasonable_logging_setup(loglevel_int, args['--lformat'])
    log.info('STDOUT loglevel: {}'.format(loglevel_int_to_str(loglevel_int)))
    run_experiment(args['<path>'], args['<add_args>'], args['--commit'])


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
