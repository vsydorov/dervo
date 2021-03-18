#!/usr/bin/env python3
"""
Dervo 0.005
Runs one of the experiments in the exp folder

Usage:
    run_exp.py <path> [options] [--] [<add_args> ...]

Options:
    --log <level>           Level of stdout logging [default: INFO]
    --lformat <level>       Which formatter to use [default: extended]
    --raw                   Run code inside the 'code_root' folder directly.
                                (Disables commit flag)
    When not raw:
        --commit <hash>     Check out this commit [default: HEAD]
"""
from docopt import docopt

import vst

from dervo import experiment, snippets


def main(args):
    # Define proper formatter right away
    loglevel_int: int = snippets.docopt_loglevel(args.get('--log'))
    log = vst.reasonable_logging_setup(loglevel_int, args['--lformat'])

    log.info('|||-------------------------------------------------------|||')
    log.info('    Start of Dervo experiment. STDOUT loglevel: {}'.format(
        snippets.loglevel_int_to_str(loglevel_int)))
    path = snippets.find_exp_path(args['<path>'])
    co_commit = None if args['--raw'] else args['--commit']
    experiment.run_experiment(path, args['<add_args>'], co_commit)
    log.info('    End of Dervo experiment')
    log.info('|||-------------------------------------------------------|||')


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
