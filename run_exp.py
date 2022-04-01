#!/usr/bin/env python3
"""
Runs one of the experiments in the exp folder

Usage:
    run_exp.py <path> [<hash>] [options] [--] [<add_args> ...]

Options:
    -f, --fake              Create workfolder, but don't execute

    Logging:
        --log <level>       Level of stdout logging. [default: INFO]
        --lformat <level>   Which formatter to use. [default: shorter]
"""
from docopt import docopt

import vst

from dervo.experiment import run_experiment


def main(args):
    # Define proper formatter right away
    loglevel_int: int = vst.docopt_loglevel(args.get('--log'))
    log = vst.reasonable_logging_setup(loglevel_int, args['--lformat'])
    log.info('STDOUT loglevel: {}'.format(vst.loglevel_int_to_str(loglevel_int)))
    log.info('|||-------------------------------------------------------|||')
    log.info('    Start of Dervo experiment')
    commit = args['<hash>'] if args['<hash>'] is not None else 'RAW'
    run_experiment(args['<path>'], commit,
            args['<add_args>'], args['--fake'])
    log.info('    End of Dervo experiment')
    log.info('|||-------------------------------------------------------|||')


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
