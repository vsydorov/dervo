#!/usr/bin/env python3
"""
Usage:
    run_clust.py (create|run) <path> [options] [--] [<add_args> ...]

Options:
    Logging:
        --log <level>           Level of stdout logging [default: INFO]
        --lformat <level>       Which formatter to use [default: extended]

    Experiments:
        --raw                   Run code inside the 'code_root' folder directly.
                                  (Disables --commit flag)
        --commit <hash>         Check out this commit [default: HEAD]
                                  (Mutually exclusive with --raw)
"""
from pathlib import Path
from docopt import docopt
import vst
from dervo.snippets import (docopt_loglevel, loglevel_int_to_str, find_exp_path)
from dervo import experiment


def main(args):
    # Define proper formatter right away
    loglevel_int: int = docopt_loglevel(args.get('--log'))
    log = vst.reasonable_logging_setup(loglevel_int, args['--lformat'])
    log.info('STDOUT loglevel: {}'.format(loglevel_int_to_str(loglevel_int)))

    if args['create']:
        # path -> path to experiment
        path = find_exp_path(args['<path>'])
        co_commit = None if args['--raw'] else args['--commit']
        experiment.prepare_cluster_experiment(path, args['<add_args>'], co_commit)
    elif args['run']:
        # path -> path to workfolder
        workfolder_w_commit = Path(args['<path>'])
        experiment.run_cluster_experiment(workfolder_w_commit, args['<add_args>'])
    else:
        raise NotImplementedError()


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
