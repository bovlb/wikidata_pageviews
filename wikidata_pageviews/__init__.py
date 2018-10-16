"""Main driver to process unprocessed logfiles and write out the result file.
"""
import sys
import argparse
import logging

from .process_log import get_files, process_file
from .util import iterate_until_n_succeed
from .constants import *
from .dump import write_combination_file

def parse_args(argv=None):
    """Wrapper for argparse.ArgumentParser()"""
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='Increases log level to INFO')
    parser.add_argument('-d', '--debug', action='store_true', help='Increases log level to DEBUG')    
    parser.add_argument('dir', type=Path, default=DEFAULT_DIR)
    parser.add_argument('--database', '--db', help='database name', default=DEFAULT_DATABASE)
    parser.add_argument("-n", "--max-files", type=int, default=10,
                        help="Maximum number of files to process")
    parser.add_argument("--maxdays", type=int, default=7, 
                        help="Maximum age of file to process in days")
    parser.add_argument("-o", '--output', type=Path, default=DEFAULT_OUTPUT,
                        help="File to write output to")
    logger = logging.getLogger(__name__)
    args = parser.parse_args(argv)
    log_level = logging.DEBUG if args.debug else logging.INFO if args.verbose else logging.WARNING
    logger.setLevel(log_level)
    logger.info(argv)
    logger.info(args)    
    return args


def main(argv=None):
    """Console script entry point"""
    args = parse_args(argv)
    files = get_files(args.dir, args.max_days)
    iterate_until_n_succeed(lambda file: process_file(file, args.database), 
                            files, args.max_files)
    write_combination_file(output=args.output, database=args.database)