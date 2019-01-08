"""Main driver to process unprocessed logfiles and write out the result file.
"""
import sys
import argparse
import logging
import gc

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
    args = parser.parse_args(argv)
    log_level = logging.DEBUG if args.debug else logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger(__name__)
    logger.info("argv=%r", argv)
    logger.info("args=%r", args)    
    return args


def main(argv=None):
    """Console script entry point"""
    args = parse_args(argv)
    files = get_files(args.dir, args.maxdays)
    n = 0
    if args.max_files > 0:
        n = iterate_until_n_succeed(lambda file: process_file(file, args.database), 
                                    files, args.max_files)
    if args.max_files == 0 or n > 0:
        if n > 0:
            gc.collect() # Try to keep memory overhead down
        write_combination_file(output=args.output, database=args.database)
