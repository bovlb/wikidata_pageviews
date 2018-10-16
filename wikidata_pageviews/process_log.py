"""Main functionality to process hourly logfile

Example::
    qid_views = process_log_entries(read_log(my_filename))
"""

import gzip
#from collections import defaultdict, Counter
from typing import NamedTuple
#import os
import re
import sys
from textwrap import dedent
import logging
#import itertools
import io
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta

from tenacity import retry, wait_random_exponential
import toolforge

from .util import *
from .constants import *
from .project import database_from_project_name

FILE_RE = re.compile(r'^pageviews-\d{8}--d{6}\.gz$')

def connect_to_database(dbname, **kargs):
    """Convenience wrapper for ``toolforge.connect`` that handles credentials.
    
    Note that, when used as a context manager, the returned object is not a connection,
    but rather a cursor.  
    
    Args:
        dbname: Database name, optionally without ``_p`` suffix
        **kargs: Keyword arguments to pass down e.g. ``local_infile=1``
        
    Returns:
        conn: Connection
    """
    conn = toolforge.connect(dbname, 
                             #host=os.environ['MYSQL_HOST'],
                             #user=os.environ['MYSQL_USERNAME'],
                             #password=os.environ['MYSQL_PASSWORD'],
                             **kargs)
    return conn
    

def convert_titles_to_qids(dbname, titles):
    """Convert set of log entries into Wikidata ids.
    
    Args:
        dbname: Name of database suitable for passing to ``toolforge.connect()``
        titles: Iterable of page titles.
        
    Returns:
        qids: Parallel list of Wikidata ids (or None)
    """
    titles = list(titles) # reiterable
    results = dict()
    logger = logging.getLogger(__name__)
    
    logger.info(f"dbname={dbname} titles={len(titles)}")

    def sql_list_of_strings(cursor, ss):
        """Returns SQL list of strings, appropriately escaped"""
        return "(" + ", ".join(cursor.connection.escape(s) for s in ss) + ")"
    
    @retry(wait=wait_random_exponential(max=300))
    def get_results(cursor, sql):
        """Given some sql for ``title`` and ``qid``,
        execute and add to ``results``.
        """
        nonlocal logger, results
        sql = dedent(sql)
        logger.debug(sql)
        cursor.execute(f"USE {dbname}_p;")
        n_results = 0
        try:
            cursor.execute(sql)
        except:
            logger.exception(sql)
            raise

        for title, qid in cursor:
            title = title.decode()
            # I don't know why, but there's a handful of items that have a lower-case "q".  
            # Probably historical.
            # To reduce memory overhead, we convert QIDs into integers.
            qid = int(qid.decode().upper()[1:])
            if title not in titles:
                logger.error(f"Unexpected title {title} for QID Q{qid}")
            results[title] = qid
            n_results += 1
        return n_results

    def get_results_direct(cursor, titles):
        """For some set of titles, try to get results for direct sitelinks"""
        # See this link for why we have to use a namespace filter
        # https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Replica_database_schema_(tables_and_indexes)
        sql = dedent(f"""
            SELECT page_title, pp_value 
            FROM page, page_props
            WHERE page_namespace = 0
            AND page_title IN {sql_list_of_strings(cursor, titles)}
            AND page_id = pp_page
            AND pp_propname = 'wikibase_item';
        """).strip()
        return get_results(cursor, sql)

    def get_results_redirect(cursor, titles):
        """For some set of titles, try to get results as a redirect"""
        # See this link for why we have to use a namespace filter
        # https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Replica_database_schema_(tables_and_indexes)
        sql = dedent(f"""
            SELECT p1.page_title, pp_value
            FROM page AS p1, redirect, page AS p2, page_props
            WHERE p1.page_namespace = 0
            AND p1.page_title IN {sql_list_of_strings(cursor, titles)}
            AND p1.page_is_redirect
            AND p1.page_id = rd_from
            AND rd_namespace = 0
            AND rd_interwiki = ""
            AND rd_fragment = ""
            AND p2.page_namespace = 0
            AND rd_title = p2.page_title
            AND p2.page_id = pp_page
            AND pp_propname = 'wikibase_item';
        """).strip()
        return get_results(cursor, sql)

    n_direct = 0
    n_redirect = 0
    with connect_to_database(dbname) as cursor:    
        for chunk in chunks(titles, 10000):
            n_direct += get_results_direct(cursor, chunk)

        remaining = [ title for title in titles if title not in results ]

        if remaining:        
            for chunk in chunks(remaining, 10000):
                n_redirect += get_results_redirect(cursor, chunk)

    logger.info(f"convert_titles_to_qids: converted {len(titles)} titles into {len(results)} QIDs "
                f"({n_direct} direct and {n_redirect} redirect)")

    return [ results.get(title) for title in titles ]
    
class LogEntry(NamedTuple):
    """Lightweight class representing a line from the log file"""
    project: str
    title: str
    views: int
        
    def dbname(self):
        return database_from_project_name(self.project)
    

def read_log(file):
    """Read the gzipped logfile and yield log entries"""
    with io.BufferedReader(gzip.GzipFile(file, 'r')) as f:
        for line in f.readlines():
            project, title, views, _ = line.decode().split(' ')
            yield LogEntry(project, title, int(views))
            
def process_log_entries(log_entries):
    """Process log entries into (qid,views) pairs.
    Additionally yields two "logging" pairs to report unconverted entries.
    """
    unconverted_titles = 0
    unconverted_views = 0
    logger = logging.getLogger(__name__)
    for dbname, log_entries in chunk_and_partition(log_entries, key=lambda le: le.dbname(), 
                                                   max_buckets=3, chunk_size=10000):
        if dbname is None:
            views = [le.views for le in log_entries]
            unconverted_titles += len(views)
            unconverted_views += sum(views)
            continue
        titles = (le.title for le in log_entries)
        if dbname == 'wikidatawiki':
            qids = [ int(title[1:]) 
                    if title.startswith("Q") else None for title in titles ]
            logger.info(f"Wikidata special case: {len(log_entries)} "
                        "converted to {sum(qid is not None for qid in qids)}")
        else:
            qids = convert_titles_to_qids(dbname, titles)
            
        for le, qid in zip(log_entries, qids):
            if qid is not None:
                yield (qid, le.views)
            else:
                unconverted_titles += 1
                unconverted_views += le.views
    logger.warning(f"Failed to convert {unconverted_titles} titles representing {unconverted_views} views")
    yield (0, unconverted_views) # File these under a fake id so they're in our total
    
    
def file_hour(file):
    """Extracts hour from filename as MariaSQL DATETIME literal
    
    https://mariadb.com/kb/en/library/date-and-time-literals/
    """
    hour_re = re.compile(r'\b(\d\d\d\d)(\d\d)(\d\d)-(\d\d)0000\b')
    m = hour_re.search(str(file))
    assert m is not None, "Trying to process a file that doesn't contain an hour: " + file
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}:00:00"        


def write_to_database(database, qid_views, start_time, filename):
    """Write results to database

    Args:
        database: Name of database
        qid_views: Iterable of QID (e.g. Q42) and view count pairs
            Each qid should appear at most once
        hour: YYYY-MM-DDTHH:0000 formatted hour
        start_time: time.time() object from start of run
        filename: Name of log file processed
    """
    logger = logging.getLogger(__name__)
    hour = file_hour(filename)
    data = [ (qid, hour, views) for qid ,views in qid_views]
    n_qids = len(data)
    max_qid = max(x[0] for x in data)
    views = sum(x[2] for x in data)
    # https://stackoverflow.com/a/13154531
    with connect_to_database(database, cluster="tools",
                             local_infile = 1) as cursor:
        batch_insert(cursor, 'qid_hourly_views', data)
        duration = time.time() - start_time
        sql = dedent(f"""
            INSERT INTO hours
            SET file = {cursor.connection.escape(filename)},
                hour = {cursor.connection.escape(hour)}, 
                duration = {duration}, 
                views = {views}, 
                max_qid = {max_qid}, 
                n_qids = {n_qids}
        """)
        logger.info(sql)
        cursor.execute(sql)

def check_for_existing(database, filename):
    """Returns true iff there is alreadys a record for this filename."""
    with connect_to_database(database, cluster="tools",
                             local_infile = 1) as cursor:
        sql = dedent(f"""
            SELECT 1 FROM hours 
                WHERE file = {cursor.connection.escape(filename)}
	""")
        cursor.execute(sql)
        return cursor.rowcount != 0


def process_file(file, database=DEFAULT_DATABASE):
    """Do complete job of reading log file and storing in database.
    
    Args:
        file: Path to hourly log file
        database: Name of database to store results in
    Return:
        status: True if file processed
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting to process file {file}")
    if check_for_existing(database, file.name):
        logger.warning(f"Record already exists for file {file}")
        return False
    start_time = time.time()
    log_entries = read_log(file)
    qid_views = process_log_entries(log_entries)
    qid_views = sum_values(qid_views)
    write_to_database(database, qid_views.items(), start_time, file.name)     
    return True

def parse_args(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='Increases log level to INFO')
    parser.add_argument('-d', '--debug', action='store_true', help='Increases log level to DEBUG')    
    parser.add_argument('dir', type=Path, default=DEFAULT_DIR)
    parser.add_argument('--database', '--db', help='database name', default=DEFAULT_DATABASE)
    parser.add_argument("-n", "--max-files", type=int, default=10,
                        help="Maximum number of files to process")
    parser.add_argument("--maxdays", type=int, default=7, help="Maximum age of file to process in days")
    logger = logging.getLogger(__name__)
    args = parser.parse_args(argv)
    log_level = logging.DEBUG if args.debug else logging.INFO if args.verbose else logging.WARNING
    logger.setLevel(log_level)
    logger.info(argv)
    logger.info(args)    
    

def get_earliest_file(dir, max_days):
    """It's expensive to traverse the entire directory structure, 
    so we short-circuit any directories or files that cannot be recent.
    
    Args:
        dir: Base directory to traverse from.  
        max_days: Number of days to go back
        
    Returns:
        file: Path to predicted earliest file (may not exist)
    """
    now = datetime.now()
    delta = timedelta(days=max_days)
    dt = now - delta
    file = dir / dt.strftime('%Y') / dt.strftime('%Y-%m') / dt.strftime('pageviews-%Y%m%d-%H%M%S.gz')
    logging.getLogger(__name__).info(f"earliest file: {file}")
    return file
    

def get_files(dir, max_days):
    """Smart traverse of directory structure using earliest file as cutoff.
    
    Args:
        dir: Directory to traverse
        max_days: Maximum number of days to go back before now.
        
    Returns:
        files: Paths from most recent backwards
    """
    def gen(dir):
        """Helper function to recurse of directories"""
        for path in dir.iterdir():
            if path.is_dir():
                if str(path) >= str(earliest_file)[:len(tr(path))]:
                    yield from get_files(path, earliest_file)
            else:
                if str(path) >= str(earliest_file) and FILE_RE.search(path.name):
                    yield path

    earliest_file = get_earliest_file(dir, max_days)
    files = sorted(gen(args.dir), reverse=True)   
    return files

    
def main(argv=None):
    args = parse_args(argv)
    assert args.dir.is_dir()
    files = get_files(args.dir, args.max_days)
    iterate_until_n_succeed(lambda file: process_file(file, args.database), 
                            files, args.max_files)