"""Main functionality to process hourly logfile

Example::
    qid_views = process_log_entries(read_log(my_filename))
"""

import gzip
#from collections import defaultdict, Counter
from typing import NamedTuple
import os
import re
import sys
from textwrap import dedent
import logging
import itertools
import io
import time
import logging
import argparse
from pathlib import Path
from .util import *
from .project import database_from_project_name
import pickle

import toolforge

DEFAULT_DATABASE = 's53865__wdpv_p'
PICKLE_FILE = '/tmp/wdpv.pickle'

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
            # I don't know why, but there's a handful of items that have a lower-case "q".  Probably historical.
            qid = qid.decode().upper()
            if title not in titles:
                logger.error(f"Unexpected title {title} for QID {qid}")
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
            qids = [ title if title.startswith("Q") else None for title in titles ]
            logger.info(f"Wikidata special case: {len(log_entries)} converted to {sum(qid is not None for qid in qids)}")
        else:
            qids = convert_titles_to_qids(dbname, titles)
            
        for le, qid in zip(log_entries, qids):
            if qid is not None:
                yield (qid, le.views)
            else:
                unconverted_titles += 1
                unconverted_views += le.views
    logger.warning(f"Failed to convert {unconverted_titles} titles representing {unconverted_views} views")
    yield ("Q0", unconverted_views) # File these under a fake id so they're in our total
    
    
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
    data = [ (int(qid[1:]), hour, views) 
        for qid ,views in qid_views]
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
    with connect_to_database(database, cluster="tools",
                             local_infile = 1) as cursor:
        sql = dedent(f"""
            SELECT 1 FROM hours 
                WHERE file = {cursor.connection.escape(filename)}
	""")
        cursor.execute(sql)
        assert cursor.rowcount == 0, f"File {filename} already has a record"


def process_file(file, database=DEFAULT_DATABASE):
    """Do complete job of reading log file and storing in database.
    
    Args:
        file: Path to hourly log file
        database: Name of database to store results in
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting to process file {file}")
    check_for_existing(database, file.name)
    start_time = time.time()
    log_entries = read_log(file)
    if True:
        qid_views = process_log_entries(log_entries)
        qid_views = sum_values(qid_views)
        with open(PICKLE_FILE, 'wb') as f:
            pickle.dump(qid_views, f)
    else:
        with open(PICKLE_FILE, 'rb') as f:
            qid_views = pickle.load(f)
    write_to_database(database, qid_views.items(), start_time, file.name)     

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', help='Increases log level to INFO')
    parser.add_argument('-d', '--debug', help='Increases log level to DEBUG')    
    parser.add_argument('files', type=Path, nargs='+', metavar='file')
    parser.add_argument('--database', '--db', help='database name', default=DEFAULT_DATABASE)
    logger = logging.getLogger(__name__)
    args = parser.parse_args(argv)
    log_level = logging.DEBUG if args.debug else logging.INFO if args.verbose else logging.WARNING
    logger.setLevel(log_level)
    logger.info(args)
    for file in args.files:
        process_file(file, args.database)
