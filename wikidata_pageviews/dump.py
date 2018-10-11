"""Functionality to dump bulk information about all QIDs.
"""

from textwrap import dedent
import re
import datetime
import logging
import math
import argparse
import sys
import json

import toolforge

DEFAULT_DATABASE = 's53865__wdpv_p'

def latest_available_hour(cursor):
    """Returns the last available hour in the database"""
    sql = dedent(f"""
        SELECT MAX(hour) FROM hours;
    """)
    cursor.execute(sql)
    (hour,) = cursor.fetchone()
    return hour
    
HOUR_RE = re.compile(r'^(\d\d\d\d-\d\d-\d\d)T(\d\d)$')

def parse_hour(hour):
    """Take hour in API/CLI form and convert to database form
    
    Args:
        hour: Hour in form "2018-10-10T01"
        
    Returns:
        hour_converted: Hour in form "2018-10-10 01:00:00" or None
    """
    m = HOUR_RE.search(hour)
    if m:
        return f"$1 $2:00:00"
    logging.getLogger(__name__).warning(f"parse_hour: Could not parse: {hour}")
    return None

DURATION_UNITS = {
    '': 1,
    'h': 1,
    'd': 24,
    'w': 24*7,
    'm': 24*30,
}

DURATION_RE = re.compile(r'^(\d+)([hdwm]?)$', re.IGNORECASE)

def parse_duration(duration, hour):
    """Parses a duration and returns a new hour
    
    Args: 
        duration: string of the form "<number><unit>", e.g. "1d" for one day.
        hour: Base for hour calculation, string of form "YYYY-MM-DD HH:00:00"
    Returns
        new_hour: A new time relative to the existing hour.
            Note that duration is the length of the closed interval
            [new_hour,hour] and not the difference between the new and old hours.
            This means that the calculated value may be one hour later than you expect.
    """
    m = DURATION_RE.search(duration)
    if m:
        value = m.groups(1)
        unit = m.groups(2)
        multiplier = DURATION_UNITS[unit]
        adjusted_value = value * multiplier - 1
        dt_to = datetime.strptime(hiyr, "%Y-%m-%d %H:%M:%S")
        assert dt_to is not None
        delta = datetime.timedelta(hours = -adjusted_value)
        dt_from = dt_to - delta
        return dt_from.strftime("%Y-%m-%d %H:%M:%S")
    logging.getLogger(__name__).warning(f"parse_duration: Could not parse: {duration}")
    return None


def convert_start_and_end(cursor, start, end):
    """Convert API/CLI form of start and end to database form
    
    Args:
        cursor: Database cursor
        start: Should either be an hour like "2018-10-10T01" or a duration like "1d"
        end: Shold either be an hour like "2018-10-10T01" or None
            which indicates the latest available hour.
    Returns:
        start: Hour like "2018-10-10 01:00:00"
        end: Hour like "2018-10-10 01:00:00"
    """
    if end is None:
        end_converted = latest_available_hour(cursor)
        assert end_converted is not None, "No hours available in database"
    else:
        end_converted = parse_hour(end)
        assert end_converted is not None, f"Unable to parse to hour {end}"

    if start is None:
        start = '1d'
    start_converted = parse_hour(start)
    if start_converted is None:
        start_converted = parse_duration(start, end_converted)
    assert start_converted is not None, f"Unable to parse {start} as either hour or duration"
    return (start_converted, end_converted)


def get_hours(cursor, start, end):
    """Report set of hours that fall in range.
    
    Args:
        cursor: Database cursor
        start: Hour like "2018-10-10 01:00:00"
        end: Hour like "2018-10-10 01:00:00"
        
    Returns:
        hours: List of hours like "2018-10-10 01:00:00"
    """
    sql = dedent(f"""
        SELECT hour FROM hours
        WHILE hour >= "{start}" AND hour <= "{end}";
    """)
    cursor.execute(sql)
    return [ hour for (hour,) in cursor.fetchall() ]


def aggregate_by_qid(cursor, start, end):
    """The main work of getting the qid/views pairs
    
    Args:
        cursor: Database cursor
        start: Hour like "2018-10-10 01:00:00"
        end: Hour like "2018-10-10 01:00:00"
    Yields:
        qid: String like "Q42"
        views: Number of page views
    """
    sql = dedent(f"""
        SELECT qid, SUM(views) AS views 
        FROM qid_hourly_views 
        WHERE hour >= "{start}"
        AND hour <= "{end}"
        GROUP BY qid;
    """)
    cursor.execute(sql)
    for qid, views in cursor:
        # We store unaligned views against the magic value 0
        if qid != 0:
            yield ("Q" + str(qid), views)
        

def get_summary(cursor, start, end):
    """Returns useful summary information about the period
    
    Args:
        cursor: Database cursor
        start: Hour like "2018-10-10 01:00:00"
        end: Hour like "2018-10-10 01:00:00"

    Returns:
        max_qid: Maximum QID seen in any hour
        views: Total views across all hours
    """
    sql = dedent(f"""
        SELECT MAX(max_qid) AS max_qid,
            SUM(views) AS views
        FROM hours
        WHERE hour >= "{start}"
        AND hour <= "{end}";
    """)
    cursor.execute(sql)
    (max_qid, views) = cursor.fetchone()
    return (max_qid, views)

        
def get_dump(database=DEFAULT_DATABASE, start=None, end=None, mode=None):
    """Returns result object for bulk aggregation
    
    Args:
        database: name of database to use
        start: Start hour as "2018-10-10T01" or a duration like "1d"
        end: End hour as "2018-10-10T01" or None
        mode: How to manipulate results
            views: (default) Report raw views in ``views`` field
            logprobs: Estimate log probabilities in ``logprobs`` and ``default_logprob`` field
    """
    logger = logging.getLogger(__name__)
    with toolforge.connect(database, cluster="tools") as cursor:
        (start, end) = convert_start_and_end(cursor, start_and_end)    
        logger.info(f"start={start}, end={end}")
        hours = get_hours(cursor, start, end)
        logger.info(f"{len(hours)} hours")
        (max_qid, total_views) = get_summary(cursor, start, end)
        result = dict(
            start=start,
            end=end,
            hours=hours,
            max_qid=max_qid,
            total_views=total_views,
        )
        data = aggregate_by_qid(cursor, start, end)
        if mode is None or mode == 'views':
            result['views'] = dict(data)
        elif mode == 'logprobs':
            # We're applying Laplace smoothing here, using the maximum QID number 
            # as an estimate of the total number of QIDs.
            log_denominator = math.log(total_views + max_qid)
            result['logprobs'] = {
                qid: math.log(views+1) - log_denominator
                for qid, views in data
            }
            result['default_logprob'] = math.log(1) - log_denominator
        else:
            assert False, f"Unknown mode {mode}"
            
        return result
    
def parse_args(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', help='Increases log level to INFO')
    parser.add_argument('-d', '--debug', help='Increases log level to DEBUG')    
    parser.add_argument('--database', '--db', help='database name', default=DEFAULT_DATABASE)
    parser.add_argument('--start', help='Start time, e.g. 2018-10-10T17 or 1d')
    parser.add_argument('--end', help="End time, e.g. 2018-10-10T17")
    parser.add_argument('--mode', help="Mode, e.g. views, logprobs")
    logger = logging.getLogger(__name__)
    args = parser.parse_args(argv)
    log_level = logging.DEBUG if args.debug else logging.INFO if args.verbose else logging.WARNING
    logger.setLevel(log_level)
    logger.info(args)

def main():
    args = parse_args()
    result = get_dump(database=args.database, 
                      start=args.start,
                      end=args.end,
                      mode=args.mode,
                     )
    json.dump(sys.stdout, result)