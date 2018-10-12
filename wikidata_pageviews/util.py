"""
Various generic functions that we require.
"""

from typing import Iterator, Generator, Tuple
import itertools
import tempfile
from textwrap import dedent

def chunk_and_partition(items, key, chunk_size=None, max_unprocessed=None, max_buckets=None):
    """Partitions items and processes in chunks
    
    Suppose you have an iterable of items that you want to process in chunks, 
    but in each chunk processed the items must all be "the same kind".  
    This function splits items from the input iterator into buckets according to the
    partitioning key, and yields (key, items) pairs.
    
    It will yield the largest bucket whenever any of the following hold:
        * A bucket (necessarily the largest) reaches ``chunk_size``
        * The total number of unprocessed items reaches ``max_unprocessed``
        * The total number of buckets would exceed ``max_buckets``
    After the input has been exhausted, remaining items are yielded in arbitrary order.

    Chunking will work well if the input has large run-lengths by partition key
    (e.g. if it is already sorted, or mostly sorted),
    or if the ratio between ``max_unprocessed`` and ``chunk_size`` 
    (or the maximum number of buckets) is comparable 
    to the number of distinct partition keys;
    otherwise the actual chunk size will tend to be smaller than ``chunk_size``.
    
    If none of ``chunk_size``, ``max_unprocessed`, and ``max_buckets`` is specified,
    then the entire input list will be partitioned before processing (cf ``itertools.groupby``)
    
    Args:
        items: Iterable of items to process
        key: Function that takes an item and returns a partitioning key (e.g. a string)
        chunk_size: Maximum number of items per chunk 
        max_unprocessed: Maximum number of items to hold unprocessed
        max_buckets: Maximum number of buckets
        
    Yields:
        partition: Result of ``key``
        items: List of items from the input
        
    Example::
        list(chunk_and_partition(range(1,25), lambda i: i%(int((i+3)/3)), chunk_size=3, max_buckets=4, max_unprocessed=5))
    """        
    
    def check_optional_positive_integer(x, name):
        assert x is None or (x > 0 and isinstance(x, int)), f"{name} must be either None or a positive integer"
    
    check_optional_positive_integer(chunk_size, "chunk_size")
    check_optional_positive_integer(max_unprocessed, "max_unprocessed")
    check_optional_positive_integer(max_buckets, "max_buckets")
    
    cache = defaultdict(list)
    n_unprocessed = 0 # Total length of lists in cache
    
    def pop_largest():
        nonlocal cache, n_unprocessed
        p, ii = max(cache.items(), key=lambda p_ii: len(p_ii[1]))
        del cache[p]
        n_unprocessed -= len(ii)
        return (p, ii)
    
    for item in items:
        partition = key(item)
        if max_buckets is not None and len(cache) == max_buckets and partition not in cache:
            yield pop_largest()
        cache[partition].append(item)
        n_unprocessed += 1
        if chunk_size is not None and len(cache[partition]) == chunk_size:
            # we already know which one is largest
            yield (partition, cache[partition])
            del cache[partition]
            n_unprocessed -= chunk_size
        elif max_unprocessed is not None and n_unprocessed == max_unprocessed:
            yield pop_largest()
        # At this point:
        # * The maximum length is less than chunk_size
        # * The total length is less than max_unprocessed

    # Now process the remaining items in arbitrary order
    for p, ii in cache.items():
        yield (p, ii)
        
        
# https://stackoverflow.com/a/24527424/9073611
def chunks(iterable:Iterable, size:int=10) -> Generator[Generator, None, None]:
    """ Breaks down an iterable into chunks. 
    
    Args:
        iterable: Input items
        size: Size of chunk
        
    Yields:
        chunk: generator for no more than ``size`` elements from ``iterable``
    """
    iterator = iter(iterable)
    for first in iterator:
        # Need to realize list if results are passed to executor.map()
        yield itertools.chain([first], itertools.islice(iterator, size - 1))

        
def sum_values(kvs:Iterable[Tuple[str, int]]):
	"""Takes pairs of key and number and returns the the sum by key.
    
    Args:
        kvs: An iterable of key/number pairs
        
    Returns:
        Dictionary from keys to totals
    """
	results = dict()
	for k, v in kvs:
        if k in results:
            results[k] += v
        else:
            results[k] = k
    return results

def batch_insert(cursor, table:str, data: Iterable[Iterable], columns: Optional[Iterable[str]]=None,
                replace=False, ignore=False, low_priority=False, concurrent=False,
                low_priority=False, concurrent=False):
    """Performs efficient insertion into a toolforge (MariaDB) database.
    
    Current implementation uses ``LOAD DATA INFILE`` following the advice of
    https://mariadb.com/kb/en/library/how-to-quickly-insert-data-into-mariadb/#loading-text-files
    
    See https://mariadb.com/kb/en/library/load-data-infile/ for more information.

    ``replace`` and ``ignore`` cannot be used together.
    ``low_priority`` and ``concurrent`` cannot be used together.

    Args:
        cursor: open cursor on database
        table: name of table to insert into
        data: iterable of row tuples
        columns: list of column names
        replace: if an inserted row has the same value as existing rows for a primary key or unique index, all such existing rows are deleted
        ignore: illegal rows (for example those with the same value as existing rows for a primary key or unique index) are ignored
        low_priority: insertions are delayed until no other clients are reading from the table
        concurrent: allows the use of concurrent inserts
        
    Returns:
        result: Result of cursor execution, hopefully a number of rows
    """
    assert not replace or not ignore, "Cannot specify both replace and ignore"
    assert not low_priory or not concurrent, "Cannot specify both low priority and concurrent"
    # We're using the default TSV layout using tabs and newlines, but with a backslash escape character.
    escape_table = str.maketrans({"\\": "\\\\", "\t": "\\\t", "\n": "\\\n"})
    file = tempfile.NamedTemporaryFile()
    with open(file, 'w') as f:
        for row in data:
            escaped_row = (x.translate(escape_table) for x in row)
            print("\t".join(escaped_row), file=f)
    
    replace_or_ignore = "REPLACE" if replace else "IGNORE" if ignore else ""
    priority = "LOW PRIORITY" if low_priority else "CONCURRENT" if concurrent else ""
    character_set 
    columns = "(" + ", ".join(columns) + ")" if columns is not None else ""
    sql = dedent(f"""
        LOAD DATA {priority} LOCAL INFILE '{file}'
            {replace_or_ignore}
            INTO TABLE {table}
            FIELDS ESCAPED BY '\\\\'
            {columns}
    """).strip()
    result = cursor.execute(sql)