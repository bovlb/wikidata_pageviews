"""
Helper module to convert project name as found in pageview logs into database name as required for ``connect``.

Example::
    database_from_project('en.m.d')
    # -> 'enwiki'
"""

from toolforge import _fetch_sitematrix

# These elements must be combined with the language
# e.g. en.z -> enwiki
_suffix_map = dict(
    z = 'wiki',
    d = 'wiktionary',
    b = 'wikibooks',
    n = 'wikinews',
    q = 'wikiquote',
    s = 'wikisource',
    v = 'wikiversity',
    voy = 'wikivoyage',
    m = 'wiki',
)

# These elements have no language
_complete_map = {
    's':'sourceswiki',
    'w': 'mediawikiwiki',
    'wd': 'wikidatawiki',
}

# These are associated with "m" and have no language
_wikimedia = set(['bd', 'dk', 'mx', 'nyc', 'rs', 'ua'])

# Special mappings for exceptional cases
_special_map = { 'be_tarask': 'be_x_oldwiki' }

_databases = None # Lazy

def database_from_project_name(project_name:str) -> str:
    """Find database name corresponding to project name
    
    This function takes a page views project name as described in 
    https://dumps.wikimedia.org/other/pagecounts-ez/projectcounts/readme.txt
    and returns a database name as described in 
    https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Naming_conventions 
    and https://quarry.wmflabs.org/query/4031
    suitable for use with toolforge.connect()"""
    global _databases
    if _databases is None:
        _databases = set(_sitematrix_database_names(_fetch_sitematrix()['sitematrix']))
    
    labels = project_name.split('.')
    result = None
    
    if len(labels) == 2 and labels[0] in ['www', 'm', 'zero'] and labels[1] in _complete_map:
        result = _complete_map[labels[1]]
    elif labels[-1] == 'm' and labels[0] in _wikimedia and (len(labels) == 2 or labels[1] in ['m', 'zero']):
        result = labels[0] + 'wikimedia'
    else:
        prefix = labels[0].replace('-', '_')

        if len(labels) > 1 and labels[1] in ['m', 'zero']:
            del labels[1]
            
        if prefix in _special_map:
            result = _special_map[prefix]
        else:
            site = labels[1] if len(labels) > 1 else 'z'
            if site in _suffix_map:
                result = prefix + _suffix_map[site]
                # Could return prefix as language

    if result is None or result not in _databases:
        return None

    return result
    
def _sitematrix_database_names(data):
    for k,v in data.items():
        if k.isdigit():
            for site in v['site']:
                if 'private' not in site:
                    yield site['dbname']
        elif k == 'specials':
            for site in v:
                if 'private' not in site:
                    yield site['dbname']

def dbinfo(data):
    results = dict()
    for k,v in data.items():
        if k.isdigit():
            for site in v['site']:
                results[site['dbname']] = site
        elif k == 'specials':
            for site in v:
                results[site['dbname']] = site
    return results