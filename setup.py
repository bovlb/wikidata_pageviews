from setuptools import setup

setup(name='wikidata_pageviews',
      version='0.1',
      description='Manages projection of WMF pageview data onto Wikidata ids',
      url='https://github.com/bovlb/wikidata_pageviews',
      author='Bovlb',
      author_email='31326650+bovlb@users.noreply.github.com',
      license='',
      packages=['wikidata_pageviews'],
      zip_safe=False,
      entry_points = {
          'console_scripts': [
              'wdpv-process-file=wikidata_pageviews.process_log:main',
              'wdpv-dump=wikidata_pageviews.dump:main',
              'wdpv-process-and-dump=wikidata_pageviews:main'
          ],
      }
)
