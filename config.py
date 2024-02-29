HARVEST_BASE_DIR = "/app/data/Crossref/"
HARVEST_LOG_DIR = HARVEST_BASE_DIR + "/UpdateAgent/"

CLASSIC_DOI_FILE = "/app/data/all.links"
CLASSIC_ALTBIBS = "/app/data/bibcodes.list.alt"
CLASSIC_DELBIBS = "/app/data/bibcodes.list.del"
CLASSIC_ALLBIBS = "/app/data/bibcodes.list.all"
CLASSIC_CANONICAL = "/app/data/bibcodes.list.can"
JOURNALSDB_ISSN_BIBSTEM = "/app/data/issn_identifiers"
COMPLETENESS_EXPORT_FILE = "/app/data/completeness_export.json"
JOURNALSDB_RELATED_BIBSTEMS = "/app/data/related_bibstems.json"

CLASSIC_DATA_BLOCKSIZE = 10000
RECORDS_PER_BATCH = 250


CELERY_INCLUDE = ['adscompstat.tasks']
CELERY_BROKER = 'pyamqp://user:password@localhost:6672/compstat'
