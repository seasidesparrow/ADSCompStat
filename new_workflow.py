import json
from adscompleteness import utils

basedir = '/proj/ads/abstracts/sources/Crossref2/'
infiles = ['doi/10.1016/./j,/ic/ar/us/,2/02/2,/11/51/88//metadata.xml']
# infiles = ['doi/10.1002/./as/na/,1/92/52/25/06/02//metadata.xml']
# infiles = ['doi/10.1029/20/02/RG/00/01/21/metadata.xml']


for f in infiles:
    f = basedir + f
    record = utils.parse_one_meta_xml(f)
    try:
        publication = record.get('publication', None)
        pagination = record.get('pagination', None)
        pids = record.get('persistentIDs', None)
        first_author = record.get('authors', None)
        title = record.get('title', None)
        if publication:
            pub_year = publication.get('pubYear', None)
            issns = publication.get('ISSN', None)
        if pids:
            doi = pids.get('DOI', None)
        if first_author:
            first_author = first_author[0]
    except Exception as err:
        print('oh noes! %s' % err)
    else:
        bib_data = {'publication': publication,
                   'pagination': pagination,
                   'persistentIDs': pids,
                   'first_author': first_author,
                   'title': title}
        source_file = f
        output_rec = (doi, json.dumps(issns), json.dumps(bib_data), source_file)
    
