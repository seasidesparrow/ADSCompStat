# ADSCompStat
ADS Completeness Statistics Database and Pipeline

## Summary

This repository includes code to provision, generate, and manage completeness
data obtained from a reference dataset obtained from Crossref for several
hundred core journals in the ADS Collection.  The code generates completeness
statistics assuming that we have harvested the entire publication history
from Crossref for each of these journals, and can either locate the ADS'
record for each paper, or can provide information about why the record
isn't present.

The package is built assuming it will be integrated into the ADS' pipeline
system(s) and has access to all backoffice data available to our curators,
most importantly our lists of canonical, alternate, and deleted bibcodes,
along with our mapping of bibcodes to DOIs.

## Runtime Options

```
usage: run.py [-h] [-p DO_PUB] [-l] [-c] [-m] [-j] [-r]

Command line options.

optional arguments:
  -h, --help            show this help message and exit
  -p DO_PUB, --publisher-prefix DO_PUB
                        Parse only logs for one publisher DOI prefix
  -l, --latest          Do only records from the most recent harvest
  -c, --classic         Load bibstem/bibcode/doi/issn data from classic flat
                        files
  -m, --completeness    Calculate completeness summary for all harvested
                        bibstems
  -j, --json            Export completeness summary to JSON file
  -r, --retry           Retry all mismatched and unmatched records
```

- `-c`, `--classic`: Provisions the database with the necessary classical record data -- bibcodes, and their mapping to DOIs when available. *Note: this must be run before any other run.py options, and must be rerun weekly when new records are added.*

- `-p` DO_PUB, `--publisher-prefix` DO_PUB: Use this option to parse records from only one crossref collection id (DO_PUB).  For example, `-p 10.3847` will process only AAS Journals (which has the CrossRef collection ID 10.3847). *Note: this option can be used with `-l`.*

- `-l`, `--latest`: Use this option to parse records from the most recent Crossref harvest date, used for doing incremental completeness updates after any harvest. *Note: this option can be used with `-p`.*

- `-r`, `--retry`: Use this option to reparse records in the master table having `master.matchtype` of "unmatched" or "mismatch". *Note: this should be run after reloading classic data (`-c`).*

- `-m`, `--completeness`: Computes the completeness summary for all parsed records currently in the database.

- `-j`, `--json`: Use this to export a summary of completeness data.  The JSON is formatted to be loadable by ADSJournalsDB for its public API.
