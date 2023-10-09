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

## Preliminaries -- required data stores

The process of determining the completeness of the ADS' holdings for a given
journal has multiple steps.  The creation of an OAIPMH archive of Crossref XML
records is assumed as already available, and will not be discussed here.
The harvest archive must have:
- a set of harvest logs having a specific format, available in `[PATH]/UpdateAgent/`.  The files have two, tab-separated columns listing the path to harvested metadata files, and the time at which they were harvested.
- a `[PATH]/doi/` directory with a nested directory structure having the CrossRef collection IDs as the top-level subdirectories.

The classic data will consist of postgresql tables created from:
- A mapping of ADS Bibliographic Code *bibstems* to their corresponding ISSN
- ADS Canonical bibcodes mapped to DOIs
- The full list of ADS bibcodes (bibcodes.list.all)
- The full list of known DOIs and their mapping to ADS bibcodes
- The list of ADS alternate bibcodes and their mapping to canonical bibcodes
- The list of ADS deleted bibcodes and their mapping to canonical bibcodes

## The matching process

### I: record parsing
The code uses the harvest logs described above to generate a list of \*.xml files (assumed to be in CrossRef XML format).  These lists of files are batched into groups, and sent to a task that will parse the record into a JSON object having a format defined in the Ingest Data Model.  This process makes use of ADSIngestParser's `adsingestp.parsers.crossref`.  Once the record's bibliographic metadata are available, the code then attempts to generate an ADS Bibliographic Code, using ADSIngestEnrichment's `adsenrich.bibcodes`

### II: record matching
Record matching is a multistep process, using both the bibcode generated from the Crossref record, and the DOI of the Crossref record.  The matching process first attempts to match these to classic, by:
- Checking that the bibcode is known to the ADS as canonical, alternate, or deleted.
- Checking whether the DOI is known to the ADS, and if so what the corresponding ADS bibcode is.

If the code is able to find matches with either or both then the matching code
will attempt to categorize the match.
