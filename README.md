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
Record matching is a multistep process, using both the bibcode generated from the Crossref record (`bibcode_meta`), and the DOI of the Crossref record.  The matching process first attempts to match these to classic, by:

- Checking that the bibcode is known to the ADS as canonical, alternate, or deleted.
- Checking whether the DOI is known to the ADS, and if so what the corresponding ADS bibcode is.

If the code is able to find matches with either or both then the matching code will attempt to categorize the match:

- `matched` means `bibcode_meta` matches a canonical bibcode
- `alternate` means `bibcode_meta` matches an alternate bibcode
- `deleted` means `bibcode_meta` matches a deleted bibcode
- `partial` means a bibcode can be found that matches `bibcode_meta` with one or more of the following issues (which are output to a JSON dict in the `classic_match` field):
  - `qual`: the qualifier letter is different (e.g. "Q", "R", "L", etc.)
  - `page`: the page number is different (lexically, not necessarily numerically)
  - `vol`: the volume number is different (lexically)
  - `year`: the year is different but within +/- 1
  - `init`: the author initial is different
  - `bibstem`: the bibstem obtained from the ISSN in the Crossref record matches a related bibstem in the corresponding canonical bibcode (e.g. "JGR.." versus "JGRD.")
- `mismatch` means the doi obtained from the Crossref record matches a significantly different bibcode in classic.  These should be followed up by curators.
- `unmatched` means neither the doi nor `bibcode_meta` matches a classic record.  This may indicate either missing content or missing DOIs in classic, and should be followed up by curators.
- `failed` means the Crossref record could not be processed, typically due to an exception in the Crossref parser.  These cases should be examined, and if needed should be listed as issues in ADSIngestParser.

### III: completeness statistics

Completeness statistics are calculated on two levels:

- overall completeness fraction for a given publication/bibstem
- completeness fraction per volume (or volume+qualifier) for a given publication/bibstem

For all calculations, the total number of possible papers is obtained from the number of records in master with a given bibstem having `status` either `Matched` or `Unmatched`.  For comparison, the total number of `Matched` papers is the sum of records with `matchtype` of `canonical`, `partial`, `alternate`, or `deleted`, and the total number of unmatched papers are those with `matchtype` of `unmatched` or `mismatched`.  The `completeness_fraction` is given as a floating point number between 0.0 (all records missing) and 1.0 (all records matched), truncated to four decimal places.

For the volume-specific calculation, the selections are based on the volume and qualifier, so that (for example) papers from volume `50` will be treated separately from volume `50L`; logic is in place to ignore some qualifiers, particularly the `Q,R,S...` that are used to indicate multiple records sharing the same published page.

The user should note that the computational load for record selection in this process is borne almost entirely by the database engine rather than the pipeline itself.  When completeness statistics are run, you will see the resource utilization by the database increase significantly for a short period of time.

Finally, once the completeness of all journals with records in the `master` table have been calculated, the data will be stored in the `summary` table for review.  These data can also be exported to JSON (using the `-j` option in `run.py`) to a file having a format ready for import into ADSJournalsDB, where they can be accessed via the `/journals` endpoint of our API.

# Suggested creation and maintenance plan

There are two modes of operation for this pipeline: an interactive one designed to be used during the initial setup and processing of a new database, and an automated one for regular updates that is designed to be controlled via cron processes in the pipeline.

## Database creation

If the completeness pipeline container has been created and brought up normally for the first time, the empty database should be ready to accept data and the workers should be initialized.  You may want to consider provisioning the container's workers differently for the creation step because the number of records (over 6.5 million) processed during this step is much larger than the typical weekly number (a few hundred thousand at most).

Assuming the container is up and ready for interactive use, the following commands must be run to populate the tables for the first time:

- `nohup python3 run.py -c > nohup.1 &`, to load the classic bibcodes and bibstem-ISSN data into the database.  These records are currently read in with bulk inserts rather than in pure streaming mode, so the process may take a few hours while the indexes are generated.  Assume this step will take 2-3 hours.
- `nohup python3 run.py > nohup.2 &`, to process all records currently in the completeness reference data store.  Depending on the number of parsing workers assigned, this may take several hours.
- `nohup python3 run.py -m > nohup.3 &`, to compute the completeness statistics.  This will likely take less than an hour.


## Regular maintenance and updates

Maintenance *requires* at least two things:
- weekly updates to the data store from Crossref via OAI harvesting (discussed elsewhere), and
- weekly updates to the lists(s) of classic bibcodes.

These steps provide raw data for the matching and calculation process.  In addition weekly completeness updates should involve the following:
- weekly running of matching using the most recent harvest lists,
- weekly rematching of mismatched, unmatched, and failed records, especially if the CrossRef parser has been updated, or if DOIs have been added to existing records in classic,
- weekly recalculation of completeness, and
- weekly export of completeness data for ingest into ADSJournalsDB.

Crossref data are currently harvested early Friday mornings US Eastern time. Data can be processed once the harvesting is completed, but it's important to consider whether classic is up to date, especially if a physics update is currently
in progress (Wed-Thu, and sometimes beyond).  The matching process should be run after the classic data store has been updated to use the most recent lists (`run.py -c`).  In update mode, the matching process is triggered using the `-l` option with `run.py`, which will attempt to load only the records from the most recent harvest (as determined by the date stamp in the log filename).

After execution of `run.py -l` is complete, the system can also attempt to re-resolve problematic records from previous harvests, using `run.py -r`.  Assuming the number of records is around 100,000, this process should take about as long as running an update.  Finally, after new matches and retries have completed, you will need to rerun `run.py -m` to regenerate completeness statistics.

A conservative cron schedule to perform this weekly maintenance on Friday could be as follows:
```
00 12 * * 5 python3 run.py -c
00 16 * * 5 python3 run.py -l
00 17 * * 5 python3 run.py -r
00 18 * * 5 python3 run.py -m
00 20 * * 5 python3 run.py -j
```

An alternate possibility would triggering the calculations after the weekly astronomy update has been activated (Su-Mon), in which case, change the `5` to `1` in the crontab.
