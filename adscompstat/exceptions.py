# run.py
class GetLogException(Exception):
    pass


class DBWriteException(Exception):
    pass


class DBClearException(Exception):
    pass


class LoadClassicDataException(Exception):
    pass


# adscompstat/tasks.py
class BibstemLookupException(Exception):
    pass


class FetchClassicBibException(Exception):
    pass


# adscompstat/utils.py
class NoHarvestLogsException(Exception):
    pass


class ParseLogsException(Exception):
    pass


class ReadLogException(Exception):
    pass


class CrossRefParseException(Exception):
    pass


class BaseParseException(Exception):
    pass


class ParseMetaXMLException(Exception):
    pass


class LoadIssnDataException(Exception):
    pass


class MergeClassicDataException(Exception):
    pass


class CompletenessFractionException(Exception):
    pass


class MissingFilenameException(Exception):
    pass


class JsonExportException(Exception):
    pass
