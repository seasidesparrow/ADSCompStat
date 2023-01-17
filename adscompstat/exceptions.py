class BadAllLinksFileException(Exception):
    pass


class NoHarvestLogsException(Exception):
    pass


class ParseLogsException(Exception):
    pass


class ReadLogException(Exception):
    pass


class ParseMetaXMLException(Exception):
    pass


class CrossRefParseException(Exception):
    pass


class BaseParseException(Exception):
    pass


class EmptyRecordException(Exception):
    pass


class LoadClassicDataException(Exception):
    pass


class NoDataHandlerException(Exception):
    pass


class GetMetaException(Exception):
    pass


class RecordException(Exception):
    pass


class GetLogException(Exception):
    pass


class DBClearException(Exception):
    pass


class CompletenessFractionException(Exception):
    pass


class JsonExportException(Exception):
    pass


class MissingFilenameException(Exception):
    pass


class LoadIssnDataException(Exception):
    pass
