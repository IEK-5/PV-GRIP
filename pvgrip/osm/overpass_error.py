class OverpassAPIError(Exception):
    """
    Raised when requestin OSM-data from OverpassAPI
    results in a response that's not an OSM file
    """
