from pathlib import Path

SERVICES = ["EB10"]

CONVERSION_LINKS = [
    "S1",
    "S2",
    "S3",
    "H0",
    "H1",
    "H2",
    "H3",
    "H4",
    "H5",
    "H6",
]

CLASSIFICATION_RELEASE_YEARS = {
    "S1": 1962,
    "S2": 1976,
    "S3": 1988,
    "H0": 1988,
    "H1": 1996,
    "H2": 2002,
    "H3": 2007,
    "H4": 2012,
    "H5": 2017,
    "H6": 2022,
    "EB10": 2010
}

FILTER_CONDITIONS = {
    "customsCode": "C00",
    "motCode": "0",
    "mosCode": "0",
    "partner2Code": 0,
    # 1, 2 are legacy numeric codes used by Comtrade pre 2000s
    "flowCode": ["M", "X", "RM", "RX", "1", "2"],
}
