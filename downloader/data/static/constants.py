from pathlib import Path

CLASSIFICATION_RELEASE_YEARS = {
    # Standard International Trade Classification (SITC)
    "S1": 1962,  # SITC Revision 1 (1962-present)
    "S2": 1976,  # SITC Revision 2 (1976-present)
    "S3": 1988,  # SITC Revision 3 (1988-present)
    # Harmonized System (HS) Classifications
    "H0": 1992,  # HS Combined (1992-present)
    "H1": 1996,  # HS 1992 vintage (1996-present)
    "H2": 2002,  # HS 2002 vintage (2002-present)
    "H3": 2007,  # HS 2007 vintage (2007-present)
    "H4": 2012,  # HS 2012 vintage (2012-present)
    "H5": 2017,  # HS 2017 vintage (2017-present)
    "H6": 2022,  # HS 2022 vintage (2022-present)
}

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

FILTER_CONDITIONS = {
    "customsCode": "C00",
    "motCode": "0",
    "mosCode": "0",
    "partner2Code": 0,
    "flowCode": ["M", "X", "RM", "RX"],
}
