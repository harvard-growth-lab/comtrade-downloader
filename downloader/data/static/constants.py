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

FILTER_CONDITIONS = {
    "customsCode": "C00",
    "motCode": "0",
    "mosCode": "0",
    "partner2Code": 0,
    "flowCode": ["M", "X", "RM", "RX"],
}
