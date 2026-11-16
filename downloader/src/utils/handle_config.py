from datetime import datetime
import os
from pathlib import Path
import logging


def get_enabled_classifications(classifications_dict: dict):
    """Get the list of classifications to process based on settings"""
    classifications = {}

    if classifications_dict["SITC1"]:
        classifications["S1"] = True

    if classifications_dict["SITC2"]:
        classifications["S2"] = True

    if classifications_dict["SITC3"]:
        classifications["S3"] = True

    if classifications_dict["HS92"]:
        classifications["H0"] = True

    if classifications_dict["HS96"]:
        classifications["H1"] = True

    if classifications_dict["HS02"]:
        classifications["H2"] = True

    if classifications_dict["HS07"]:
        classifications["H3"] = True

    if classifications_dict["HS12"]:
        classifications["H4"] = True

    if classifications_dict["HS17"]:
        classifications["H5"] = True

    if classifications_dict["HS22"]:
        classifications["H6"] = True
        
    if classifications_dict["EB10"]:
        classifications["EB10"] = True

    return classifications


def get_end_year(end_year: int):
    """Calculate end year based on user configuration."""
    if end_year is not None:
        return end_year
    return datetime.now().year - 1


def get_download_type(download_type: bool) -> str:
    """
    Determine download type based on whether weights are being run

    Two options:
     "classic" = as-reported data (original country classifications)
     "final" = standardized data (converted to specific classification)

    """
    if download_type == "as_reported":
        return "classic"
    elif download_type == "by_classification":
        return "final"
    else:
        raise ValueError("invalid download type")