from pathlib import Path
import re
from datetime import datetime


class ComtradeFile:
    """File object for data files downloaded from Comtrade to extract metadata
    from filename as given by Comtrade API"""

    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.name = self.file_path.name
        self._parse_filename()

    def _parse_filename(self) -> None:
        """
        Parses the filename with two patterns based on download type and
        extracts the metadata from the filename

        Metadata:
        - reporter_code: as given by Comtrade; https://comtradeplus.un.org/ListOfReferences
        - year: relevant year of data
        - classification: 2 digit classification code
        - published_date: date the data was published
        """

        patterns = [
            r"COMTRADE-FINALCLASSIC-CA(?P<reporter>\d{3})(?P<year>\d{4})(?P<classification>\w+)\[(?P<date>[\d-]+)\]",
            r"COMTRADE-FINAL-CA(?P<reporter>\d{3})(?P<year>\d{4})(?P<classification>\w+)\[(?P<date>[\d-]+)\]",
        ]

        for pattern in patterns:
            match = re.match(pattern, self.name)
            if match:
                self.match = match
                self.reporter_code = match.group("reporter")
                self.year = int(match.group("year"))
                self.classification = match.group("classification")
                self.published_date = datetime.strptime(match.group("date"), "%Y-%m-%d")
                return
        raise ValueError(f"File format not recognized: {self.name}")

    def swap_classification(self, new_classification: str) -> None:
        """
        Swap the classification in the filename and update the object's classification
        name and file_path

        new_classification : str
            The new classification code to use (e.g., 'H0', 'H1', 'S3', etc.)
        """
        old_filename = self.name

        self.classification = new_classification

        if "FINALCLASSIC" in old_filename:
            pattern = r"(COMTRADE-FINALCLASSIC-CA\d{3}\d{4})(\w+)(\[[\d-]+\]\.parquet)"
        else:
            pattern = r"(COMTRADE-FINAL-CA\d{3}\d{4})(\w+)(\[[\d-]+\]\.parquet)"

        # Replace the classification part with the new classification
        self.name = re.sub(pattern, rf"\1{new_classification}\3", old_filename)

        # Update the file_path to match the new name
        self.file_path = self.file_path.parent / self.name


class ComtradeFiles:
    """Extract file(s) paths based on provided metadata"""

    def __init__(self, files):
        self.files = files

    def get_file_names(self, reporter_code: str, dates: list[datetime]) -> list:
        """
        Get the file names for a given reporter code and dates

        Returns:
            list: List of file names
        """
        files = set()
        for f in self.files:
            for date in dates:
                date_str = date.strftime("%Y-%m-%d")
                file = re.search(
                    f".*COMTRADE-FINAL-CA{reporter_code}\\d{{4}}\\w+\\[{date_str}]", f
                )
                try:
                    files.add(file.string)
                except AttributeError as e:
                    pass
        return files
