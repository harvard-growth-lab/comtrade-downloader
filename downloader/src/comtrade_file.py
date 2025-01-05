from pathlib import Path
import regex as re
from datetime import datetime


class ComtradeFile:
    """Parses and stores Comtrade file metadata."""

    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.name = self.file_path.name
        self._parse_filename()

    def _parse_filename(self) -> None:
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
        raise ValueError(f"File format has not been handled: {self.name}")
        
        
class ComtradeFiles:
    """Extract file(s) paths based on provided metadata"""

    def __init__(self, files):
        self.files = files

    def get_file_names(self, reporter_code, dates) -> list:
        files = set()
        for f in self.files:
            for date in dates:
                date_str = date.strftime("%Y-%m-%d")
                file = re.search(f".*COMTRADE-FINAL-CA{reporter_code}\\d{{4}}\\w+\\[{date_str}]", f)

                # file = re.search(f".*COMTRADE-FINAL-CA{reporter_code}\\d{{4}}\\w+\\[\\d{{4}}-\\d{{2}}-\\d{{2}}\\]", f)
                try:
                    files.add(file.string)
                except AttributeError as e:
                    pass
        return files
                
            
