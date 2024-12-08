from dataclasses import dataclass
from pathlib import Path
import regex as re
from datetime import datetime


@dataclass
class ComtradeFile:
    """Parses and stores Comtrade file metadata."""
    file_path: str
    
    def __post_init__(self):
        self.file_path = Path(self.file_path)
        self.name = self.file_path.name
        self._parse_filename()

    def _parse_filename(self) -> None:
        patterns = [r"COMTRADE-FINALCLASSIC-CA(?P<reporter>\d{3})(?P<year>\d{4})(?P<classification>\w+)\[(?P<date>[\d-]+)\]", r"COMTRADE-FINAL-CA(?P<reporter>\d{3})(?P<year>\d{4})(?P<classification>\w+)\[(?P<date>[\d-]+)\]"]
                    
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



