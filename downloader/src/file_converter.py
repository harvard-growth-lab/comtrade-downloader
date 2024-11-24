from downloader.src.api_downloader import ComtradeDownloader


class DataFrameConverter(ComtradeDownloader):
    """Base class for conversions involving pandas DataFrames"""
    
    def __init__(self, 
                 input_path: Union[str, Path], 
                 output_path: Union[str, Path], 
                 dtype_map: Optional[Dict[str, Any]] = None):
        super().__init__(input_path, output_path)
        self.dtype_map = dtype_map or {}
    
    def _read_dataframe(self) -> pd.DataFrame:
        raise NotImplementedError
        
    def _write_dataframe(self, df: pd.DataFrame) -> None:
        raise NotImplementedError
        
    def convert(self) -> None:
        df = self._read_dataframe()
        self._write_dataframe(df)

class GZToParquet(DataFrameConverter):
    """Convert gunzip compressed files to Parquet format"""
    
    def _read_dataframe(self) -> pd.DataFrame:
        return pd.read_csv(self.input_path, compression='gzip', sep='\t')
    
    def _write_dataframe(self, df: pd.DataFrame) -> None:
        df.to_parquet(self.output_path, compression='snappy')

class GZToStata(DataFrameConverter):
    """Convert Excel files to CSV format"""
    
    def __init__(self, input_path: Union[str, Path], output_path: Union[str, Path],
                 sheet_name: Union[str, int] = 0):
        super().__init__(input_path, output_path)
        self.sheet_name = sheet_name
    
    def _read_dataframe(self) -> pd.DataFrame:
        return pd.read_csv(self.input_path, compression='gzip', sep='\t')
    
    def _write_dataframe(self, df: pd.DataFrame) -> None:
        df.to_stata(self.output_path, write_index=False, compression='infer')

