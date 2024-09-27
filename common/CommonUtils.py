from io import BytesIO

import pyarrow.parquet as pq


class CommonUtils:
    @staticmethod
    def is_parquet_file(file_data: BytesIO):
        try:
            # Kiểm tra nếu tệp có định dạng Parquet
            pq.ParquetFile(file_data)
            return True
        except Exception as e:
            return False
