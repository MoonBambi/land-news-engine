import pandas as pd
import os

class ExcelClient:
    def __init__(self, output_dir='.'):
        self.output_dir = output_dir

    def save(self, data, filename):
        """
        Save list of dictionaries to Excel
        :param data: list of dicts
        :param filename: output filename (e.g. 'data.xlsx')
        """
        if not data:
            print("No data to save.")
            return

        df = pd.DataFrame(data)
        file_path = os.path.join(self.output_dir, filename)
        
        try:
            df.to_excel(file_path, index=False, engine='openpyxl')
            print(f"已保存到 {file_path}")
        except Exception as e:
            print(f"保存 Excel 失败: {e}")
