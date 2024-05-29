import os
from dataclasses import dataclass, asdict, field
#from scraper_settings import OUTPUT_FILE_DIRECTORY
import pandas as pd

@dataclass
class Content:
    category: str
    title: str
    rating: str
    price: str
    availability: str
    link: str

@dataclass
class Dataset:
    endpoint: str
    records: list[Content] = field(default_factory=list)
    
    def dataframe(self):
        return pd.json_normalize(
            [asdict(record) for record in self.records])
    
    # def save_to_file(self, filename: str, type: str='csv'):
    #     if (type == 'csv'):
    #         filename = OUTPUT_FILE_DIRECTORY + filename + '.csv'
    #     elif (type == 'excel'):
    #         filename = OUTPUT_FILE_DIRECTORY + filename + '.xlsx'
    #     else:
    #         filename = OUTPUT_FILE_DIRECTORY + filename + '.csv'
    #     # check if file exists
    #     try:
    #         file_exists = os.path.isfile(filename)
    #         if (file_exists):
    #             if (type == 'csv'):
    #                 self.dataframe().to_csv(filename, mode='a', index=False, header=False)
    #             elif (type == 'excel'):
    #                 self.dataframe().to_excel(filename, mode='a', index=False, header=False)
    #         else:
    #             if (type == 'csv'):
    #                 self.dataframe().to_csv(filename, index=False)
    #             elif (type == 'excel'):
    #                 self.dataframe().to_excel(filename, index=False)
                
    #     except Exception as e:
    #         print('Failed to save data to file!!')
    #         print(f'Exception: {e.__class__.__name__}: {e}')
    #     else:
    #         print(filename + ' saved Successfully!')