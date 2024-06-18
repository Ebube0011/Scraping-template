from dataclasses import dataclass, asdict, field
from pandas import json_normalize

@dataclass
class Content:
    category:str = ''
    title:str = ''
    rating:str = ''
    price:str = ''
    availability:str = ''
    link:str = ''

@dataclass
class Dataset:
    endpoint:str = ''
    records:list[Content] = field(default_factory=list)
    
    def dataframe(self):
        return json_normalize(
            data=[asdict(record) for record in self.records],
            # record_path="counties", 
            # meta=["state", "shortname", ["info", "governor"]],
            # errors='ignore',
            # max_level=1,
            )