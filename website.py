from dataclasses import dataclass

@dataclass
class Website:
    name:str
    url:str
    targetPattern:str
    absoluteUrl:bool
    paginationTag:str
    itemsTag:str
    categoryTag:str
    titleTag:str
    ratingTag:str
    priceTag:str
    availabilityTag:str
    linkTag:str