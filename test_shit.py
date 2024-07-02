from dataclasses import dataclass, asdict, field
# from multiprocessing.dummy import Pool as ThreadPool
# import threading
import time
from concurrent.futures import ThreadPoolExecutor

@dataclass
class Content:
    category:str
    title:str
    rating:float
    price:str
    availability:str
    link:str
    endpoint:str = field(repr=False, default='')
    # search_string:str = field(init=False)
def task(number:float):
    max_shit = 10
    book = Content(
        category='Fighting',
        title='Jungle Book',
        rating=number%max_shit,
        price='85.6',
        availability='12',
        link='123.com',
        endpoint='Food',
    )
    time.sleep(number)
    del book.endpoint
    print(asdict(book))

def main():
    shits = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 5, 5, 5, 5, 5, 5, 5]
    with ThreadPoolExecutor(max_workers=5) as exec:
            exec.map(task, range(30))
    print('work_done')

if __name__ == '__main__':
    main()