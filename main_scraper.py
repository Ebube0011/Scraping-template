from beautifulCrawler import Crawler
import asyncio

async def main():
    ''' Instantiate the crawler to crawl through the provided websites '''
    # crawl websites
    async with Crawler() as spider_man:
        await spider_man.run()

if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    asyncio.run(main())