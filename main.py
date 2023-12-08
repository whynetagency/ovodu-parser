import asyncio
from scraper_fazwaz import main as fazwaz_page_scraper
from scraper_thailandproperty import main as thaiprop_page_scraper

from base import FirebaseScraperIO
from base import logging

import datetime
import schedule
import time



s_time = input("Введите время запуска \n пример: 01:00 \n -->")
RUN_TIME = s_time

run_now = bool(input("Запустить сейчас? \n 1 - да, 0 - нет \n -->")) 

def main() -> None:
    logging.info('Scraper loop starting')
    loop = asyncio.get_event_loop()

    loop.run_until_complete(fazwaz_page_scraper(loop))
    loop.run_until_complete(thaiprop_page_scraper(loop))
    
    


schedule.every().day.at(RUN_TIME).do(main)

if __name__ == '__main__':
    if run_now:
        main()
    while True:
        schedule.run_pending()
        time.sleep(30)
    
