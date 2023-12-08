import asyncio
from bs4 import BeautifulSoup
# import aiohttp
# import ssl

from base import fetch_all, fetch_images, clear_text, amentities_divider

from base import ShortUrl, Offer, FirebaseScraperIO, RealEstate

import time

import logging

import gc
import aiohttp

# import requests

# phone_base_request_url = 'https://www.thailand-property.com/get-agent-info?pv_id=sea_th_pv_7d1f9ae6-447a-421d-8d04-0bb8ef67e4ef'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

BUY_TEST_URL = 'https://www.thailand-property.com/ads/condo-for-sale-in-sea-sky-condominium-phuket-karon-phuket_e7a74285d35d-31ae-e256-515a-97273c40'

DEMO_MODE = False

BUY_BASE_URL = 'https://www.thailand-property.com/properties-for-sale?page='
RENT_BASE_URL = 'https://www.thailand-property.com/properties-for-rent?page='


SEARCH_BUY_HOUSE: str = 'https://www.thailand-property.com/houses-for-sale?page='

SEARCH_BUY_CONDO: str = 'https://www.thailand-property.com/condos-for-sale?page='

SEARCH_RENT_CONDO: str = 'https://www.thailand-property.com/condos-for-rent?page='

SEARCH_RENT_HOUSE: str = 'https://www.thailand-property.com/houses-for-rent?page='

IMAGE_URL_PREFIX = 'https://img.thailand-property.com/'

async def get_data_from_page(url: ShortUrl, loop: asyncio.BaseEventLoop):
    logging.debug(f'Getting {url.realty_type} {url.url}')
    html = await fetch_all([url.url], loop)
    # print(html[0])
    soup = BeautifulSoup(html[0], 'html.parser')

    name = soup.find(attrs={'class': 'page-title'}).text.replace('\n', '').strip()

    property_type = 'HOUSE'
    if name.lower().find('condo') != -1:
        property_type = 'CONDO'
    elif name.lower().find('apartment') != -1:
        property_type = 'APARTMENT'

    internal_id = soup.find(attrs={'class': 'internal-ref'}).text.replace('\n', '').strip().replace('Listing ID: ', '')
    location = soup.find(attrs={'class': 'location'}).text.replace('\n', '').strip()
    city = location.split(',')[0]
    location = location.replace(city, '')
    if url.realty_type == 'RENT':
        price = soup.find(attrs={'class': 'price-title'}).text.replace('\n', '').strip().replace('Rent: ฿ ', '')
    else:
        price = soup.find(attrs={'class': 'price-title'}).text.replace('\n', '').strip().replace('Sale: ฿ ', '')
    
    price_period = None
    if url.realty_type == 'RENT':
        price_period = price.split('/')[1].strip()
    
    price = price.split('/')[0].strip()
    description = soup.find(attrs={'class': 'text-description'}).text.replace('\n', '').replace('\xa0', ' ')\
        .replace('Telephone number:     View Phone', '').strip()
    
    facilities = []
    try:
        facilities_tags = soup.find('ul', attrs={'class': 'facilities'}).find_all('li')
        
        for f in facilities_tags:
            facilities.append(f.text.replace('\n', '').strip().lower())
    except:
        pass
    
    
    # Download all images for further upload to S3
    image_data = []
    image_tags = soup.find(attrs={'id': 'hiddenGallery'}).find_all('li')
    for i in range(len(image_tags)):
        image_data.append({
            'url': image_tags[i].attrs['data-src'],
            'image_name': 'thaiprop_' + str(internal_id) + '_' + str(i) +'.jpg' 
        })
    
    await fetch_images(image_data=image_data)

    coords_el = str(soup.find(attrs={'id': 'appleMapsModal'}))
    blocks = coords_el.split('script')
    gps_lon = None
    gps_lat = None
    for b in blocks:
        if b.find('var gps_lon =') != -1:
            gps_lon = b.replace('> var gps_lon = "', '').replace('";</', '')
        if b.find('var gps_lat =') != -1:
            gps_lat = b.replace('> var gps_lat = "', '').replace('";</', '')

    # print(gps_lon, gps_lat)
    # print(f'Downloaded {len(image_data)} images')
    
    beds = 1
    bathes = 1
    usable_area = '40'
    floors = 1
    land_area = None

    key_features = soup.find('ul', attrs={'class': 'key-featured'}).find_all('li')
    
    for feature in key_features:
        feature_value = feature.find('span').text
        # print(feature.text)
        # print(feature_value)
        if feature.text.find('Bed') != -1:
            beds = int(feature_value)
        elif feature.text.find('Bath') != -1:
            bathes = int(feature_value)
        elif feature.text.find('Usable area') != -1:
            usable_area = int(feature_value.replace(' m2', '').replace('.', ','))
        elif feature.text.find('Land area') != -1:
            land_area = feature_value
        elif feature.text.find('Floor') != -1:
            floors = int(feature_value)

    # try:
    try:
        real_estate = await get_complex(loop,
                                soup.find('div', attrs={'class': 'user-company-detail'})\
                                    .find('a').attrs['href'], property_type)
    except:
        real_estate = None
    # except Exception as e:
    #     input(str(e))
    #     real_estate = None      

    offer = Offer(
        source='THPROP',
        name=name,
        internal_id=internal_id,
        property_type=property_type,
        city=city,
        location=location,
        realty_type=url.realty_type,
        price=price,
        price_period=price_period,
        description=description,
        phone='+6625088490',
        amentities=amentities_divider(facilities),
        images=[img['image_name'] for img in image_data],
        image_urls=None,
        latitude=gps_lat,
        longtitude=gps_lon,
        beds=beds,
        bathes=bathes,
        floors=floors,
        area=usable_area,
        toilets=bathes,
        land_area=land_area,
        min_rental_duration="1m" if url.realty_type == "rent" else None,
        real_estate=real_estate
    )

    return offer

urls = ['https://www.thailand-property.com/ads/1-bedroom-condo-for-rent-in-chalong-miracle-pool-villa-chalong-phuket_6f2da4f7f921-a581-06f6-ee5a-45c88268']


async def get_complex(loop: asyncio.BaseEventLoop, url: str, ltype='CONDO') -> str:
    ''' get complex unit, returns real-estate id '''

    async with aiohttp.ClientSession(loop=loop) as session:
        async with session.get(url) as response:
            response = await response.text()
    if ltype == 'APARTMENT':
        ltype = 'houses'
    elif ltype == 'CONDO':
        ltype = 'condos'
    elif ltype == 'HOUSE':
        ltype = 'houses'
    else:
        logging.critical(f'Unexpected type {ltype}, setting as houses')
        ltype = 'houses'
    
    soup = BeautifulSoup(response, 'html.parser')
    internal_id=url.split('/')[4]
    # print(internal_id)
    name = soup.find('h1', attrs={'class': 'page-title'}).text.strip()

    images = soup.find('div', attrs={'id': 'hiddenGallery'}).find_all('li')
    image_urls = []
    for i in range(len(images)):
        image_urls.append({
            'url': images[i].attrs['data-src'],
            'image_name': 'thaiprop_estate_no_' + str(internal_id) + '_' + str(i) +'.jpg' 
        })
    await fetch_images(image_data=image_urls)
    coords_el = str(soup.find(attrs={'id': 'appleMapsModal'}))
    blocks = coords_el.split('script')
    gps_lon = None
    gps_lat = None
    for b in blocks:
        if b.find('var gps_lon =') != -1:
            gps_lon = b.replace('> var gps_lon = "', '').replace('";</', '')
        if b.find('var gps_lat =') != -1:
            gps_lat = b.replace('> var gps_lat = "', '').replace('";</', '')
    
    description = soup.find('div', attrs={'class': 'description'}).text.strip()

    try:
        facilities = list(map(lambda x: x.text.strip(),
                        soup.find('ul', attrs={'class': 'facilities'}).find_all('li')
        ))
    except:
        facilities = []
    try:
        items_for_sale = int(soup.find('a', attrs={'id': 'open-tab-sale'}).text.strip().replace(' units', ''))
    except AttributeError:
        items_for_sale = 0
    try:
        items_for_rent = int(soup.find('a', attrs={'id': 'open-tab-rent'}).text.strip().replace(' units', ''))
    except AttributeError:
        items_for_rent = 0
    
    try:
        adress = soup.find('div', attrs={'class': 'view-on-map-info-location'}).attrs['title']
    except:
        adress = 'N/A'


    return RealEstate(
        internal_id=internal_id,
        about=description,
        images=[img['image_name'] for img in image_urls],
        image_urls=None,
        source='THAIPROP',
        facilities=facilities,
        floors=0,
        rentprice=0,
        squareprice=0,
        square=0,
        title=name,
        unitsForRent=items_for_rent,
        unitsForSale=items_for_sale,
        unitsTotal=items_for_rent+items_for_sale,
        adress=adress,
        longtitude=gps_lon,
        ltype=ltype,
        latitude=gps_lat
    )



async def get_offer_urls(loop: asyncio.BaseEventLoop) -> list[ShortUrl]:
    start_time = time.time()
    res = []

    rent_available = True
    rent_page_No = 1
    rent_pages_urls = []
    buy_available = True
    buy_page_No = 1
    buy_pages_urls = []
    while rent_available and buy_available:
        if rent_available:
            for i in range(rent_page_No, rent_page_No+10):
                rent_pages_urls.append(RENT_BASE_URL+str(i))
            rent_page_No += 10
            responses = await fetch_all(rent_pages_urls, loop)
            rent_pages_urls.clear()
            for html in responses:
                soup = BeautifulSoup(html, 'html.parser')
                items = soup.find_all(attrs={'class': 'hj-listing-snippet'})
                
                # print(items)
                if len(items) == 0:
                    rent_available = False
                else:
                    for item in items:
                        res.append(ShortUrl(url=item.attrs['href'], realty_type='RENT'))
        if buy_available:
            for i in range(buy_page_No, buy_page_No+10):
                buy_pages_urls.append(BUY_BASE_URL+str(i))
            buy_page_No += 10
            responses = await fetch_all(buy_pages_urls, loop)
            buy_pages_urls.clear()
            for html in responses:
                soup = BeautifulSoup(html, 'html.parser')
                items = soup.find_all(attrs={'class': 'hj-listing-snippet'})
                if len(items) == 0:
                    buy_available = False
                else:
                    for item in items:
                        res.append(ShortUrl(url=item.attrs['href'], realty_type='BUY'))
        # TODO: Remove on deploy
        break

    end_time = time.time()
    logging.info(f'Items found: {len(res)}')  
    logging.info(f'search time: {end_time - start_time}')  
    return res
            

async def main(loop: asyncio.BaseEventLoop):
    scraperIO = FirebaseScraperIO()

    urls = await get_offer_urls(loop)

    urls_2 = []
    for url in urls:
        if url not in urls_2:
            urls_2.append(url)
    urls = urls_2

    start_time = time.time()

    results = [] 
    for url in urls:
        logging.info(f'getting {urls.index(url)} / {len(urls)}')
        results.append(await get_data_from_page(url, loop))
        offer: Offer
        for offer in results:
            
            scraperIO.add_or_update(offer)
            if offer.real_estate is not None:
                # logging.debug(offer.real_estate)
                scraperIO.add_or_update_estate(offer.real_estate)
            # input('a')
        results.clear()

    end_time = time.time()
    # logging.debug(results)
    logging.info(f'Scraping time: {end_time - start_time}') 



if __name__ == '__main__':
    logging.info(f'{__name__} is running')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    exit()
