import asyncio

import aiohttp

from base import fetch_all, fetch_images

from base import Offer, ShortUrl, FirebaseScraperIO, RealEstate

from base import logging

from watermark_resolver import remove_watermark

from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim

import gc

import hashlib


# Sample url to check the scraper itself
# TODO: remove it

test_url: ShortUrl = ShortUrl('https://www.fazwaz.com/property-sales/1-bedroom-condo-for-sale-at-sapphire-luxurious-condominium-rama-3-in-bang-phongphang-bangkok-u1374472',
               'BUY')

SEARCH_BUY_URL: str = 'https://www.fazwaz.com/property-for-sale/thailand?order_by=rank|asc&page={}&center=13.507155459536346,101.82115065206693&bound=22.735656852206496,109.18199049581693:3.908098881894123,94.46031080831693'
SEARCH_RENT_URL: str = 'https://www.fazwaz.com/property-for-rent/thailand?order_by=rank|asc&page={}&center=13.507155459536346,101.82115065206693&bound=22.735656852206496,109.18199049581693:3.908098881894123,94.46031080831693'


# Used to declare how many items per iteration
# will be checked. res = pages_per_loop * buy/rent_iter 

PAGES_PER_LOOP: int = 15
RENT_ITER = 1 # TODO: set 2 on deploy
BUY_ITER = 1 # TODO: set 4 on deploy

# LOCATION = Nominatim(user_agent="GetLoc")

async def get_pages_data(loop: asyncio.BaseEventLoop) -> list[ShortUrl]:
    # GET MAX_PAGE_LOGIC
    htmls: list = await fetch_all([SEARCH_BUY_URL, SEARCH_RENT_URL], loop)
    soup_buy = BeautifulSoup(htmls[0], 'html.parser')
    soup_rent = BeautifulSoup(htmls[1], 'html.parser')

    buy_max_page: int = int(soup_buy.find_all(attrs={'class': 'page-link'})[-2].text.strip())
    rent_max_page: int = int(soup_rent.find_all(attrs={'class': 'page-link'})[-2].text.strip())
    
    buy_urls = []
    rent_urls = []

    urls_pack = []
    
    for i in range(0, BUY_ITER):  
        for p in range(i*PAGES_PER_LOOP, (i+1)*PAGES_PER_LOOP):
            if p < buy_max_page:
                urls_pack.append(SEARCH_BUY_URL.format(p))
        
        buy_pages_data = await fetch_all(urls_pack, loop)
        for page in buy_pages_data:
            soup = BeautifulSoup(page, 'html.parser')
            urls = soup.find_all('a', attrs={'class': 'link-unit'})
            for url in urls:
                buy_urls.append(url.attrs['href'])
        
        urls_pack.clear()
    
    for i in range(0, RENT_ITER):  
        for p in range(i*PAGES_PER_LOOP, (i+1)*PAGES_PER_LOOP):
            if p < rent_max_page:
                urls_pack.append(SEARCH_RENT_URL.format(p))
        
        rent_pages_data = await fetch_all(urls_pack, loop)
        for page in rent_pages_data:
            soup = BeautifulSoup(page, 'html.parser')
            urls = soup.find_all('a', attrs={'class': 'link-unit'})
            for url in urls:
                rent_urls.append(url.attrs['href'])
        
        urls_pack.clear()
    
    # print(urls)
    logging.info(f'Got {len(list(set(rent_urls)))} rent urls')
    logging.info(f'Got {len(list(set(buy_urls)))} buy urls')
    for url in buy_urls:
        urls_pack.append(ShortUrl(url, 'BUY'))
    for url in rent_urls:
        urls_pack.append(ShortUrl(url, 'RENT'))
    
    
    return urls_pack
        
        
async def get_real_estate(url: str, loop: asyncio.BaseEventLoop, ltype='CONDO') -> RealEstate:
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

    name = soup.find('h1', {'class': 'project-name'}).text
    # total_units = int(soup.find('small', attrs={'class': 'gallery-project-price-title__count-units'}).text.replace(' Units', ''))

    floors: int = 0
    buildings: int = 0
    units: int = 0
    off_plan: str = 'N/A'
    project_area: int = 0

    detail_info_tags = soup.find_all('div', attrs={'class': 'property-info-element'})
    for tag in detail_info_tags:
        tag_title = tag.find('small').text.strip()
        tag_value = tag.text.replace(tag_title, '').strip()

        match tag_title:
            case 'Floors':
                try:
                    floors: int = int(tag_value.replace(',', ''))
                except:
                    pass
            case 'Off Plan':
                off_plan = tag_value
            case 'Buildings':
                try:
                    buildings = int(tag_value.replace(',', ''))
                except:
                    pass
            case 'Units':
                try:
                    units = int(tag_value.replace(',', ''))
                except:
                    pass
            case 'Project Area':
                project_area = int(tag_value.replace('SqM', '').replace(',', '').strip())
    
    try:
        description = soup.find('div', attrs={'class': 'about-project'}).text.strip()
    except:
        description = "N/A"
    images = []
    image_tags = soup.find(attrs={'class': 'photo-gallery-detail-page'}).find_all('img')
    img_data = [{'url': img.attrs['src'], 'image_name': 'fwestate'+img.attrs['src'].split('/')[-1]} for img in image_tags]
    await fetch_images(image_data=img_data)
    for img in img_data:
        try:
            remove_watermark(img['image_name'])
            images.append(img['image_name'])
        except AttributeError:
            pass
    feature_tags = soup.find_all('li', attrs={'class': 'thumbnail-item'})
    features = []
    for f in feature_tags:
        features.append(f.find('div').text.strip())
    try:
        adress = soup.find('div', attrs={'class': 'project-location'}).text.strip()
    except:
        adress = 'N/A'
    
    loc = Nominatim(user_agent="Nominatim")
    longitude = 0
    latitude = 0
    getLoc = loc.geocode(adress)
    if getLoc is not None:
        longitude = getLoc.longitude
        latitude = getLoc.latitude
    else:
        logging.error('Location is not found')    

    internal_id = int(hashlib.sha1(name.encode("utf-8")).hexdigest(), 16) % (10 ** 8)
    return RealEstate(
        internal_id=internal_id,
        about=description,
        images=images, 
        image_urls=None,
        source='FAZWAZ',
        facilities=features,
        floors=floors,
        rentprice=0,
        squareprice=0,
        square=project_area,
        title=name,
        unitsTotal=units,
        unitsForRent=0,
        unitsForSale=0,
        adress=adress,
        ltype=ltype,
        longtitude=longitude,
        latitude=latitude,
        additional_attrs=({
            'name': 'offPlan',
            'value': off_plan
        }, {
            'name': 'buildings',
            'value': buildings
        })

    )


async def get_data_from_page(url: ShortUrl, loop: asyncio.BaseEventLoop) -> Offer | None:
    price_period = None
    images = []
    if url.realty_type == 'RENT':
        price_period = 'month'
    html: list = await fetch_all([url.url], loop)
    soup = BeautifulSoup(html[0], 'html.parser')
    try:
        name = soup.find(attrs={'class': 'unit-name'}).text
    except AttributeError:
        try:
            name = soup.find(attrs={'class': 'unit-name-no-project'}).text
        except AttributeError:
            return None
    
    try:
        adress = soup.find(attrs={'class': 'project-location'}).text.strip()
    except AttributeError:
        return None
    try:
        price = int(soup.find(attrs={'class': 'unit-sale-price__header-price'}).text.replace('฿', '').replace(',', '').strip())
    except ValueError:
        price = int(soup.find(attrs={'class': 'unit-sale-price__header-price'}).text.replace('฿', '').replace(',', '').strip().split('\n')[0])
   
    description = soup.find(attrs={'class': 'unit-view-description'}).text.replace('\xa0', ' ').strip()
    internal_id = soup.find(attrs={'class': 'unit-info-element__item__unit-id'}).text.strip()

    house_name: str = soup.find_all(attrs={'class': 'breadcrumb-item'})[-2].text.strip()

    search_location_str: str = adress
    city = adress.split(',')[-1].strip()
    search_location_str = search_location_str.strip()

    loc = Nominatim(user_agent="Nominatim")
    longitude: float = 0.0
    latitude: float = 0.0
    
    logging.debug(internal_id)
    try:
        # Not all objects have its project ->
        project_name = soup.find('a', attrs={'class': 'project-information-info'}).text.strip()
    except AttributeError:
        project_name = ""
    getLoc = loc.geocode(search_location_str+ " " + project_name)
    if getLoc is not None:
        longitude = getLoc.longitude
        latitude = getLoc.latitude
    else:
        getLoc = loc.geocode(search_location_str)
        if getLoc is not None:
            longitude = getLoc.longitude
            latitude = getLoc.latitude
        else:
            search_location_str = search_location_str.split(',')[0].strip() + ', ' + search_location_str.split(',')[1].strip()
            logging.info(search_location_str)
            getLoc = loc.geocode(search_location_str)
            if getLoc is not None:
                longitude = getLoc.longitude
                latitude = getLoc.latitude
            else:
                search_location_str = search_location_str.split(',')[0].strip()
                getLoc = loc.geocode(search_location_str)
                if getLoc is not None:
                    longitude = getLoc.longitude
                    latitude = getLoc.latitude
                else:
                    logging.critical(f'Location cannot be found on fazwaz. internal_id: {internal_id}')

    real_estate: RealEstate | None = None
    try:
        estate_url = soup.find('a', attrs={'class': 'project-information-info'}).attrs['href']
        real_estate = await get_real_estate(estate_url, loop)
    except (AttributeError, KeyError):
        logging.debug('Real estate not found here')

    # logging.info(search_location_str)
    logging.info("Getting FAZWAZ: " + internal_id)
    logging.info("Location: " + search_location_str+" " +project_name)
    if longitude == 0.0 and latitude == 0.0:
        logging.error("Location not found!")
    
    image_tags = soup.find(attrs={'class': 'photo-gallery-detail-page'}).find_all('img')
    img_data = [{'url': img.attrs['src'], 'image_name': img.attrs['src'].split('/')[-1]} for img in image_tags]
    await fetch_images(image_data=img_data)
    for img in img_data:
        try:
            remove_watermark(img['image_name'])
            images.append(img['image_name'])
        except AttributeError:
            pass

    info_tags = soup.find_all(attrs={'class': 'basic-information__item'})
    amentities = []
    property_type = 'HOUSE'
    land_area = 0
    inner_area = 0
    beds = 0
    bathes = 0
    area = 0
    floor = 0
    min_rental_duration = None
    for tag in info_tags:
        tag_topic: str = tag.find(attrs={'class': 'basic-information-topic'}).text.strip()
        tag_value: str = tag.find(attrs={'class': 'basic-information-info'}).text.strip()
        # print(tag_topic)
        # print(tag_value)
        # amentities.append(tag_topic)
        try:
            if tag_topic == 'Property Type':
                if tag_value.lower().find('house') != -1:
                    property_type = 'HOUSE'
                elif tag_value.lower().find('apartment') != -1:
                    property_type = 'APARTMENT'
                elif tag_value.lower().find('condo') != -1:
                    property_type = 'CONDO'
            if tag_topic == 'Min. Rental Duration':
                min_rental_duration = tag_value.lower().strip()
                if min_rental_duration.find('n/a') == -1:
                    if min_rental_duration.find('month') != -1:
                        min_rental_duration = min_rental_duration.replace('month', 'm').replace(' ', '')
                    else:
                        min_rental_duration = '12m'
            if tag_topic == 'Size':
                area = int(tag_value.lower().replace('sqm', ''))
            if tag_topic == 'Floor':
                floor = int(tag_value)
            if tag_topic == 'Bedroom' or tag_topic == 'Bedrooms':
                # beds = int(tag_value)
                pass
            if tag_topic == 'Available From':
                pass
            if tag_topic == 'Pets':
                pass
            if tag_topic == 'Construction':
                pass
            if tag_topic == 'Indoor Area':
                pass
            if tag_topic == 'Date Listed':
                pass
            if tag_topic == 'Unit ID':
                # internal_id = tag_value
                pass
            if tag_topic == 'Pets':
                pass
            if tag_topic == 'Pool Size':
                pass   
        except Exception as e:
            logging.error(f'{str(e)} on {tag_topic}')
    
    features_tags = soup.find_all('div', attrs={'class':'project-features__item'})
    for feature in features_tags:
        amentities.append(feature.text.strip())
    logging.info(str(amentities))
    # logging.info(getLoc.address)
    # logging.info(getLoc.longtitude, getLoc.latitude)

    base_features = soup.find_all('div', attrs={'class': 'property-info-element'})
    for feature in base_features:
        try:
            feature_type = feature.find('small').text.strip()
            feature_value = feature.text.replace(feature_type, '').strip()
            if feature_type.find('Bedroom') != -1:
                beds = int(feature_value)
            if feature_type.find('Bathroom') != -1:
                bathes = int(feature_value.replace('.', ','))
            if feature_type.find('Floor') != -1:
                floor = int(feature_value)
            if feature_type.find('Size') != -1:
                area = int(feature_value.lower().replace('sqm', ''))
            
        except AttributeError:
            logging.error('Error in attribure "property-info-element" found!')
        except ValueError:
            logging.error(f'Error with converting value in {feature_type}')
    

    offer = Offer(
        source='FAZWAZ',
        name=name,
        internal_id=internal_id,
        property_type=property_type,
        city=city,
        location=adress,
        realty_type=url.realty_type,
        price=price,
        price_period=price_period,
        description=description,
        phone='+66 (0) 2 026 8323',
        amentities=amentities,
        images=images,
        image_urls=None,
        latitude=latitude,
        longtitude=longitude,
        beds=beds,
        bathes=bathes,
        floors=floor,
        area=area,
        toilets=0,
        land_area=0,
        min_rental_duration=min_rental_duration,
        real_estate=real_estate
    )
    
    unreachable = gc.collect()
    logging.debug("GC collected: " + str(unreachable))
    if offer.latitude == offer.longtitude and offer.longtitude == 0:
        return None
    return offer

async def main(loop: asyncio.AbstractEventLoop):
    z = await get_pages_data(loop)
    scraperIO = FirebaseScraperIO()

    for a in z:
        data = await get_data_from_page(a, loop)
        if data != None:
            scraperIO.add_or_update(data)
            if data.real_estate is not None:
                # logging.debug(data.real_estate)
                scraperIO.add_or_update_estate(data.real_estate)
        else: 
            logging.critical('Offer skipping')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(main(loop))
    exit()