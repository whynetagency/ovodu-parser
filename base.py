'''
    Scraper frequently used functions and constants
'''

import os
import json
import random

from datetime import datetime, timedelta

from dataclasses import dataclass

import aiohttp
import ssl
import asyncio

import firebase_admin
from firebase_admin import credentials, storage
from firebase_admin import firestore

import boto3

import logging


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

s3 = boto3.client('s3')
bucket_name = 'ovodu-images'

SERVICE_DICT = {
    'listingTypes': {
        'RENT': 'publish-listing.start.rentType',
        'SELL': 'publish-listing.start.sellType'
    },
    'propertyTypes': {
        'ROOM': 'publish-listing.start.room',
        'APARTMENT': 'publish-listing.start.apartment',
        'HOUSE': 'publish-listing.start.house'
    },
    'minimumStay': {
        '1m': 'minMonths.option1',
        '2m': 'minMonths.option2',
        '3m': 'minMonths.option3',
        '4m': 'minMonths.option4',
        '5m': 'minMonths.option5',
        '6m': 'minMonths.option6',
        '7m': 'minMonths.option7',
        '8m': 'minMonths.option8',
        '9m': 'minMonths.option9',
        '10m': 'minMonths.option10',
        '11m': 'minMonths.option11',
        '12m': 'minMonths.option12',
        '13m': 'minMonths.option13',
        '14m': 'minMonths.option14',
        '15m': 'minMonths.option15',
        '16m': 'minMonths.option16',
        '17m': 'minMonths.option17',
        '18m': 'minMonths.option18',
        '19m': 'minMonths.option19',
        '20m': 'minMonths.option20',
        '21m': 'minMonths.option21',
        '22m': 'minMonths.option22',
        '23m': 'minMonths.option23',
        '24m': 'minMonths.option24',
    },
    'maximumStay': {
        '-': 'maxMonths.option1',
        '1m': 'maxMonths.option2',
        '2m': 'maxMonths.option3',
        '3m': 'maxMonths.option4',
        '4m': 'maxMonths.option5',
        '5m': 'maxMonths.option6',
        '6m': 'maxMonths.option7',
        '7m': 'maxMonths.option8',
        '8m': 'maxMonths.option9',
        '9m': 'maxMonths.option10',
        '10m': 'maxMonths.option11',
        '11m': 'maxMonths.option12',
        '12m': 'maxMonths.option13',
        '13m': 'maxMonths.option14',
        '14m': 'maxMonths.option15',
        '15m': 'maxMonths.option16',
        '16m': 'maxMonths.option17',
        '17m': 'maxMonths.option18',
        '18m': 'maxMonths.option19',
        '19m': 'maxMonths.option20',
        '20m': 'maxMonths.option21',
        '21m': 'maxMonths.option22',
        '22m': 'maxMonths.option23',
        '23m': 'maxMonths.option24',
        '24m': 'maxMonths.option25',
    },
    'includedServices': {
        'rental contract': 'services.option1',
        'cleaning service': 'services.option2',
        'city Hall registration support': 'services.option3',
        'maintenance service': 'services.option4',
    },
    'activityType': {
        'studies': 'publish-listing.step-six.studyOption',
        'works': 'publish-listing.step-six.workOption',
        'works or studies': 'publish-listing.step-six.workStudyOption',
    },
    'gender': {
        'M': 'publish-listing.step-six.maleOption',
        'F': 'publish-listing.step-six.femaleOption',
        'N': 'publish-listing.step-six.nonBinaryOption'
    },
    'amenities': {
        'tv': 'amenities.name1',
        'wi-fi': 'amenities.name2',
        'air conditioning': 'amenities.name3',
        'parking': 'amenities.name4',
        'heating': 'amenities.name5',
        'lift': 'amenities.name6',
        'washing machine': 'amenities.name7',
        'dryer': 'amenities.name8',
        'doorman': 'amenities.name9',
        'furnished': 'amenities.name10',
        'pool': 'amenities.name11',
        'dishwasher': 'amenities.name12',
        'wheelchair friendly': 'amenities.name13',
        'garden': 'amenities.name14',
        'terrace': 'amenities.name15',
        'working space': 'amenities.name16',
        'gym': 'amenities.name17',
        'jacuzzi': 'amenities.name18',
        'sauna': 'amenities.name19',
    },
    'bedType': {
        'sofa bed': 'publish-listing.step-two.bedTypeOption1',
        'single bed': 'publish-listing.step-two.bedTypeOption2',
        'double bed': 'ublish-listing.step-two.bedTypeOption3'
    },
    'rules': {
        'pet friendly': 'rules.name2',
        'smoker friendly': 'rules.name1'
    },
    'space': {
        'interior': 'publish-listing.step-two.interiorOption',
        'exterior': 'publish-listing.step-two.exteriorOption'
    }
}

AMENTITIES_SYNONYMS = {
    'internet': 'Wi-fi',
    'elevator': 'Lift',
    'swimming pool': 'Pool',
    'car park': 'Parking'
}

BASE_AWS_IMG_URL = 'https://ovodu-images.s3.eu-north-1.amazonaws.com/property_images/{}'
BASE_AWS_IMG_URL_ESTATE = 'https://ovodu-images.s3.eu-north-1.amazonaws.com/estate_images/{}'


async def fetch(session, url):

    async with session.get(url, ssl=ssl.SSLContext()) as response:
        res = await response.text()
        return res
    


async def fetch_all(urls, loop):
    
    async with aiohttp.ClientSession(loop=loop) as session:
        results = await asyncio.gather(*[fetch(session, url) for url in urls])
        return results
    
        

async def download_image(session, url, img_name):
    
    try:
        async with session.get(url) as response:
            if response.status == 200:
                image_data = await response.read()
                image_path = os.path.join('downloaded_images', img_name)
                with open(image_path, 'wb') as f:
                    f.write(image_data)
                # print(f"Downloaded {image_name}")
            else:
                print(f"Failed to download {url}: {response.status}")
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")

async def fetch_images(image_data: list[dict[str, str]]):
    # Takes images in format of list[{'url', 'image_name'}]
    os.makedirs('downloaded_images', exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = [download_image(session, data['url'], data['image_name']) for data in image_data]
        await asyncio.gather(*tasks)
    

def clear_text(text: str) -> str:
    return text.replace('\n', '').strip()

def generate_upload_date() -> datetime:
    now: datetime = datetime.now()

    time_before: timedelta = timedelta(days=random.randint(0, 2), hours=random.randint(0, 23))

    now -= time_before

    return now

def amentities_divider(amentities: list) -> dict[list, list]:
    '''
        Splits amentities onto the ones which are in 
        SERVICE_DICT.amentities and not_listed_amentities.
        uses amentities synonyms to divide them
    '''
    listed_amentities = []
    not_listed_amentities = []

    for amentity in amentities:
        if SERVICE_DICT['amenities'].get(amentity, None) is not None:
            listed_amentities.append(SERVICE_DICT['amenities'].get(amentity))
        elif AMENTITIES_SYNONYMS.get(amentity, None) is not None:
            listed_amentities.append(SERVICE_DICT['amenities'].get(AMENTITIES_SYNONYMS.get(amentity)))
        else:
            not_listed_amentities.append(amentity)
    return {
        'listed_amentities': listed_amentities,
        'not_listed_amentities': not_listed_amentities
    }


@dataclass(slots=True)
class ShortUrl:
    url: str
    realty_type: str


@dataclass()
class RealEstate:
    internal_id: str
    about: str 
    images: list[dict]
    image_urls: list[str]
    source: str # FAZWAZ / THPROP
    facilities: list[str]
    floors: int|None
    rentprice: int
    squareprice: int
    square: int
    title: str
    unitsForRent: int
    unitsForSale: int
    unitsTotal: int
    ltype: str
    adress: str
    longtitude: float
    latitude: float

    images_uploaded: bool = False
    additional_attrs: set[dict] = ()
    

    def get_unique_id(self) -> str:
        src = "FW" if self.source == 'FAZWAZ' else 'TH'
        
        return f"{src}REALESTATE{self.internal_id}"
    
    def to_dict(self) -> dict:
        if not self.images_uploaded:
            self.upload_images()

        return {
            'id': self.get_unique_id(),
            'about': self.about,
            'images': self.image_urls,
            'adress': {
                'latLng': {
                    'lat': self.latitude,
                    'lng': self.longtitude
                },
                'title': self.adress
            },
            'amenties': self.facilities,
            'currency': 'THB',
            'developer': 'QZqmAk2zpBzPjbDtgsfk',
            'title': self.title,
            'floors': self.floors,
            'area': self.square,
            'rentPrice': self.rentprice,
            'squarePrice': self.squareprice,
            'facilities': self.facilities,
            'unitsForRent': self.unitsForRent,
            'unitsForSale': self.unitsForSale,
            'unitsTotal': self.unitsTotal,
            'additionals': self.additional_attrs,
            'type': self.ltype
        }

    def clear_images(self) -> None:
        ''' Deletes downloaded images '''
        for img in self.images:
            try:
                os.remove('downloaded_images/'+img)
                logging.info(f'Image {img} deleted from local file system!')
            except FileNotFoundError:
                logging.error(f"File '{img}' not found.")
            except Exception as e:
                logging.critical(f"An error occurred: {e}")
    
    def upload_images(self) -> list[str]:
        ''' uploads downloaded images to AWS '''
        self.image_urls = []
        self.images_uploaded = True
        for img in self.images:
            try:
                s3.upload_file('downloaded_images/'+img, bucket_name, 'estate_images/'+img)
                self.image_urls.append(BASE_AWS_IMG_URL_ESTATE.format(img))
                logging.info(f'Image {img} uploaded to aws!')
            except FileNotFoundError:
                pass
        self.clear_images()
        return self.image_urls

@dataclass()
class Offer:
    image_urls: list[str]
    images: list[str]  # Stores images local path
    source: str # 'FAZWAZ' / 'THPROP'

    name: str
    internal_id: str
    property_type: str # CONDO/HOUSE/APARTMENT
    city: str
    location: str
    realty_type: str # RENT/SELL
    price: int
    price_period: str | None
    description: str
    phone: str
    
    latitude: int
    longtitude: int

    # Features
    beds: int
    toilets: int
    bathes: int
    floors: int
    area: int # m2


    

    amentities: dict[list, list]
    images_uploaded: bool = False

    land_area: str | None = None
    min_rental_duration: str | None = None

    real_estate: RealEstate|None = None

    def get_unique_id(self) -> str:
        src = "FW" if self.source == 'FAZWAZ' else 'TH'
        offer_type = "C"
        if offer_type == "APARTMENT":
            offer_type = 'A'
        elif offer_type == 'HOUSE':
            offer_type = 'H'
        return f"{src}{offer_type}{self.internal_id}"
    
    

    def to_dict(self) -> dict:
        if not self.images_uploaded:
            self.upload_images()
        amentities = amentities_divider(self.amentities)
        logging.debug(f"Images amount {len(self.image_urls)}")
        
        real_estate = None if self.real_estate is None else self.real_estate.get_unique_id()

        res = {
            'listingType': SERVICE_DICT['listingTypes'].get(self.property_type, 'publish-listing.start.rentType'),
            'listingFlatmate': None,
            'postedBy': 'pSlXm06WcQYLwo7c0ztECuGVoKG2',
            'visitCalendar': None,
            'listingAvailability': {
                'isBillsIncluded': False,
                'includedServices': [],
                'maximumStay': 'maxMonths.option1',
                'noDeposit': True,
                'from': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                'monthlyRent': self.price,
                'currency': 'THB',
                'minimumStay': SERVICE_DICT['minimumStay'].get(self.min_rental_duration, 'minMonths.option1')
            },
            'listingAddress': {
                'streetAddress': self.location,
                'adressDetail': '',
                'city': self.city,
                'latLng': {'lat': self.latitude, 'lng': self.longtitude}
            },
            'id': self.get_unique_id(),
            'internal_id': self.internal_id,
            'source': self.source,
            'listingAnalytics': {
                'views': [],
                'connections': [],
                'replies': [],
                'favourites': []
            },
            'propertyType': SERVICE_DICT['propertyTypes'].get(self.property_type),
            'postedAt': {'seconds': (datetime.now() - datetime(1970, 1, random.randint(1, 3), random.randint(1, 22))).total_seconds(), 'nanoseconds': 0}, # TODO: random time
            'status': 'ACTIVE',
            'listingDescription': {
                'mainPhoto': self.image_urls[0],
                'title': self.name,
                'uploadedPhotos': self.image_urls,
                'description': self.description
            }, 
            'listingProperty': {
                'roomSize': self.area,
                'space': None,
                'propertySize': self.land_area,
                'isBed': True,
                'bedroomsAmount': {
                    'single': 0,
                    'double': self.beds,
                },
                'isOwnerLiveInProperty': None,
                'bathroomsAmount': {
                    'toilets': self.toilets,
                    'bathrooms': self.bathes
                },
                'flatmates': {
                    'female': None,
                    'male': None,
                    'nonBinary': None
                },
                'type': SERVICE_DICT['propertyTypes'].get(self.property_type)
                
            },
            'type': SERVICE_DICT['listingTypes'].get(self.property_type, 'publish-listing.start.rentType'),
            'rules': None,
            'amentities': amentities['listed_amentities'],
            'not_listed_amentities': amentities['not_listed_amentities'],
            'rules': [],
            'bedType': 'ublish-listing.step-two.bedTypeOption3',
            'last_updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'real_estate': real_estate
        }
        return res
    
    def __str__(self) -> str:
        ''' for debug '''
       
        return f"Offer {self.get_unique_id()} \n src: {self.source} \n internal_id: {self.internal_id}"

    def clear_images(self) -> None:
        ''' Deletes downloaded images '''
        for img in self.images:
            try:
                os.remove('downloaded_images/'+img)
                logging.info(f'Image {img} deleted from local file system!')
            except FileNotFoundError:
                logging.error(f"File '{img}' not found.")
                
            except Exception as e:
                logging.critical(f"An error occurred: {e}")
    
    def upload_images(self) -> list[str]:
        ''' uploads downloaded images to AWS '''
        self.image_urls = []
        self.images_uploaded = True
        for img in self.images:
            s3.upload_file('downloaded_images/'+img, bucket_name, 'property_images/'+img)
            self.image_urls.append(BASE_AWS_IMG_URL.format(img))
            logging.info(f'Image {img} uploaded to aws!')
        self.clear_images()
        return self.image_urls


class FirebaseScraperIO:
    def __init__(self) -> None:
        try:
            self.cred = credentials.Certificate("creds.json")
            firebase_admin.initialize_app(self.cred)
            self.db = firestore.client()
            
            self.collections = self.db.collections()
            
            self.items_collection = self.db.collection("listings")
            self.estates_collection = self.db.collection("real-estate")
            logging.info('Firebase init success!')
        except Exception as e:
            logging.critical(f'Firebase init failed with the following error: {str(e)}')
    
    def add_or_update(self, offer: Offer):
        if type(offer) != Offer:
            logging.critical('Only objects of type Offer allowed!') 
            return
        query = self.items_collection.where("id", "==", offer.get_unique_id()).limit(1)
        query_result = query.stream()

        if len(list(query_result)) > 0:

            logging.debug(f'updating an item: {offer.get_unique_id()}')
            for item in query_result:
                item.reference.update({'listingAvailability': {'price': offer.price}})
            
        else:
            # Add the new item
            listing_ref = self.items_collection.document(offer.get_unique_id())
            logging.debug(f'Offer: {offer.get_unique_id()} created')
            listing_ref.set(offer.to_dict())
            logging.debug(f'Offer {offer.get_unique_id()} data set')    

    def add_or_update_estate(self, offer: RealEstate):
        if type(offer) != RealEstate:
            logging.critical('Only objects of type RealEstate allowed!') 
            return
        query = self.estates_collection.where("id", "==", offer.get_unique_id()).limit(1)
        query_result = query.stream()

        if len(list(query_result)) == 0:
            # Add the new item
            listing_ref = self.estates_collection.document(offer.get_unique_id())
            logging.debug(f'Estate: {offer.get_unique_id()} created')
            listing_ref.set(offer.to_dict())
            logging.debug(f'Estate {offer.get_unique_id()} data set')
            
        else:
            logging.debug(f'Estate with id {offer.get_unique_id()} already exists, skipping')
    
    def upload_image(self, file_path, destination_path):
        blob = self.bucket.blob(destination_path)
        blob.upload_from_filename(file_path)

if __name__ == '__main__':
    scraper_IO = FirebaseScraperIO()
    # cred = credentials.Certificate("creds.json")
    # firebase_admin.initialize_app(cred)
    # db = firestore.client()

    # collections = db.collections()

    # for collection in collections:
    #     print(f"Collection ID: {collection.id}")

    # items_collection = db.collection("listings")
    # available_items = items_collection.stream()
    # print(len(available_items))
    # x = list(available_items)[0].to_dict()
    # print(x)
    # listingTypes = []
    # propertyTypes = []
    # for item in available_items:
    #     item_data = item.to_dict()
        
        # listingTypes.append(item_data['listingType'])
        # propertyTypes.append(item_data['propertyType'])

    # listingTypes = list(set(listingTypes))
    # propertyTypes = list(set(propertyTypes))
    # print(listingTypes)
    # print(propertyTypes)


    # print(db)