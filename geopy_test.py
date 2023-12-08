# importing geopy library
from geopy.geocoders import Nominatim
from geopy.geocoders import SERVICE_TO_GEOCODER

for key in SERVICE_TO_GEOCODER:
    
    print(key)

    # calling the Nominatim tool
    loc = Nominatim(user_agent="Nominatim")
    
    # entering the location name

    loc_str = "Phichai" #2"
    getLoc = loc.geocode(loc_str)
    
    # printing address
    print("Search str: " + loc_str)

    if getLoc is not None:
        print(getLoc.address)

        print("Latitude = ", getLoc.latitude, "\n")
        print("Longitude = ", getLoc.longitude)
        print(key)
        exit()
    else:
        print("cant find location")

    break