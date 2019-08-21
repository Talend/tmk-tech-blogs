from math import ceil, sin, cos, sqrt, atan2, radians

output = input

# Compute distance between two coordinates (lat, long)
# Approximate radius of earth in km

R = 6373.0
lat1 = radians(49.00838)
lon1 = radians(2.538441)
lat2 = radians(abs(input['Lat']))
lon2 = radians(abs(input['Long']))
dlon = lon2 - lon1
dlat = lat2 - lat1

a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
c = 2 * atan2(sqrt(a), sqrt(1 - a))

distance = R * c

# Return distance from CDG airport (miles)
output['Distance'] = ceil(distance * 0.621371)
