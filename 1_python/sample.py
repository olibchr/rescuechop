import pyModeS as pms
import csv

results = []

with open("rawMessageSample") as enc_msgs:

    for msg in enc_msgs:
        icao = pms.adsb.icao(msg)
        callsign = pms.adsb.callsign(msg)
        altitude = pms.adsb.altitude(msg)
        velocity = pms.adsb.velocity(msg)
        speed_heading = pms.adsb.speed_heading(msg)

        results.append([icao, callsign, altitude, velocity, speed_heading])

    with open('decoded_msg.csv', 'wb') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerow(results)