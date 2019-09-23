import urllib.parse, http.client, pandas as pd
import requests
import time
 
# Set globals, GA Property Id and obtain raw data
property_id = "UA-XXXXXXXXX-X"
csv = 'my_file.csv'

# Initiate counter, hit, dataframe from raw data
count = 0
hit = ''
df = pd.read_csv(csv)
 
# Initiate timer
start_time = time.time()
 
for index, row in df.iterrows():
    params = urllib.parse.urlencode({
            'v': 1,
            'aip': 1,
            'tid': property_id,
            'cid': row['client_id'],         # where clientId represents a column with header clientId in your csv
            't': 'event',
            'ec': row['event_category'],    # where event_category represents a column with header event_category in your csv
            'ea': row['event_action'],
            'el': row['event_label'],       # where event_clabel represents a column with header event_label in your csv
            'ni': 1,                        # set to 1 so this is passed into GA as a non-interaction hit
            'ds': row['data_source'],       # set the data source for the import
            'cd09': row['cd_09']            # where cd_09 represents a column with header cd_o9 in your csv, for example, to represent custom dimension index 9
    })
 
    hit += str(params) + '\r\n'             # Append each independent hit to our batched hit as a new line
    count += 1                       # Increment counter
 
    if (count == 20 or index == len(df) - 1):                       # when 20 hits have been batched, or end of file reach, send the batched hit
        connection = http.client.HTTPConnection('www.google-analytics.com')     #Initiate connection to collection endpoint
        r = connection.request('POST', '/batch', hit)                               # ensure /batch endpoint is used
        count = 0                                                               # reset counter for next batch
        print(hit)
        hit = ''                                                                # reset hits for next batch
 
# Print time taken to execute all hits
print("--- %s seconds ---" % (time.time() - start_time))