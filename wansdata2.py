import csv, time, os, pandas as pd
from datetime import date
from tqdm import tqdm
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive 

#Create csv file name variables
csv_file_name = "inquiry.csv" 

# Use tqdm to display a status bar while loading the CSV file
with tqdm(total=100, desc="Loading CSV file") as pbar:
    df = pd.read_csv(csv_file_name, chunksize=10000)
    df_chunks = []
    for chunk in df:
        df_chunks.append(chunk)
        pbar.update(1)

#Creating a pandas dataframe from the csv data
df = pd.concat(df_chunks, axis=0)

# Encoding Data | Dropping Unwanted Columns | Renaming Columns
df = df.drop(columns=['Area','E-Mail Sent','Time to\nrespond','for\ncalc','Inq\nDate','Inq\nPROP','A\nA/C','CONTACT'])
df = df.rename(columns={'Building Name':'Building_Name','Client Name':'Client_Name','Inquiry Date':'Inquiry_Date'})
df['Price'] = df['Price'].str.replace(',','').fillna(0).astype(int)

# The index of the column to extract unique values from
column_index = 5  

unique_values = set()

# Use tqdm to display a status bar while filtering the data
with tqdm(total=len(df), desc="Filtering data") as pbar:
    with open(csv_file_name, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row

        for row in reader:
            unique_values.add(row[column_index])
            pbar.update(1)

#Create a list of all unique buildings in the data set
allBuildings = list(unique_values)

#Sort the list of buildlings
sortedBuildings = allBuildings.sort()

#Create a list of buildings to filter from
filterBuildings = ['Blocs 77', 'American Passport', 'Supalai Oriental Sukhumvit 39', 'The Esse Asoke', 'Fair Tower', 'The Base Park West Sukhumvit 77', 'Chambers On-Nut Station', 'Serio Sukhumvit 50', 'Life Sukhumvit 48', 'The Base Park East Sukhumvit 77', 'The Next Garden Suite', 'The Crest', 'Icon Iii', 'Aspire Sukhumvit 48', 'The Waterford Diamond', 'House By The Pond', 'Thonglor Tower', 'D 65', 'Kawa Haus', 'Xt Ekkamai', 'Charan Tower', 'Siri At Sukhumvit', 'Grand Mercure Bangkok Asoke Residence', 'The Nest Sukhumvit 71', 'Hyde Sukhumvit', 'The Esse At Singha Complex', 'Siamese Gioia', 'Supalai Premier @ Asoke', 'The Room Sukhumvit 79', 'Sathorn Gardens', 'Sukhumvit City Resort', 'Monterey Place']

#Create pricing standard for data filtering
oneBedPrice = 15000
twoBedPrice = 20000
threeBedPrice = 65000
fourBedPrice = 75000

#Filter the data based on Building|Price|Bedrooms
filtered_data = df.loc[(df.Building_Name.isin(filterBuildings)) & (((df.Price > oneBedPrice) & (df.Bed == 1)) |((df.Price > twoBedPrice) & (df.Bed == 2)) | ((df.Price > threeBedPrice) & (df.Bed == 3)) | ((df.Price > fourBedPrice) & (df.Bed == 4)))]

#Filter the dataframe down to specific columns
final_filter = filtered_data[['Client_Name','Type','Building_Name','R/B','Bed','Price','Sqm','Inquiry_Date','Agent','Comment']]

#A variable to filter the datafram to only return units that are available for rent and fill NaN with "No_Data"
clientListRentals = final_filter.loc[final_filter['R/B']=='Rent'].fillna('No_Data')

#Set today's date
today = date.today()

#Create output csv file name for the daily report to include todays date
outputFileName = 'WANSDaily_Report24thmar'+str(today)+'.csv'

#Export datafram to csv file
clientListRentals.to_csv(outputFileName, index=False)

#Display message when export is complete
print('Daily Lead .csv File Created Successfully')

#Sleep program to allow the the exported file to be updated in the file system before next steps
time.sleep(5)

# Authenticate and create the Google Drive client
gauth = GoogleAuth()
#Authenticate using a local webserver
gauth.LocalWebserverAuth() 
drive = GoogleDrive(gauth)

#Upload folder ID
folder_id = '1lbpkRDwH2PciH6FtZ4YOllL0iDGLYEZw'

#File path on local machine
file_path = outputFileName

# Creating the file object in Google Drive
file_title = os.path.basename(file_path)
file_drive = drive.CreateFile({
    'title': file_title,
    'parents': [{'id': folder_id}]
})

# Setting the content of the file to the local file's content
file_drive.SetContentFile(file_path)

# Uploading the file to Google Drive
file_drive.Upload()

#Display a success message to terminal
print(f"File '{file_title}' uploaded to Google Drive folder '{folder_id}'.")
