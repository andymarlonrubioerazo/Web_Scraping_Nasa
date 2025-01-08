# %% [markdown]
# Hi, you need to install: pandas, requests, BeautifulSoup, re, datetime, pytube, threadpoolctl, numpy, pymongo, os.
# 
# Web scraping return a document call Nasa_apod.csv from the API NASA APOD, I extract the information: date, name, link, format("imagen" or "other"for video, gif), Explanation and download of each picture of the nasa.
# We send a little example because is a big file.  
# 
# Store data in MongoDB, we cann't do it. Sorry so much.

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import datetime
from threading import Thread
import os
import numpy as np


os.mkdir("imagen_pythonic")#CREATE A FOLDER CALLED "imagen_pythonic" WITHIN THE DIRECTORY 
os.chdir("imagen_pythonic") #CHANGE THE DIRECTORY 
print("In the next directory, we save the images")
url = 'https://apod.nasa.gov/apod/archivepix.html' #URL FOR WEB SCRAPING

response = requests.get(url) # Make the GET request
soup = BeautifulSoup(response.content, 'html.parser') # Parse HTML content
entriesa= soup.find_all('a') # FIND ALL ENTRANCE WITH  LABEL "a" 
andya=entriesa[3:-11]  ##DELETE UPPER AND LOWER TEXT    
ti = datetime.datetime.now() #GET EXACTLY TIME OF THE START
print("\n Start time ", ti)
data=[] #CREATE A VECTOR TO SAVE THE CONTENT

#########________FUNCTION: EXTRACT EACH TITLE, LINK, DATE, LINK IN EVERY PAGE WEB OF THE APOD-API NASA
def funcion(andya, xi,xf ): 
    for k in range(xi,xf):
        date = andya[k].previous_sibling.strip()  # GET THE DATE
        date=date.replace(":","")
        title = andya[k].text.strip()  # GET THE TITLE
        link = andya[k-1].find_next('a')['href']  # GET THE LINK
#         print(f'k={k},Date: {date}, Title: {title}, Link: https://apod.nasa.gov/apod/{link}')

        ########Extraction of the text in each link
        url = f"https://apod.nasa.gov/apod/{link}"
        response = requests.get(url)   
        soup = BeautifulSoup(response.content, 'html.parser')

        entries=soup.find_all('p')
        entries= BeautifulSoup(str(entries[2]), 'html.parser')
        texto_sin_html = entries.get_text(strip=False)
        texto_sin_html=texto_sin_html[16:]
        texto_sin_html=texto_sin_html.replace("\n","")

        test_str = texto_sin_html
        wrd ="Tomorrow's"
        test_str = test_str.split()
        res = -1
        s=0
        for idx in test_str: 
            if len(re.findall(wrd, idx)) > 0:
                res = test_str.index(idx) + 1
                break
            s+=len(idx)
        texto_sin_html=texto_sin_html[:s+res]
        ############################# End of the extraction of the text
        
        ######################## Download imagen of each link
        #FIND LABEL <img> AND GET  ATTRIBUTE "src"  AND EXTRACT  IMAGE'S URL
        entries=soup.find_all("img")
        if not entries:
            print("Video at",date)
            data.append([date, title,f"https://apod.nasa.gov/apod/{link}","Other",texto_sin_html] )
            continue
            
        data.append([date, title,f"https://apod.nasa.gov/apod/{link}","Image",texto_sin_html] )
        image_url = soup.find('img')['src']
        image_url=f'https://apod.nasa.gov/apod/{image_url}'
        # Section: download imagen
        response = requests.get(image_url)
        
        # Write the image in a file 
        d1=date.replace(" ","_")
        with open(f'{d1}.jpg', 'wb') as file:
            file.write(response.content)
        ################### End download of each link    
    
    
############____________________ END FUNCTION    
############################### MULTITHREADING  
if __name__=="__main__":
    
    hilos=[]
    cores=os.cpu_count()
    print("You have",cores," cores")
    ###################################
    x0=int(len(andya)/cores)
    xi=1
    xf=x0+1
    ####################################
    print("---- Instanciar")
    
    for n in range (cores-1):
        hilo=Thread(target=funcion, args=(andya, xi,xf )) ##input function for n-1 cores
        hilos.append(hilo)
        xi+=x0
        xf+=x0
    xf=len(andya)    
    hilo=Thread(target=funcion, args=(andya, xi,xf )) ##nput function for the last core
    hilos.append(hilo)
    print("-----Ejecutar")
    
    for hilo in hilos:
        hilo.start()    
    print("-----Espera")
    
    for hilo in hilos:
        hilo.join()
    print("------- Regreso a la ejercicion inicial")
    
    #########________Merge the information
    data=pd.DataFrame(data)
    data=data.rename(columns={0:"Date",1:"Name",2:"Link",3:"Format",4:"Explanation"})

    tf = datetime.datetime.now()
    print("\n End time ", tf)

    print("\n Lag Time",tf-ti)
    ########________End the MULTITHREADING  
    
    
year=[]
day=[]
month=[]
for k in data["Date"]:
    year.append(k[:4])
    day.append(k[-2:])
    month.append(k[5:-3])
    
data["day"]=pd.DataFrame(day)
data["month"]=pd.DataFrame(month)
data["year"]=pd.DataFrame(year)
del data["Date"]
new_cols = ["day","month","year","Name","Link","Format","Explanation"]
data = data[new_cols]
data.to_csv('Nasa_apod.csv',index=False) #Save all information in file .csv, its name is "Nasa_apod.csv"
