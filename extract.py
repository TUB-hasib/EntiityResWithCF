# extract 2 files from internet and create csv file from them based on constraints
import tarfile
import requests
from io import BytesIO
import pandas as pd

def downloadFile(url:str):
    
    response = requests.get(url)

    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the response content
        file_object = BytesIO(response.content)

        # Use tarfile to extract the contents
        with tarfile.open(fileobj=file_object, mode='r:gz') as tar:
            tar.extractall(path='data')
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
    
def findPublicationVenue(publicationVenue:str|None) -> str | None :

    venue = publicationVenue.upper()
    sigmod = "SIGMOD"
    vldb = "VLDB"
    if sigmod in venue:
        return sigmod
    elif vldb in venue:
        return vldb
    elif "VDLB" in venue:
        return vldb
    else:
        return None

def findPublicationYear(publicationYear: int|None) -> int | None :
    if publicationYear >= 1995 and publicationYear <= 2004:
        return publicationYear
    else:
        return None


def cleanAndLaodFile(fileUrl:str, saveUrl: str) -> list: 

    with open(fileUrl) as fileReader:
        records = []
        record = None
        for line in fileReader:
            line = line.strip()
            if line.startswith("#*"):

                if record and "yearOfPublication" in record and "publicationVenue" in record:
                    records.append(record)
                    # print(f"record - {record}")
                    
                record = {}
                record = {"publicationTitle": line[2:].strip()}

            elif line.startswith("#@"):
                record["Authors"] = line[2:].strip().split(", ")
            
            elif line.startswith("#index"):
                record["publicationID"] = line[6:].strip()

            elif line.startswith("#t"):
                publicationYear = findPublicationYear(int(line[2:].strip()))
                if publicationYear is None:
                    continue
                else:
                    record["yearOfPublication"] = publicationYear

            elif line.startswith("#c"): 
                publicationVenue = findPublicationVenue(line[2:])
                if publicationVenue is None:
                    continue
                else:
                    record["publicationVenue"] = publicationVenue
        
        df = pd.DataFrame(records)
        df.to_csv(saveUrl, index=False)
        print(df)
        
        return records


def extract():
    
    #  downloading files from given url
    # urlAcm = "https://lfs.aminer.cn/lab-datasets/citation/citation-acm-v8.txt.tgz"
    # urlDblp = "https://lfs.aminer.cn/lab-datasets/citation/dblp.v8.tgz"
    # downloadFile(urlAcm)
    # downloadFile(urlDblp)



    # url from downladed files and also file where to save data 
    fileReaderACM = "data/sampleAcm.txt"
    csvWriterACM = "data/sampleACM.csv"

    fileReaderDBLP = "data/sampleDblp.txt"
    csvWriterDBLP = "data/sampleDBLP.csv"

    # fileReaderACM = "data/citation-acm-v8.txt"
    # csvWriterACM = "data/ACM_1995_2004.csv"

    # fileReaderDBLP = "data/dblp.txt"
    # csvWriterDBLP = "data/DBLP_1995_2004.csv"

    # cleaning the data based on condition and saving it into csv files
    cleanAndLaodFile(fileReaderACM, csvWriterACM)
    cleanAndLaodFile(fileReaderDBLP, csvWriterDBLP)






if __name__ == "__main__":
    extract()
    