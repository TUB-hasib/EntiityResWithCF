import pandas as pd
from pprint import pprint
from nltk.metrics import jaccard_distance
from erWithBlocking import calculateSimilarity, threshold  # same threshhold as  erWithBlocking. doing it to not change twice  
import time

def er():

    # # working with sample data url
    # urlACM = "data/sampleACM.csv"
    # urlDBLP = "data/sampleDBLP.csv"

    # real data url
    urlACM = "data/ACM_1995_2004.csv"
    urlDBLP = "data/DBLP_1995_2004.csv"

    # read data
    dfACM = pd.read_csv(urlACM)
    dfDBLP = pd.read_csv(urlDBLP)

    startTime = time.time()
    #********************************* MATCHING STAGE  *********************************
    allPairsMatches = []
    for indexAcm, recordAcm in dfACM.iterrows():
        acmItem = dict(recordAcm)
        # print(acmItem)
        for indexDblp, recordDblp  in dfDBLP.iterrows():
            # print(f"acm: {recordAcm['publicationTitle']} \ndblp: {recordDblp['publicationTitle']} ")
            dblpItem = dict(recordDblp)
            similarityValue = calculateSimilarity(acmItem, dblpItem, threshold)
            if similarityValue:
                # print(f'acm: {acmItem}')
                # print(f'{dblp: dblpItem}')
                # print(f"Jaccard Similarity: {similarityValue}")
                recordId = acmItem['publicationID'] + dblpItem['publicationID']
                match = {'recordId':recordId, 'acm':acmItem, 'dblp':dblpItem, 'similarityScore':similarityValue}
                allPairsMatches.append(match)
                # break  # if acm and dblp already has a value which matches, no need to continue current dblp inner loop

    df = pd.DataFrame(allPairsMatches)
    df.to_csv('data/Matched Entities with Brute Force.csv', index=False)
    print(df)

    endTime =  time.time()
    executionTime = endTime - startTime
    print(f"executionTime: {executionTime} sec")




if __name__ == "__main__":
    er()
    