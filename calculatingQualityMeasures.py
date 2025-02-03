import pandas as pd
import ast

def calculateQualityMeasures():
    
    # reading data for matched entities
    urlErBase = "data/Matched Entities with Brute Force.csv"
    urlErWithBlocking = "data/Matched Entities.csv"

    dfBase = pd.read_csv(urlErBase)
    dfWithBlocking= pd.read_csv(urlErWithBlocking)

    noOfRecordBase = dfBase.shape[0]
    noOfRecordwithBlocking = dfWithBlocking.shape[0]
    print(f"no of entities in base : {dfBase.shape[0]}")
    print(f"no of entities with blocking :{dfWithBlocking.shape[0]}")


    # calculating accuracy 
    accuracy =  noOfRecordwithBlocking/ noOfRecordBase
    print(f"accuracy :{accuracy}")

    # calculating pricision and recall 
    truePositive = noOfRecordwithBlocking
    falsePositive = 0
    falseNegative = noOfRecordBase - noOfRecordwithBlocking
    
    pricision = truePositive / (truePositive + falsePositive)
    recall = truePositive / (truePositive + falseNegative)
    fScore = (2 * pricision * recall)/(pricision + recall)
    print(f"pricision :{pricision}")
    print(f"recall :{recall}")
    print(f"fScore :{fScore}")


    # dfCommonRecords = pd.merge(dfBase, dfWithBlocking, on ='recordId', how='inner')
    # print(dfCommonRecords)

    # duplicates_dfBase = dfBase[dfBase.duplicated(subset='recordId')]
    # duplicates_dfWithBlocking = dfWithBlocking[dfWithBlocking.duplicated(subset='recordId')]
    # duplicates_dfCommonRecords = dfCommonRecords[dfCommonRecords.duplicated(subset='recordId')]
    # print(duplicates_dfBase)
    # print(duplicates_dfWithBlocking)

    # print(duplicates_dfCommonRecords)






if __name__ == "__main__":
    calculateQualityMeasures()
    
    