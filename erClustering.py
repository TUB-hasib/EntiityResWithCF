import pandas as pd
import networkx 


def erClustering():
    #********************************* CLUSTERING STAGE  *********************************

    urlACM = "data/Matched Entities.csv"
    dfMatchedEnities = pd.read_csv(urlACM)


    dfClustering =dfMatchedEnities[['recordId']] # just need the recordID as the key from acm and dblp are already here
    dfClustering[['ACM', 'DBLP']] = dfClustering['recordId'].apply(lambda x: pd.Series([x[:24], x[24:]])) # each id size is 24. creating 2 col(acm and dblp id) by dividig the recordID
    edgeList = list(zip(dfClustering['ACM'], dfClustering['DBLP'])) # putting them in a list to calculate connected component. 

    # graph creation
    graph = networkx.Graph()
    graph.add_edges_from(edgeList)
    # Finding connected components from the graph
    components = list(networkx.connected_components(graph))
    print("connected component list: ")
    for component in components:
        print(component)



if __name__ == "__main__":
    erClustering()
    
    