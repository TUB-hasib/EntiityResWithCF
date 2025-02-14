import flask
import functions_framework
import requests
from extract import extract
from erBruteForce import er as er_brute_froce
from erWithBlocking import er as er_with_blocking
# from spark
from calculatingQualityMeasures import calculateQualityMeasures
from erClustering import erClustering

@functions_framework.http
def func_file_extraction_pattern(request: flask.Request) -> flask.typing.ResponseReturnValue:
    
    printMultiline("func_file_extraction", "Started")
    try:
        extract()
        printMultiline("func_file_extraction", "Completed")
    except Exception as e:
        return {"status": "error", "message": f"Error in function Extract: {str(e)}"}, 500

    # Make an HTTP request to er_with_blocking
    response = requests.get('http://localhost:8082/func_er_with_blocking')
    print(response.text)
    if response.status_code != 200:
        return {"status": "error", "message": f"Failed to call func_er_with_blocking: {response.text}"}, response.status_code

    # Make an HTTP request to er_with_bruce_force
    response = requests.get('http://localhost:8083/func_er_with_brute_force')
    print(response.text)
    if response.status_code != 200:
        return {"status": "error", "message": f"Failed to call func_er_with_brute_force : {response.text}"}, response.status_code


    # Make an HTTP request to calculate_quality_measures
    response = requests.get('http://localhost:8084/func_calculate_quality_measures')
    print(response.text)
    if response.status_code != 200:
        return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code


    # Make an HTTP request to er_clustering
    response = requests.get('http://localhost:8085/func_er_clustering')
    print(response.text)
    if response.status_code != 200:
        return {"status": "error", "message": f"Failed to call func_er_clustering : {response.text}"}, response.status_code


    return {"status": "success", "message": "pipeline Completed successfully"}, 200
            


@functions_framework.http
def func_file_extraction(request: flask.Request) -> flask.typing.ResponseReturnValue:

    printMultiline("func_file_extraction", "Started")
    try:
        extract()
        printMultiline("func_file_extraction", "Completed")
        return {"status": "success", "message": "func_file_extraction Completed successfully"}, 200
    except Exception as e:
        return {"status": "error", "message": f"Error in function Extract: {str(e)}"}, 500

    



@functions_framework.http
def func_er_with_blocking(request: flask.Request) -> flask.typing.ResponseReturnValue:

    printMultiline("func_er_with_blocking", "Started")
    try:
        er_with_blocking()
        printMultiline("func_er_with_blocking", "Completed")
        return {"status": "success", "message": "func_er_with_blocking Completed successfully"}, 200
    except Exception as e:
        return {"status": "error", "message": f"Error: {str(e)}"}, 500
    




@functions_framework.http
def func_er_with_brute_force(request: flask.Request) -> flask.typing.ResponseReturnValue:

    printMultiline("func_er_with_brute_force", "Started")
    try:
        # er_brute_froce()
        printMultiline("func_er_with_brute_force", "Completed")
        return {"status": "success", "message": "func_er_with_brute_force Completed successfully"}, 200
    except Exception as e:
        return {"status": "error", "message": f"Error: {str(e)}"}, 500
    


@functions_framework.http
def func_calculate_quality_measures(request: flask.Request) -> flask.typing.ResponseReturnValue:

    printMultiline("func_calculate_quality_measures", "Started")
    try:
        calculateQualityMeasures()
        printMultiline("func_calculate_quality_measures", "Completed")
        return {"status": "success", "message": "func_calculate_quality_measures Completed successfully"}, 200
    except Exception as e:
        return {"status": "error", "message": f"Error: {str(e)}"}, 500
    


@functions_framework.http
def func_er_clustering(request: flask.Request) -> flask.typing.ResponseReturnValue:
    
    printMultiline("func_er_clustering", "Started")
    try:
        erClustering()
        printMultiline("func_er_clustering", "Completed")
        return {"status": "success", "message": "func_er_clustering Completed successfully"}, 200
    except Exception as e:
        return {"status": "error", "message": f"Error: {str(e)}"}, 500
    
    
@functions_framework.http
def func_spark(request: flask.Request) -> flask.typing.ResponseReturnValue:
    return "Hello world!"

@functions_framework.http
def func_test(request: flask.Request) -> flask.typing.ResponseReturnValue:
    return "Hello world!"


def printMultiline(fun_name, status):
    
    print(f"""
    **
    **
    ****************** {fun_name} ******************
    ****************** {status} ******************
    **
    **   
    """)