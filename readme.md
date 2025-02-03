to run the server 
functions-framework --target hello --debug

curl localhost:8080
http://localhost:8080/


# for-developers-gcp-cloud-function-python
Python Cloud Function 
This function is using Flask and Function Framework for testing.

Prerequisite:
- install Function framework
```
pip install functions-framework
```

To start cloud function locally:
```
functions-framework --target func_file_extraction --host=127.0.0.1 --port=8081 --debug
functions-framework --target func_er_with_blocking --host=127.0.0.1 --port=8082 --debug
functions-framework --target func_er_with_brute_force --host=127.0.0.1 --port=8083 --debug
functions-framework --target func_calculate_quality_measures --host=127.0.0.1 --port=8084 --debug
functions-framework --target func_er_clustering --host=127.0.0.1 --port=8085 --debug


functions-framework --target func_name --host=127.0.0.1 --port=8084 --debug

```
To execute the function open http://localhost:8080/ in your browser and see Hello world!
If you want to provide a parameter then use http://localhost:8080/?message=HelloYou

For all the details see https://cloud.google.com/functions/docs/running/function-frameworks
