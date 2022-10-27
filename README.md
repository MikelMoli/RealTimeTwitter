## Running stream

In one console:

    python src/twitter_stream.py

In other console:

    python src/stream_consumer.py 


## Troubleshooting

### **Streamed Fields to null:**

If all fields of streamed data appears as null, it can be because there is something wrong with the data. When SparkStreaming doesn't understarnd a field it put every field to null. An example would be passing a None value instead of null, as Spark interprets it as a String.

### **Module not found inside UDF:**

- Set it for session in venv:
    
    With virtualenv activated enter next command.

        #! /bin/sh

        PYSPARK_PYTHON=./venv/bin/python

- Set it forever in venv:



        nano venv/bin/activate

    In the end of the file:

        export PYSPARK_PYTHON=./venv/bin/python
        export FLASK_APP=model/app.py
    
    https://stackoverflow.com/questions/9554087/setting-an-environment-variable-in-virtualenv 


Running flask:

    flask run --host=0.0.0.0