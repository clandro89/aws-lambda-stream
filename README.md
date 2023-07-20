# aws-lambda-stream

This a python version of [aws-lambda-stream](https://github.com/jgilbert01/aws-lambda-stream) using [ReactiveX](https://github.com/ReactiveX/RxPY)


## Installation
With `pip` installed, run:

````
pip install aws-lambda-stream
````

With `poetry`, run:
````
poetry add aws-lambda-stream
````

## Project Templates
The following project templates are provided to help get your event platform up and running:

* event hub
* event-lake-s3
* event-fault-monitor
* rest-bff-service
* control-service


Create your own project using the Serverless Framework, such as: `sls create --template-url https://github.com/clandro89/aws-lambda-stream/tree/master/templates/event-hub --path myprefix-event-hub`


## Credits
- [aws-lambda-stream](https://github.com/jgilbert01/aws-lambda-stream)

## License
This library is licensed under the MIT License. See the LICENSE file.