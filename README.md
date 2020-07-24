# loki-messenger-APNs-provider

## This is a python script for loki messenger APN service

#### Use Python 3.7
#### To run this script:
To install all the requirements, use  
`pip install -r requirements.txt`   

  
To start the server, use  
`python server.py`


#### RESTful API:
The server is build with [Flask](https://github.com/pallets/flask) and [gevent](https://github.com/gevent/gevent).  

To send a device token to the server:  
- Method: **POST**
- URL: ```/register```
- Headers: ```["Content-Type": "application/json"]```
- Body: 
```
{
    "token": "XXXXXXXXXXXXXXXXXXX(device token)"
}
```

If the request is successful, you will get a response like this  
```
{
    "code": 1, 
    "message": "Success"
}
```

If the request fails, you will get something like  
```
{
    "code": 0, 
    "message": "Missing parameter"
}
```

#### Push Notification Service Provider
Use APN for iOS push notifications

Use [PyAPNs2](https://github.com/Pr0Ger/PyAPNs2) to interact with APNs.

Use FCM for Android push notifications
