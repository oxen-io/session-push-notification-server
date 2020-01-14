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

#### APNs Provider Loop
Use [PyAPNs2](https://github.com/Pr0Ger/PyAPNs2) to interact with APNs.  

The loop will send requests to APNs every 1 - 3 minutes RANDOMLY to push silent notifications to all the devices that have registered their device tokens to our server.  
The device tokens are now stored in a local file called `token_db` as a json array.  

#### Notice
- Silent notifications are not guaranteed to be received by apple devices.
- To enable silent notifications, users should enable the ```Background App Refresh``` function for ```Loki Messenger``` on their apple devices.
- ~~To successfully receive silent notifications, ```Loki Messenger``` cannot be totally killed.~~ Actually we can receive silent notifications when the app is totally killed. 