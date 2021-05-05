# Session Push Notification Server

## This is a python script for loki messenger APN service

#### Use Python 3.7
#### To run the server:
Use `pip install -r requirements.txt` to install all the requirements first.


To start the server, use `python server.py`


The server is built with [Flask](https://github.com/pallets/flask) and [tornado](https://github.com/tornadoweb/tornado).  
The server uses APN for iOS push notifications, [PyAPNs2](https://github.com/Pr0Ger/PyAPNs2) to interact with APNs, and FCM for Android push notifications.

Right now the server only receives onion requests through the endpoint `/loki/v2/lsrpc`.

The new push notification server works this way:
- The client (Session Desktop or Mobile app) sends encrypted message data with the recipients' session id to server.
- The server checks the database to see if the recipients has registered their devices.
- The server generates and sends the push notification to the devices registered with their tokens.

### Statistics
There is a new endpoint for statistics data:  `/get_statistics_data`
- Method: **POST**
- Authorization: ```Basic base64(username:password)```
- Body: 
```
  { 
    "start_date": "2021-5-4 03:40:00" (optional),
    "end_date": "2021-5-4 06:00:00" (optional)
  }
  ```
- Response:
```
{
    "code": 0,
    "data": [
        {
            "android_pn_number": 0,
            "end_date": "2021-05-04 03:41:47",
            "ios_pn_number": 0,
            "start_date": "2021-05-04 03:40:47",
            "total_message_number": 0
        },
        {
            "android_pn_number": 0,
            "end_date": "2021-05-04 03:42:47",
            "ios_pn_number": 0,
            "start_date": "2021-05-04 03:41:47",
            "total_message_number": 0
        }
    ]
}
```

