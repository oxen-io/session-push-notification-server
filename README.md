# Session Push Notification Server

## This is a python script for Session remote notification service

#### Use Python 3.7
#### To run the server:
Use `pip install -r requirements.txt` to install all the requirements first.


To start the server, use `python server.py`


The server is built with [Flask](https://github.com/pallets/flask) and [tornado](https://github.com/tornadoweb/tornado).  
The server uses APN for iOS push notifications, [PyAPNs2](https://github.com/Pr0Ger/PyAPNs2) to interact with APNs, and FCM for Android push notifications.

Right now the server only receives onion requests through the endpoint `/loki/v2/lsrpc` for
- `register`: register a device token associated with a session id
- `unregister`: unregister a device token from a session id's devices
- `subscribe_closed_group`: add a session id to a closed group as a member
- `unsubscribe_closed_group` remove a session id from a closed group members
- `notify`: send a message from remote notification

The new push notification server works this way:
- The client (Session Desktop or Mobile app) sends encrypted message data with the recipients' session id to server.
- The server checks the database to see if the recipients has registered their devices.
- The server generates and sends the push notification to the devices registered with their tokens.

### Statistics
There is a new endpoint for statistics data:  `/get_statistics_data`
- Method: **POST**
- Authorization: `[Authorization: Basic base64(username:password)]`
- Header: `[Content-Type: application/json]`
- Body: ( Note: All fields are optional )
```
  { 
    "start_date": "2021-5-4 03:40:00",
    "end_date": "2021-5-4 06:00:00",
    "ios_pn_number": 1,
    "android_pn_number": 1,
    "total_message_number": 1
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
