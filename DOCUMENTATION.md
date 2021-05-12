# Documentation

### Lsprc (`/loki/v2/lsrpc`)
- Endpoint: `/register`
- Method: **POST**
- Header: `[Content-Type: application/json]`
- Expected Body:
```
    {
        "token": String
        "pubKey": String
    }
```

- Endpoint: `/unregister`
- Method: **POST**
- Header: `[Content-Type: application/json]`
- Expected Body:
```
    {
        "token": String
    }
```

- Endpoint: `/subscribe_closed_group`
- Method: **POST**
- Header: `[Content-Type: application/json]`
- Expected Body:
```
    {
        "closedGroupPublicKey": String
        "pubKey": String
    }
```

- Endpoint: `/unsubscribe_closed_group`
- Method: **POST**
- Header: `[Content-Type: application/json]`
- Expected Body:
```
    {
        "closedGroupPublicKey": String
        "pubKey": String
    }
```

- Endpoint: `/notify`
- Method: **POST**
- Header: `[Content-Type: application/json]`
- Expected Body:
```
    {
        "data": String
        "send_to": String
    }
```

- Response:
```
    {
        "status": Number,
        "body": {
            "code": 0 or 1
            "message": "Success" or Error Message String
        }
    }
```

### Statistics
- Endpoint:  `/get_statistics_data`
- Method: **POST**
- Authorization: `[Authorization: Basic base64(username:password)]`
- Header: `[Content-Type: application/json]`
- Expected Body: ( Note: All fields are optional )
```
  { 
    "start_date": Date String formated "%Y-%m-%d %H:%M:%S" or "%Y-%m-%d",
    "end_date": Date String formated "%Y-%m-%d %H:%M:%S" or "%Y-%m-%d",
    "ios_pn_number": Boolean,
    "android_pn_number": Boolean,
    "closed_group_message_number": Boolean
    "total_message_number": Boolean
  }
  ```
- Response:
```
{
    "code": 0,
    "data": [
        {
            "start_date": Date String formated "%Y-%m-%d %H:%M:%S",
            "end_date": Date String formated "%Y-%m-%d %H:%M:%S",
            "ios_pn_number": Number,
            "android_pn_number": Number,
            "closed_group_message_number": Number,
            "total_message_number": Number
        },
    ]
}
```
