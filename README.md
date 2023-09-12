# Session Push Notification Server

This is the Session Push Notification Server (SPNS) used by Session mobile clients to enable "fast
mode" phone notifications for new messages in 1-on-1 conversations and groups.

## Overview

The SPNS manages communication with three different parties:

- Android or iOS Session clients (who have enabled "Fast Mode" message notifications)
- The Oxen network's storage servers
- The relevant service (Apple, Google, Huawei) to which the requested notifications will be pushed

The Session clients (but only ones who have opted in to received messages) make an onion request to
the SPNS to subscribe to notifications; it provides the notification type (e.g. "apns" for iOS), a
upstream service-provided device ID, an encryption key, as well as a signed swarm subscription
signature that is stored by the SPNS to subscribe to updates in the user's message swarm for new
incoming messages.

The SPNS then maintains persistent connections to all of the network's service nodes and submits
this subscription signature to all of the nodes in a subscriber's swarm; the swarm members then push
requests to the SPNS server back along this open connection for the SPNS to deal with as soon as
they arrive.

When such a notification arrives the SPNS encrypts both the received message along with metadata
about the message (such as the message hash) using the encryption key the user provided, then sends
this encrypted payload to the provider's push notification service (APNS for iOS, Firebase for
Android, and so on), where the phone makers proprietary notification service allows waking the phone
up in a way that is impossible for third party apps.  This then wakes up Session to decrypt and
display the incoming message.

Throughout the process the data stays encrypted and metadata protected as much as possible: while
the SPNS itself has the keys to monitor public messages sent to your account, it does not have the
ability to read the data or do anything else in your account.  The service-specific token provided
to the SPNS server has no purpose at all other than delivering Session push notifications; and the
services themselves never see anything useful (they just see an encrypted blob that only your device
can decrypt).

## Architecture

SPNS is a mix of Python and C++ with a postgresql database for persistent storage.  The core that
manages connections to the swarm, processes messages, and interacts with the database is C++, with
some bits of Python for initialization and dealing with configuration files.  The web front-end
(which proxies subscriptions from clients to the core) and the code that communicates with the
notification services are written in Python.

These components run independently, but communicate together using OxenMQ: there is one main
high-performance process, called "hivemind", which provides an interface for notifier services and
the web service to connect to.  The hivemind takes care of all subscription logic, receives
notifications, performs encryption, and so on; once the final notification is ready to be passed off
to a notification service it then passes the message along with needed details (such as the unique
iOS or Android identifier) to an attached notifier for delivery.

## Requirements

To build the C++ code you must have the following libraries (and dev versions of the packages)
installed on the system:

- libsodium 1.0.18+
- oxen-encoding 1.0.4+
- oxen-mq 1.2.14+
- nlohmann-json 3.7.0+
- libsystemd
- libpq
- CMake 3.18+
- a C++ compiler capable of C++17

The two `oxen-` packages can either be built directly, or on Ubuntu/Debian systems can be obtained
from [our deb repository](https://deb.oxen.io) (the two required package names are `liboxenmq-dev`
and `liboxenc-dev`).

Some other libraries are also included as submodules, and will be built as part of the project.
Make sure you have updated the project submodules using

    git submodule update --init --recursive

To run the code (which all start via Python, even the C++ parts) you need a few more Python packages
to be installed:

- Python 3.8+
- python3-systemd
- python3-flask
- uwsgi-core
- uwsgi-plugin-python3
- uwsgi-emperor (not required, but nice especially if you have multiple uwsgi applications on the
  system)
- python3-uwsgidecorators
- python3-coloredlogs
- python3-oxenmq (available in our repository)
- python3-pyonionreq (available in our repository)

### Just give me some stuff to blindly copy and paste!

Okay here you go (for a recent Ubuntu or Debian installation):
```
    sudo curl -so /etc/apt/trusted.gpg.d/oxen.gpg https://deb.oxen.io/pub.gpg
    echo "deb https://deb.oxen.io $(lsb_release -sc) main" | sudo tee /etc/apt/sources.list.d/oxen.list
    sudo apt update
    sudo apt install cmake g++ lib{sodium,oxenmq,oxenc,systemd,pq}-dev nlohmann-json3-dev \
        python3 python3-{systemd,flask,uwsgidecorators,coloredlogs,oxenmq,pyonionreq} \
        uwsgi-plugin-python3 uwsgi-emperor
```


## Building the project

### Building the C++ code

The top-level project contains a simple `Makefile` will configure and build the C++ code when you
run `make`, e.g.

    make -j6

If you know a bit about cmake and want to poke around some more with the process you can build
yourself via:

    mkdir build
    cd build
    cmake ..
    make -j6

This will build the C++ code into a file such as `spns/core.cpython-311-x86_64-linux-gnu.so` (in the
project root, *not* the build directory) that contains all the C++ code, suitable built to be
invoked from Python.

If you upgrade packages on the system that break your installation, it may be necessary to repeat
these steps to rebuild against updated dependencies (you can reuse the same build directory and just
start from the `cmake ..` step).

### Configuration

A `spns.ini.example` file is included in the project root; you should copy this to `spns.ini` and
edit it to suite your requirements.  The settings inside it are full of comments to guide you
further in this initial setup, including details on how to create the necessary key for receiving
onion requests.

The notifiers require various magic special files obtained after pleading with the powers that be to
oh-please-let-us-into-your-blessed-walled-garden!  Here there be dragons because the business model
of Google and Apple is all about making things pretty and smooth for end-users but they really don't
put much effort into making things nice or convenient for devs.

So after much mucking around, perhaps involving paid accounts and whatnot, you can eventually coax
the systems into giving your the required magic tokens needed to connect with their servers.  If you
are unhappy with this situation, no doubt Google and Apple management will be receptive to your
suggestions as to how they might improve their systems.

## Running the server

There are two supported modes of operation: an "all-in-one" that runs all the components under
uwsgi; and alternatively a mode that uses uwsgi just for the web interface and uses systemd to
manage the other components.

The package is designed to run from the project directory, i.e. it there is no "install" step.

The systemd setup is recommended; it requires slightly more setup, but is generally more robust and
easier to manage.

### Via systemd

Copy the three files in the `systemd/` directory into `/etc/systemd/system`.  You will likely have
to modify the paths in these files to match where you have the project checked out.

To run the main hivemind service you then `systemctl enable --now spns-hivemind`.  (If you don't
want to make it persistently start after a reboot, use `systemctl start spns-hivemind` instead.)

The individual services are controlled by the `spns-notifier@.service` service template.  For
example, to start and persistently enable to firebase notifier (for Android push notifications) you
would use:

    systemctl enable --now spns-notifier@firebase.service

or for APNS:

    systemctl enable --now spns-notifier@apns.service

You can enable any number of services this way; each service corresponds to one of the notifier
scripts in the spns/notifiers/ directory (e.g. spns/notifiers/firebase.py).

For the web front-end, see below.

### Web interface

Whether you opt for systemd or uwsgi, you still require uwsgi to process HTTP subscription requests
(and proxy them into the hivemind process).

A sample uwsgi configuration file is provided as `uwsgi-spns.ini`.  This file will likely need some
path updates to function properly; then the file is used to configure uwsgi to service requests from
a spns.wsgi socket.

This socket is then mapped to requests from a nginx or Apache server for the domain on which the
push notification server runs.  For example, for nginx this configuration snippet will properly
forward the requests:

    server {
        listen 80;
        listen [::]:80;

        server_name push.example.org;

        location / {
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header Host $host;
            include /etc/nginx/uwsgi_params;
            uwsgi_pass unix:///home/push/session-push-notification-server/spns.wsgi;
        }
    }

### UWSGI mules

As an alternate mode of operation you can configure UWSGI to run all of the components of the SPNS,
each as a "mule" process that uwsgi runs alongside your regular uwsgi process for serving HTTP
requests.

This is easier for a one-shot setup, but, for our official server, was more difficult to manage and
diagnose versus using separate processes.  Nevertheless, the support still exists.

To enable it, uncomment the lines in the bottom half of the `uwsgi-spns.ini` configuration file; you
will need one mule for the hivemind process, and one additional mule for each notification service
that you want to run.

## Checking the logs

Logging depends on how you run; for systemd, use `journalctl` (for instance, to view the live logs
for the spns-hivemind service: `journalctl -u spns-hivemind -af`).  For uwsgi, you configure a log
file in the uwsgi-spns.ini configuration file where all logging ends up for all the processes.

## Session client API

See [DOCUMENTATION.md](DOCUMENTATION.md).

## Creating a new notification backend

SPNS is deliberately designed to be extensible to new backends: a backend is written in Python and
only needs to worry about checking the validity of tokens (when subscribing) and pushing out
notifications (when so instructed).

See the (live) versions in the spns/notifiers/ directory for examples.

## Older versions

This is the second version of the SPNS, which rewrote the project and how it worked considerably;
this version launched in September 2023.  Earlier versions are preserved in the project history
under the `v1` branch.  A working prototype of the v2 approach written entirely in Python (i.e.
without the C++ core) is deliberately preserved in the v2 history, though was never used in
production for performance reasons.
