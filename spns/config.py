import configparser
import os
import re
import logging
import coloredlogs
from nacl.public import PrivateKey
from spns.core import Config, logger as core_logger
import oxenmq

logger = logging.getLogger("spns")

# Set up colored logging; we come back to set the level once we know it
coloredlogs.install(milliseconds=True, isatty=True, logger=logger)

# Global config; we set values in here, then pass it to HiveMind during startup.
config = Config()

# Keypairs; the "hivemind" key in here gets set in `config` for the main hivemind instance;
# "onionreq" is the main onionreq keypair; other keys can be set as well (e.g. for notifiers).
PRIVKEYS = {}
PUBKEYS = {}

# We stash anything in a `[notify-xyz]` into `NOTIFY['xyz']` for notifiers to piggyback on the
# config.
NOTIFY = {}

# Will be true if we're running as a uwsgi app, false otherwise; used where we need to do things
# only in one case or another (e.g. database initialization only via app mode).
RUNNING_AS_APP = False
try:
    import uwsgi  # noqa: F401

    RUNNING_AS_APP = True
except ImportError:
    pass


truthy = ("y", "yes", "Y", "Yes", "true", "True", "on", "On", "1")
falsey = ("n", "no", "N", "No", "false", "False", "off", "Off", "0")
booly = truthy + falsey


def looks_true(val):
    """Returns True if val is a common true value, False if a common false value, None if neither."""
    if val in truthy:
        return True
    if val in falsey:
        return False
    return None


def load_config():
    if "SPNS_CONFIG" in os.environ:
        conf_ini = os.environ["SPNS_CONFIG"]
        if conf_ini and not os.path.exists(conf_ini):
            raise RuntimeError(f"SPNS_CONFIG={conf_ini} specified, but path does not exist!")
    else:
        conf_ini = "spns.ini"
        if not os.path.exists(conf_ini):
            raise RuntimeError(
                "spns.ini does not exist; either create it or use SPNS_CONFIG=... to specify an"
                " alternate config file"
            )

    if not conf_ini:
        return

    logger.info(f"Loading config from {conf_ini}")
    cp = configparser.ConfigParser()
    cp.read(conf_ini)

    # Set log level up first (we'll set it again below, mainly to log it if we have debug logging
    # enabled).
    if "log" in cp.sections() and "level" in cp["log"]:
        coloredlogs.install(level=cp["log"]["level"], logger=logger)

    def path_exists(path):
        return not path or os.path.exists(path)

    def val_or_none(v):
        return v or None

    def days_to_seconds(v):
        return float(v) * 86400.0

    def days_to_seconds_or_none(v):
        return days_to_seconds(v) if v else None

    def set_of_strs(v):
        return {s for s in re.split("[,\\s]+", v) if s != ""}

    def bool_opt(name):
        return (name, lambda x: x in booly, lambda x: x in truthy)

    # Map of: section => { param => ('config_property', test lambda, value lambda) }
    # global is the string name of the global variable to set
    # test lambda returns True/False for validation (if None/omitted, accept anything)
    # value lambda extracts the value (if None/omitted use str value as-is)
    setting_map = {
        "db": {"url": ("pg_connect", lambda x: x.startswith("postgresql"))},
        # 'keys': ... special handling ...
        "hivemind": {
            "subs_interval": ("subs_interval", None, int),
            "max_connects": ("max_pending_connects", None, int),
            "filter_lifetime": ("filter_lifetime", None, int),
            "startup_wait": ("notifier_wait", None, lambda x: round(1000 * float(x))),
            "listen": ("hivemind_sock", lambda x: re.search("^(?:tcp|ipc)://.", x)),
            "listen_curve": ("hivemind_curve", lambda x: re.search("^tcp://.", x)),
            "listen_curve_admin": (
                "hivemind_curve_admin",
                lambda x: re.search("^(?:[a-fA-F0-9]{64}\s+)*[a-fA-F0-9]{64}\s*$", x),
                lambda x: set(bytes.fromhex(y) for y in x.split() if y),
            ),
            "oxend_rpc": (
                "oxend_rpc",
                lambda x: re.search("^(?:tcp|ipc|curve)://.", x),
                lambda x: oxenmq.Address(x),
            ),
        },
    }

    def parse_option(fields, s, opt):
        if opt not in fields:
            logger.warning(f"Ignoring unknown config setting [{s}].{opt} in {conf_ini}")
            return
        conf = fields[opt]
        value = cp[s][opt]

        assert isinstance(conf, tuple) and 1 <= len(conf) <= 3
        global config
        assert hasattr(config, conf[0])

        if len(conf) >= 2 and conf[1]:
            if not conf[1](value):
                raise RuntimeError(f"Invalid value [{s}].{opt}={value} in {conf_ini}")

        if len(conf) >= 3 and conf[2]:
            value = conf[2](value)

        logger.debug(f"Set config.{conf[0]} = {value}")
        setattr(config, conf[0], value)

    for s in cp.sections():
        if s == "keys":
            for opt in cp["keys"]:
                filename = cp["keys"][opt]
                with open(filename, "rb") as f:
                    keybytes = f.read()
                    if len(keybytes) == 32:
                        privkey = PrivateKey(keybytes)
                    else:
                        # Assume hex-encoded
                        keyhex = keybytes.decode().strip()
                        if len(keyhex) != 64:
                            raise RuntimeError(
                                f"Could not read '{filename}' for option [keys]{opt}: invalid file size"
                            )
                        if any(x not in "0123456789abcdefABCDEF" for x in keyhex):
                            raise RuntimeError(
                                f"Could not read '{filename}' for option [keys]{opt}: expected bytes or hex"
                            )

                        privkey = PrivateKey(bytes.fromhex(keyhex))
                    PRIVKEYS[opt] = privkey
                    PUBKEYS[opt] = privkey.public_key

                    logger.info(
                        f"Loaded {opt} X25519 keypair with pubkey {PUBKEYS[opt].encode().hex()}"
                    )
        elif s == "log":
            for opt in cp["log"]:
                if opt == "level":
                    core_logger.set_level(cp["log"][opt])

        elif s.startswith("notify-"):
            for opt in cp[s]:
                NOTIFY.setdefault(s[7:], {})[opt] = cp[s][opt]

        elif s in setting_map:
            for opt in cp[s]:
                parse_option(setting_map[s], s, opt)

        else:
            logger.warning(f"Ignoring unknown section [{s}] in {conf_ini}")

    config.privkey = PRIVKEYS["hivemind"].encode()
    config.pubkey = PUBKEYS["hivemind"].encode()


try:
    load_config()
except Exception as e:
    logger.critical(f"Failed to load config: {e}")
    raise
