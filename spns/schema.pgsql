
CREATE TABLE subscriptions (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    account BYTEA NOT NULL,       -- 33-byte swarm account id (e.g. session id or closed group id)
    session_ed25519 BYTEA,        -- for a 05 account (session id) this is the 32-byte ed25519 key
    subaccount_tag BYTEA,         -- optional subaccount tag for subaccount authentication
    subaccount_sig BYTEA,         -- optional subaccount tag signature for subaccount authentication
    signature BYTEA NOT NULL,     -- subscription authentication signature (for swarm)
    signature_ts BIGINT NOT NULL, -- unix timestamp of the auth signature (for swarm)
    want_data BOOLEAN NOT NULL,   -- whether the client wants msg data included in the notification
    enc_key BYTEA NOT NULL,       -- encryption key with which we encrypt the payload
    service VARCHAR NOT NULL,     -- subscription service type ("apns", "firebase")
    svcid VARCHAR NOT NULL,       -- unique device/app identifier
    svcdata BYTEA,                -- arbitrary data for subscription service

    UNIQUE(account, service, svcid)
);

CREATE INDEX subscriptions_signature_ts_idx ON subscriptions(signature_ts);

CREATE INDEX subscriptions_service_idx ON subscriptions(service);

CREATE TABLE sub_namespaces (
    subscription BIGINT NOT NULL REFERENCES subscriptions ON DELETE CASCADE,
    namespace SMALLINT NOT NULL,

    PRIMARY KEY(subscription, namespace)
);

CREATE TABLE service_stats (
    service VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    val_str VARCHAR,
    val_int BIGINT,

    PRIMARY KEY(service, name),
    CHECK((val_str IS NULL AND val_int IS NOT NULL) OR (val_str IS NOT NULL AND val_int IS NULL))
);

-- vim:ft=sql
