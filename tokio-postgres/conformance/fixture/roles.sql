-- Synthetic roles and extensions for the upstream tokio-postgres tests.
-- Adapts the SQL block from upstream docker/sql_setup.sh at
-- f1cb6ec0d5766b136cbd68f3010d64142a5daa66. Passwords are fixed fixture
-- values for a throwaway loopback server, not credentials for any real system.
-- Explicit MD5 encryption changes the upstream fixture to exercise MD5 on PG17.
CREATE ROLE pass_user PASSWORD 'password' LOGIN;
SET password_encryption TO 'md5';
CREATE ROLE md5_user PASSWORD 'password' LOGIN;
SET password_encryption TO 'scram-sha-256';
CREATE ROLE scram_user PASSWORD 'password' LOGIN;
CREATE ROLE ssl_user LOGIN;
CREATE EXTENSION hstore;
CREATE EXTENSION citext;
CREATE EXTENSION ltree;
