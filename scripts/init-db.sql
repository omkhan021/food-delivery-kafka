-- Run once against your existing local Postgres instance:
--   psql -h localhost -U postgres -f scripts/init-db.sql
--
-- Creates the shared `fooddelivery` database and one schema per microservice.
-- Each service's own Flyway migration (V1__init.sql inside that service) creates
-- its tables inside its schema the first time that service starts.

-- Conditionally create the database (CREATE DATABASE can't run inside a transaction
-- block or an IF NOT EXISTS clause, so we use psql's \gexec trick).
SELECT 'CREATE DATABASE fooddelivery'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'fooddelivery')\gexec

\c fooddelivery

CREATE SCHEMA IF NOT EXISTS order_service;
CREATE SCHEMA IF NOT EXISTS payment_service;
CREATE SCHEMA IF NOT EXISTS kitchen_service;
CREATE SCHEMA IF NOT EXISTS delivery_service;
CREATE SCHEMA IF NOT EXISTS notification_service;

-- If you connect as a non-superuser app role instead of `postgres`, grant it
-- rights on all 5 schemas (uncomment and set the role name):
-- GRANT ALL ON SCHEMA order_service, payment_service, kitchen_service, delivery_service, notification_service TO fooddelivery_app;

\echo 'fooddelivery database ready with 5 schemas: order_service, payment_service, kitchen_service, delivery_service, notification_service'
