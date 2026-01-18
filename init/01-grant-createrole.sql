-- Grant CREATEROLE privilege to dk400 user
-- This allows the application to create PostgreSQL roles for DK/400 users
ALTER ROLE dk400 WITH CREATEROLE;

-- Grant necessary privileges on public schema
GRANT ALL ON SCHEMA public TO dk400;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dk400;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dk400;

-- Future tables also get proper grants
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dk400;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO dk400;
