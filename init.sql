-- Runs once when the Postgres container is first created.
-- Tables themselves are also created via SQLAlchemy on API startup,
-- but seeding demo accounts here makes the API immediately testable.

-- (Tables are created by app/database.py::init_db() on API startup.
--  This file just seeds demo data after that happens, so we guard with
--  a DO block that no-ops if tables don't exist yet on very first boot.
--  Simplest fix: just re-run `python -m app.seed` after `docker compose up`.)
