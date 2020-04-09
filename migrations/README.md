# Database migrations

To create a database migration

- `./manage.py db migrate -m 'description of db change'`

It is important to know that alembic has a few limitations so migrations always need to be tested
before hand and possibly amended in particular in the case of renaming columns.

https://alembic.sqlalchemy.org/en/latest/autogenerate.html#what-does-autogenerate-detect-and-what-does-it-not-detect


To run database migrations

- `./manage.py db upgrade` or `./manage.py dev db --run_migrations`


If your database is up-to-date already and you'd like to fake the migrations

- `./manage.py db stamp head`
