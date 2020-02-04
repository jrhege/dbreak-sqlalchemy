# dbreak-sqlalchemy
A plugin for [dbreak](https://github.com/jrhege/dbreak) that allows it to work with SQLAlchemy engine objects.

## Installation
Install from PyPi using pip:

```
pip install dbreak-sqlalchemy
```

## Usage
There's no need to import the plugin separately, just pass a SQLAlchemy Engine object to dbreak.show_console().

```
import sqlalchemy
import dbreak

# Set up a SQLite SQLAlchemy connection
connection = sqlalchemy.create_engine("sqlite://")

# Pause execution and enter the console
dbreak.start_console(connection)
```