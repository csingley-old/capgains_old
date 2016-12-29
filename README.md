# `capgains`

## Description

`capgains` is a Python library for computing taxable capital gains from a series
of securities transactions, by parsing Open Financial Exchange (OFX)
data - both OFXv1 (SGML) and OFXv2 (pure XML) - which is the standard format
for downloading financial information from banks and stockbrokers.

Securities position information can be loaded from CSV files, and reports on
positions or capital gains for various time periods can be exported as
CSV files so they can be manipulated with 3rd party spreadsheet software.

Gains match lots for purchases/sales, in order to accurately calculate the
amount, timing, and character (long term vs. short term) of realized gains.

There is also basic handling of wash sales per the US tax code (i.e. disallowing
loss on wash sales and rolling into basis of replacement shares).


`capgains` depends on:
* [the ofxtools package](https://github.com/csingley/ofxtools).
* [the SQLAlchemy package](http://www.sqlalchemy.org).  You'll need SQLAlchemy version 1.0 or higher.


# Installation

Use the Python user installation scheme:

    python setup.py install --user

In addition to the Python package, this will also install a script `capgains`
in `~/.local/bin`.



# Basic Usage

To get started, you'll need a CSV file containing initial positions.  The
first row of the CSV file should consist of the following headers:

  * 'brokerid': broker ID; from OFX INVACCTFROM aggregate
  * 'acctid': account number; from OFX INVACCTFROM aggregate
  * 'ticker': security trading symbol; from OFX SECINFO aggregate
  * 'secname': Security name; from OFX SECINFO aggregate
  * 'uniqueidtype': usually 'CUSIP' or 'ISIN'; from OFX SECINFO aggregate
  * 'uniqueid': CUSIP/ISIN for the security; from OFX SECINFO aggregate
  * 'dtopen': date/time the lot was opened, in ISO 8601 format
  * 'units'
  * 'cost'

It's not possible to read this data from an OFX statement response, since
OFX INVPOS aggregates don't record cost or opening date information, which
is critical in computing gains.  Some brokers will generate a position
report in CSV format that you can mangle into the above format.  You may
also be able to copy/paste this information from your broker's website;
detailed breakdowns of individual lots are commonly available.  Of course, 
you can also manually type in rows for each lot.  You'll need to open an OFX
statement response downloaded from your brokerage in order to find the
`brokerid` and CUSIP data.

To roll this initial position data forward, modifying lots and generating
gains, you'll need OFX data downloads from your broker.

Use the `capgains` script with appropriate arguments.  See the `--help`
for explanation of the script options.  The database schema should be in
sqlalchemy format.  For example:

  ```
  export DB='sqlite:///test.db'
  capgains -d $DB load positions_20051231.csv # Load initial positions from CSV
  capgains -d $DB import ~/ofx_downloads/*.ofx # Parse OFX transactions
  capgains -d $DB calc # Compute realized gains; process wash sales
  capgains -d $DB gains -s 2016-01-01 -e 2016-12-31 gains_2016.csv
  # The above reports capital gains activity for all of 2016,
  # and writes the report as a CSV file at gains_2016.csv
  ```

## Contributing

If you want to contribute with this project, create a virtualenv and install
all development requirements:

    virtualenv .venv
    source .venv/bin/activate
    pip install -r requirements-development.txt


Then, run the tests with `make`:

    make test

Or directly with `nosetests`:

    nosetests -dsv --with-yanc --with-coverage --cover-package capgains

Feel free to [create pull
requests](https://help.github.com/articles/using-pull-requests/) on [capgains
repository on GitHub](https://github.com/csingley/capgains).
