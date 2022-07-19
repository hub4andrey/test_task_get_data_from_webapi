# 

## What is it?

This service is collecting up-to-date market data from api.meine-bank.ch and store it in DataWarehouse.

## Main Features

- fetches CSV file with asset prices and portfolio returns over the API from api.meine-bank.ch and reads it into a pandas DataFrame
- calculates the monthly portfolio return per customer ID
- yields a time series of the hourly average price per asset (of course, only the most up-to-date records should be taken into account)
- sends the result via email to some recipients (as plain text in the body, not as an attachment)
- prevents duplicates in DataWarehouse
- provides data validation

Below formual is used to `calculate the monthly portfolio return from the daily ones`:

$r_m$ = $\prod_t (1 + r_d)$ - 1

where:
- $r_m$ - monthly return
- $r_d$ - daily return

I.e. the monthly return is the product of (1 plus the daily returns of the month), minus 1.

---

## Installation from sources



Clone repository and prepare Python environment:

    git clone https://github.com/hub4andrey/test_task_get_data_from_webapi.git

    cd test_task_get_data_from_webapi

    pyenv local 3.9.12
    python -m venv ./.venv
    source .venv/bin/activate

    python -m pip install --upgrade pip
    pip install -r requirements.txt

In project root directory find and duplicate file `.env.secret_example` . Rename it to `.env.secret`. Modify the content.

Run PostgreSQL server in Docker container:

    docker-compose up

Run the server.

    cd src
    python -m main
