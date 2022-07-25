# InYoVa web scrapper and DataWarehouse

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

    cd your_project_dir
    git clone https://github.com/hub4andrey/test_task_get_data_from_webapi.git

In project root directory find and duplicate file `.env.secret_example` . Rename it to `.env.secret`. Modify the content:

    cd your_project_dir
    cd test_task_get_data_from_webapi/apps/backendapp1
    vim .env.secret_example
    mv .env.secret_example .env.secret

Run Docker containers:

    cd your_project_dir
    cd test_task_get_data_from_webapi
    docker-compose -f docker-compose.yml up

## How to configure tasks schedule

### Either before run containers:

    cd your_project_dir
    cd test_task_get_data_from_webapi/apps/backendapp1/cron
    # modify schedule:
    vim crontab_user.cron

Then run Docker containers (see above).

### OR when container is running:

    # get into container:
    docker exec -ti inyova_webscrapper bash
    cd cron
    # modify schedule:
    vim crontab_user.cron
    # apply cron changes:
    crontab crontab_user.cron
    service cron reload

## How to run task individually

How to:

    # get into container:
    docker exec -ti inyova_webscrapper bash
    cd src
    # get help:
    python -m main --help
    # run tast (see --run section):
    python -m main --run get_portfolio_return get_asset_plublication email_portfolio_monthly_return

## How to run unit tests

How to:

    # get into container:
    docker exec -ti inyova_webscrapper bash
    cd src
    # get help:
    python -m unittest

enjoy.
