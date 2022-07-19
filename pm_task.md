# Original task. Data Engineer Technical Assignment

Data Engineering Architectures and Workflows

## Business story. Introduction

SuperStonk corp. is an online asset manager. Through their app, they offer 10’000s different assets and process 100’000s transactions per month. The app has a PostgreSQL database as a backend containing the `assets catalog`, `price`, `currencies`, and all the `stock transactions`. The whole application is deployed in the cloud.

In order to get more data-driven and increase performance, the company wants to get up-to-date stock prices from an `external provider`. The data *analytics team* mainly uses downloads the files containing `price data` and `portfolio return data` (in *CSV format*) from the website of an external source, processes them in *Jupyter notebooks*, and `uploads` the results into the data warehouse `every 15 minutes by a routine`.

---

## Asset prices data description

The table has the following schema:

| Column name | Data type | Description |
| --- | --- | --- |
| datetime | DATETIME | date and time of the price |
| asset_id | INT64 | numerical asset identifier |
| price | FLOAT64 | asset price |
| insertion_timestamp | TIMESTAMP | time when the record is inserted into the table |

Sometimes, the data provider publishes a NULL value or a wrong price. In both cases, the value is rectified and re-published by the data provider later. In this case, the new, updated value will be appended to the table as a new record. I.e. the existing, outdated record will not be updated with the new value nor deleted.

E.g. the table could look like this:

    datetime asset_id price insertion_timestamp
    2022-02-23T12:45:15 19846 1.2 2022-02-23T12:45:22 UTC
    2022-02-23T12:45:15 76156 5.4 2022-02-23T12:45:22 UTC
    2022-02-23T12:45:30 19846 NULL 2022-02-23T12:45:34 UTC <= red
    2022-02-23T12:45:30 76156 5.3 2022-02-23T12:45:34 UTC
    2022-02-23T12:45:30 19846 1.05 2022-02-23T12:45:48 UTC <= green
    2022-02-23T12:45:45 19846 1.05 2022-02-23T12:45:48 UTC
    2022-02-23T12:45:45 76156 5.45 2022-02-23T12:45:48 UTC <=red
    2022-02-23T12:46:00 19846 0.98 2022-02-23T12:46:03 UTC
    2022-02-23T12:46:00 76156 5.51 2022-02-23T12:46:03 UTC
    2022-02-23T12:45:45 76156 5.43 2022-02-23T12:46:03 UTC <= green
    ...

(Highlighted in red are the records with the original values, in green the updated readings.)

`Write a query that yields a time series of the hourly average price per asset (of course, only the most up-to-date records should be taken into account)`.

## API. Automation

Now the external data provider announced it will **soon** provide an `API` for `downloading the data files`.

The data analytics department wants to automate the work process as soon as possible and, therefore, asks you to start working on a solution.

`Write a Python script that`:

- `fetches the file over the API and reads it into a pandas DataFrame`,
- `calculates the monthly portfolio return per customer ID`,
- `sends the result via email to some recipients (as plain text in the body, not as an attachment)`.

The `API specs` are as follows:

- HTTP GET request to https://api.meine-bank.ch/v1/accounts
- HTTP basic authentication with:
  - user ID user_name@mail.com
  - password some_pass
- Request header: Accept: text/csv

The `CSV file contains daily data` and `one record per customer and (business) day`. It has the `following structure`:

    date customer_id portfolio_return
    2022-01-10 1245 -0.0365
    2022-01-10 5458 0.001256
    2022-01-11 1245 0.00024
    2022-01-11 5458 -0.01756
    2022-01-12 1245 0.01036
    ...

---

## E-mail report

The `output from monthly portfolio return calculation is sent via email`. It should have the `following structure`:

    month customer_id monthly_return
    2022-01 1245 0.04236
    2022-01 5458 0.05647
    2022-02 1245 0.00024
    ...

---

## Calculate the monthly portfolio return from the daily ones

To `calculate the monthly portfolio return from the daily ones`, you can use the following formula:

$r_m$ = $\prod_t (1 + r_d)$ - 1

[see latex formulas](https://www.overleaf.com/learn/latex/Subscripts_and_superscripts)

where:
- $r_m$ - monthly return
- $r_d$ - daily return

I.e. the monthly return is the product of (1 plus the daily returns of the month), minus 1.

---

## Exception handling. Python script reliability

Your script will be deployed as an automated service and not run in Jupyter anymore. Therefore, also think about 

- `exception handling`
- how you could `increase the reliability of your script` (you don’t have to implement this, some comments in the code on what/how you would do this are sufficient).

---

## Unit testing

Since the API is not released yet, you can’t test your script end-to-end. However, you still want to make sure that your code is working properly.

Write `unit tests` to ensure your code produces the expected results. The `tests should cover` all parts of the code:

- data acquisition
- calculations in pandas
- email generation
