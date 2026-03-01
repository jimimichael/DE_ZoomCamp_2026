import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb

    con = duckdb.connect("taxi_pipeline.duckdb")

    return (con,)


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(con):
    con.execute("DESCRIBE taxi_pipeline_dataset.trips").df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Q1 – Start and end date of the dataset
    """)
    return


@app.cell
def _(con):
    con.execute("""
        SELECT
            MIN(trip_pickup_date_time)  AS start_date,
            MAX(trip_dropoff_date_time) AS end_date
        FROM taxi_pipeline_dataset.trips
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Q2 – Proportion of trips paid with credit card
    """)
    return


@app.cell
def _(con):
    con.execute("""
        SELECT
            CAST(SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) AS DOUBLE)
            / COUNT(*) AS proportion_credit
        FROM taxi_pipeline_dataset.trips
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Q3 – Total tips
    """)
    return


@app.cell
def _(con):
    con.execute("""
        SELECT SUM(tip_amt) AS total_tips
        FROM taxi_pipeline_dataset.trips
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Homework answers

    1. Date range: 2009‑06‑01 to 2009‑07‑01
    2. Proportion credit card: 26.66%
    3. Total tip amount: $6,063.41
    """)
    return


if __name__ == "__main__":
    app.run()
