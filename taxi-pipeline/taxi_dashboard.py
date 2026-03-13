import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import duckdb
    import pandas as pd
    import altair as alt

    con = duckdb.connect("taxi_pipeline.duckdb")
    mo.md("# NYC Taxi Trip Data — Homework Dashboard")


@app.cell
def __(con, mo):
    date_df = con.execute("""
        SELECT
            MIN(trip_pickup_date_time) AS start_date,
            MAX(trip_pickup_date_time) AS end_date
        FROM nyc_taxi_data.trips
    """).df()

    start = date_df["start_date"][0].strftime("%Y-%m-%d")
    end = date_df["end_date"][0].strftime("%Y-%m-%d")

    mo.md(f"""
    ## Q1: Dataset Date Range
    | | Date |
    |---|---|
    | **Start** | {start} |
    | **End** | {end} |

    **Answer: 2009-06-01 to 2009-07-01**
    """)


@app.cell
def __(alt, con, mo):
    pay_df = con.execute("""
        SELECT
            CASE
                WHEN LOWER(payment_type) = 'credit' THEN 'Credit'
                WHEN LOWER(payment_type) = 'cash'   THEN 'Cash'
                ELSE payment_type
            END AS payment_type,
            COUNT(*) AS trips,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
        FROM nyc_taxi_data.trips
        GROUP BY payment_type
        ORDER BY trips DESC
    """).df()

    chart = alt.Chart(pay_df).mark_arc(innerRadius=60).encode(
        theta=alt.Theta("trips:Q"),
        color=alt.Color("payment_type:N", legend=alt.Legend(title="Payment Type")),
        tooltip=["payment_type:N", "trips:Q", "pct:Q"],
    ).properties(
        title="Q2: Trip Payment Type Distribution",
        width=350,
        height=350,
    )

    credit_pct = pay_df[pay_df["payment_type"] == "Credit"]["pct"].values[0]

    mo.vstack([
        mo.ui.altair_chart(chart),
        mo.md(f"**Credit card proportion: {credit_pct}% → Answer: 26.66%**"),
    ])


@app.cell
def __(alt, con, mo):
    tips_df = con.execute("""
        SELECT
            CASE
                WHEN LOWER(payment_type) = 'credit' THEN 'Credit'
                WHEN LOWER(payment_type) = 'cash'   THEN 'Cash'
                ELSE payment_type
            END AS payment_type,
            ROUND(SUM(tip_amt), 2) AS total_tips
        FROM nyc_taxi_data.trips
        GROUP BY payment_type
        ORDER BY total_tips DESC
    """).df()

    total = tips_df["total_tips"].sum()

    bar = alt.Chart(tips_df).mark_bar().encode(
        x=alt.X("payment_type:N", title="Payment Type"),
        y=alt.Y("total_tips:Q", title="Total Tips ($)"),
        color=alt.Color("payment_type:N", legend=None),
        tooltip=["payment_type:N", "total_tips:Q"],
    ).properties(
        title="Q3: Total Tips by Payment Type",
        width=400,
        height=300,
    )

    mo.vstack([
        mo.ui.altair_chart(bar),
        mo.md(f"**Total tips across all trips: ${total:,.2f} → Answer: $6,063.41**"),
    ])


if __name__ == "__main__":
    app.run()
