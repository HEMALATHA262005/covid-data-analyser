import pandas as pd
import sqlite3
import os

# ── Create report folder ───────────────────────────────
os.makedirs("report", exist_ok=True)

# ── Block 1: Load dataset ──────────────────────────────
print("Loading dataset...")
df = pd.read_csv("covid_data.csv")

print("\n===== DATASET OVERVIEW =====")
print(f"  Total rows    : {len(df)}")
print(f"  Total columns : {len(df.columns)}")

# ── Block 2: Clean & Filter ────────────────────────────
print("\n===== CLEANING DATA =====")

df = df[df["iso_code"].str.len() == 3]

cols = ["location", "date", "total_cases", "new_cases",
        "total_deaths", "new_deaths", "people_vaccinated",
        "population"]
df = df[cols]

df = df.dropna(subset=["location", "date"])

df[["total_cases", "new_cases", "total_deaths",
    "new_deaths", "people_vaccinated"]] = df[[
    "total_cases", "new_cases", "total_deaths",
    "new_deaths", "people_vaccinated"]].fillna(0)

df["date"] = pd.to_datetime(df["date"])

df["death_rate"] = (
    (df["total_deaths"] / df["total_cases"]) * 100
).round(2).fillna(0)

print(f"  Cleaned rows  : {len(df)}")
print(f"  Columns kept  : {list(df.columns)}")
print("\n✓ Data cleaned successfully!")

# ── Block 3: Store into SQLite ─────────────────────────
print("\n===== STORING INTO DATABASE =====")

conn = sqlite3.connect("covid_database.db")
cursor = conn.cursor()

df.to_sql("covid_data", conn, if_exists="replace", index=False)

print(f"  Records stored : {len(df)}")
print("✓ Database created: covid_database.db")

cursor.execute("SELECT COUNT(*) FROM covid_data")
count = cursor.fetchone()[0]
print(f"  Verified rows in DB : {count}")

# ── Block 4: SQL Queries & Analysis ───────────────────
print("\n===== COVID-19 ANALYSIS REPORT =====")

print("\n📊 TOP 10 COUNTRIES BY TOTAL CASES")
query1 = """
    SELECT location,
           MAX(total_cases) AS total_cases,
           MAX(total_deaths) AS total_deaths,
           ROUND(MAX(death_rate), 2) AS death_rate_percent
    FROM covid_data
    GROUP BY location
    ORDER BY total_cases DESC
    LIMIT 10
"""
df_q1 = pd.read_sql_query(query1, conn)
print(df_q1.to_string(index=False))

print("\n💀 TOP 10 COUNTRIES BY DEATH RATE %")
query2 = """
    SELECT location,
           MAX(total_cases) AS total_cases,
           MAX(total_deaths) AS total_deaths,
           ROUND(MAX(death_rate), 2) AS death_rate_percent
    FROM covid_data
    WHERE total_cases > 10000
    GROUP BY location
    ORDER BY death_rate_percent DESC
    LIMIT 10
"""
df_q2 = pd.read_sql_query(query2, conn)
print(df_q2.to_string(index=False))

print("\n💉 TOP 10 MOST VACCINATED COUNTRIES")
query3 = """
    SELECT location,
           MAX(people_vaccinated) AS people_vaccinated,
           MAX(population) AS population,
           ROUND(MAX(people_vaccinated) * 100.0
                 / MAX(population), 2) AS vaccinated_percent
    FROM covid_data
    WHERE population > 0
    GROUP BY location
    ORDER BY vaccinated_percent DESC
    LIMIT 10
"""
df_q3 = pd.read_sql_query(query3, conn)
print(df_q3.to_string(index=False))

print("\n📅 YEARLY GLOBAL CASE TREND")
query4 = """
    SELECT SUBSTR(date, 1, 4) AS year,
           ROUND(SUM(new_cases)) AS total_new_cases,
           ROUND(SUM(new_deaths)) AS total_new_deaths
    FROM covid_data
    GROUP BY year
    ORDER BY year
"""
df_q4 = pd.read_sql_query(query4, conn)
print(df_q4.to_string(index=False))

print("\n🇮🇳 INDIA STATISTICS")
query5 = """
    SELECT location,
           MAX(total_cases)       AS total_cases,
           MAX(total_deaths)      AS total_deaths,
           MAX(people_vaccinated) AS people_vaccinated,
           ROUND(MAX(death_rate), 2) AS death_rate_percent
    FROM covid_data
    WHERE location = 'India'
"""
df_q5 = pd.read_sql_query(query5, conn)
print(df_q5.to_string(index=False))
# ── Block 5: Export Report to CSV ─────────────────────
print("\n===== EXPORTING REPORTS =====")

df_q1.to_csv("report/top10_cases.csv", index=False)
print("✓ Saved: report/top10_cases.csv")

df_q2.to_csv("report/top10_death_rate.csv", index=False)
print("✓ Saved: report/top10_death_rate.csv")

df_q3.to_csv("report/top10_vaccinated.csv", index=False)
print("✓ Saved: report/top10_vaccinated.csv")

df_q4.to_csv("report/yearly_trend.csv", index=False)
print("✓ Saved: report/yearly_trend.csv")

df_q5.to_csv("report/india_stats.csv", index=False)
print("✓ Saved: report/india_stats.csv")

# Close database connection
conn.close()

print("\n✓ Database connection closed")
print("\n===== PROJECT COMPLETE =====")
print("  Files saved in /report folder")
print("  Database: covid_database.db")
print("\n🎉 COVID-19 Data Analyser built successfully!")