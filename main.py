"""
main.py - PM Intern Job Scraper

Pipeline:
1. Scrape jobs from LinkedIn via Apify
2. Clean and process data
3. Store in SQLite database
4. Generate Markdown report
"""

from dotenv import load_dotenv
load_dotenv()  # Load .env file for local development

from apify_client import ApifyClient
import json
from pathlib import Path

from config.settings import (
    APIFY_API_TOKEN,
    APIFY_ACTOR_ID,
    SEARCH_QUERIES,
    SEARCH_LOCATION,
    SEARCH_LIMIT,
)
from src.cleaner import clean_jobs, load_raw_data, save_cleaned_data
from src.database import (
    create_connection,
    create_tables,
    insert_jobs,
    get_jobs_per_country,
    get_jobs_per_company,
    get_remote_ratio,
)
from src.report_generator import generate_markdown_report


# ============================================================
# SCHRITT 1: DATEN SCRAPEN
# ============================================================

def scrape_jobs():
    """
    Scrape jobs from LinkedIn via Apify.

    Searches for all role queries defined in settings.py.

    Returns:
        List of job dictionaries
    """
    print("\n" + "="*50)
    print("[STEP 1] Scraping Jobs...")
    print("="*50)

    if not APIFY_API_TOKEN:
        raise ValueError("APIFY_API_TOKEN environment variable is not set!")

    client = ApifyClient(APIFY_API_TOKEN)

    # Build OR query from all search terms
    search_query = " OR ".join(SEARCH_QUERIES)
    print(f"[+] Searching for {len(SEARCH_QUERIES)} role types in {SEARCH_LOCATION}")

    run_input = {
        "keywords": search_query,
        "location": SEARCH_LOCATION,
        "date_posted": "month",
        "limit": SEARCH_LIMIT,
        "scrapeCompany": True,
    }

    print(f"[+] Starting Apify Actor...")
    run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)

    # Collect results
    jobs = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        jobs.append(item)

    print(f"[OK] {len(jobs)} Jobs scraped")

    # Save raw data as backup
    raw_path = "data/raw/jobs_raw.json"
    Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    print(f"[OK] Raw data saved: {raw_path}")

    return jobs


# ============================================================
# SCHRITT 2: DATEN CLEANEN
# ============================================================

def process_data(raw_data):
    """
    Rohdaten bereinigen.

    Args:
        raw_data: Liste von Job-Dicts (von scrape_jobs oder aus JSON)

    Returns:
        Bereinigter DataFrame
    """
    print("\n" + "="*50)
    print("[STEP 2] Cleaning Data...")
    print("="*50)

    df = clean_jobs(raw_data)

    # Bereinigten DataFrame als CSV speichern
    csv_path = "data/processed/jobs_clean.csv"
    save_cleaned_data(df, csv_path)

    return df


# ============================================================
# SCHRITT 3: IN DATENBANK SPEICHERN
# ============================================================

def store_in_database(df):
    """
    DataFrame in SQLite speichern.

    Args:
        df: Bereinigter DataFrame

    Returns:
        Datenbankverbindung (für weitere Queries)
    """
    print("\n" + "="*50)
    print("[STEP 3] Storing in Database...")
    print("="*50)

    conn = create_connection("database/jobs.db")
    create_tables(conn)
    insert_jobs(conn, df)

    return conn


# ============================================================
# SCHRITT 4: ANALYSE & REPORT
# ============================================================

def analyze_and_report(conn):
    """
    Run analysis queries and generate markdown report.

    Args:
        conn: Database connection
    """
    print("\n" + "="*50)
    print("[STEP 4] Analysis & Report Generation...")
    print("="*50)

    # Output folder
    Path("outputs").mkdir(parents=True, exist_ok=True)

    # Quick stats
    print("\n[+] Jobs per Country:")
    df_country = get_jobs_per_country(conn)
    print(df_country.to_string())

    print("\n[+] Top Hiring Companies:")
    df_companies = get_jobs_per_company(conn, limit=10)
    print(df_companies.to_string())

    print("\n[+] Remote vs On-site:")
    df_remote = get_remote_ratio(conn)
    print(df_remote.to_string())

    # Generate markdown report
    print("\n[+] Generating Markdown Report...")
    report_path = generate_markdown_report(
        conn,
        output_path="outputs/INTERNSHIP_OPPORTUNITIES.md"
    )
    print(f"[OK] Report created: {report_path}")


# ============================================================
# HAUPTPROGRAMM
# ============================================================

def main():
    """
    Komplette Pipeline ausführen.
    """
    print("\n" + "="*60)
    print("PM INTERN JOB SCRAPER - PIPELINE START")
    print("="*60)

    # Option A: Neu scrapen
    raw_data = scrape_jobs()

    # Option B: Aus gespeicherter JSON laden (für Tests)
    # raw_data = load_raw_data("data/raw/jobs_raw.json")

    # Option C: Mit Testdaten arbeiten (zum Üben)
    """raw_data = [
        {
            "job_id": "4200179442",
            "job_title": "Product Manager (m/f/d)",
            "company": "SIXT",
            "location": "Munich, Bavaria, Germany",
            "work_type": None,
            "posted_at": "2025-12-16 11:27:45",
            "job_insights": ["Full-time"],
            "applicant_count": 50,
            "job_url": "https://www.linkedin.com/jobs/view/4200179442"
        },
        {
            "job_id": "4200179443",
            "job_title": "Associate Product Manager",
            "company": "BMW",
            "location": "Berlin, Germany",
            "work_type": "Remote",
            "posted_at": "2025-12-15 09:00:00",
            "job_insights": ["Full-time"],
            "applicant_count": 120,
            "job_url": "https://www.linkedin.com/jobs/view/4200179443"
        },
        {
            "job_id": "4200179444",
            "job_title": "Junior Product Manager",
            "company": "Zalando",
            "location": "Berlin, Germany",
            "work_type": None,
            "posted_at": "2025-12-14 14:30:00",
            "job_insights": ["Full-time"],
            "applicant_count": 89,
            "job_url": "https://www.linkedin.com/jobs/view/4200179444"
        },
        {
            "job_id": "4200179445",
            "job_title": "Product Manager Intern",
            "company": "SAP",
            "location": "Walldorf, Baden-Württemberg, Germany",
            "work_type": None,
            "posted_at": "2025-12-13 10:15:00",
            "job_insights": ["Internship"],
            "applicant_count": 200,
            "job_url": "https://www.linkedin.com/jobs/view/4200179445"
        },
        {
            "job_id": "4200179446",
            "job_title": "Product Manager",
            "company": "Booking.com",
            "location": "Amsterdam, North Holland, Netherlands",
            "work_type": "Hybrid",
            "posted_at": "2025-12-12 08:00:00",
            "job_insights": ["Full-time"],
            "applicant_count": 75,
            "job_url": "https://www.linkedin.com/jobs/view/4200179446"
        }
    ]"""

    # Pipeline ausführen
    df = process_data(raw_data)
    conn = store_in_database(df)
    analyze_and_report(conn)

    # Verbindung schließen
    conn.close()

    print("\n" + "="*60)
    print("PIPELINE COMPLETE!")
    print("="*60)
    print("\nOutput: outputs/INTERNSHIP_OPPORTUNITIES.md")


if __name__ == "__main__":
    main()