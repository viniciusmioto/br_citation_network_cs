import time
import json
import logging
import pandas as pd
from math import ceil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

# Configure logging to write to debug.log with detailed formatting.
logging.basicConfig(
    filename="debug.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Set up the Selenium Chrome driver (update the chromedriver path as needed)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")  # run Chrome in headless mode
service = Service()  # update this path
driver = webdriver.Chrome(service=service, options=chrome_options)


def fetch_api_data_with_selenium(url):
    """
    Fetch data from the API using Selenium with Chrome.
    Navigates to the given URL, waits for the page to load,
    and extracts the JSON from the page's body.
    """
    logging.debug("Fetching API data from URL: %s", url)
    try:
        driver.get(url)
        # Wait for the page to load; adjust the sleep time if necessary.
        time.sleep(2)
        body_text = driver.find_element(By.TAG_NAME, "body").text
        data = json.loads(body_text)
        logging.debug("Successfully fetched and parsed API data.")
        return data
    except Exception as e:
        logging.exception("Error fetching API data from %s: %s", url, e)
        raise


def parse_results(json_data):
    """
    Parses the JSON data to extract specific fields for each work.
    Returns a list of dictionaries containing selected fields.
    """
    logging.debug("Parsing JSON results.")
    results = []
    works = json_data.get("results", []) if isinstance(json_data, dict) else []

    for work in works:
        # Remove the prefix from the id and doi strings
        work_id = work.get("id", "").removeprefix("https://openalex.org/")
        doi = (work.get("doi") or "").removeprefix("https://doi.org/")
        title = work.get("title", "")
        primary_topic = work.get("primary_topic", {})
        subfield = primary_topic.get("subfield", {})
        subfield_display_name = subfield.get("display_name", "")
        referenced_works_count = work.get("referenced_works_count", 0)
        referenced_works = work.get("referenced_works", [])
        cited_by_api_url = work.get("cited_by_api_url", "")

        results.append(
            {
                "id": work_id,
                "doi": doi,
                "title": title,
                "subfield_display_name": subfield_display_name,
                "referenced_works_count": referenced_works_count,
                "referenced_works": referenced_works,
                "cited_by_api_url": cited_by_api_url,
            }
        )
    logging.debug("Parsed %s records from JSON data.", len(results))
    return results


def fetch_all_data(base_url, per_page=10):
    """
    Fetches all data from the API using Selenium.
    First, the first page is fetched to determine the total count of records.
    Then, subsequent pages are fetched sequentially.

    Returns a combined list of all parsed work records.
    """
    logging.debug("Starting to fetch all data from the API using Selenium.")
    all_results = []

    # Fetch the first page
    first_page_url = f"{base_url}&page=1&per_page={per_page}"
    first_page_data = fetch_api_data_with_selenium(first_page_url)
    first_page_results = parse_results(first_page_data)
    all_results.extend(first_page_results)

    # Determine total pages from the meta information
    total_count = first_page_data.get("meta", {}).get("count", 0)
    total_pages = ceil(total_count / per_page) if total_count else 1
    logging.debug("Total count: %s, Total pages: %s", total_count, total_pages)

    # Fetch remaining pages sequentially
    for page in range(2, total_pages + 1):
        page_url = f"{base_url}&page={page}&per_page={per_page}"
        logging.debug("Fetching page %s: %s", page, page_url)
        page_data = fetch_api_data_with_selenium(page_url)
        page_results = parse_results(page_data)
        logging.debug("Fetched %s records from page %s.", len(page_results), page)
        all_results.extend(page_results)

    logging.debug("Completed fetching all data. Total records: %s", len(all_results))
    return all_results


# Main execution block
if __name__ == "__main__":
    logging.debug("Program started.")
    try:
        # Define the API endpoint URL (without the page and per_page parameters)
        api_url = (
            "https://api.openalex.org/works?"
            "select=id,doi,title,publication_year,primary_topic,"
            "referenced_works_count,referenced_works,cited_by_api_url&"
            "filter=authorships.countries:countries/br,publication_year:2024,"
            "primary_topic.field.id:fields/17,primary_topic.subfield.id:subfields/1704&"
            "sort=publication_year:desc"
        )
        logging.debug("Full API URL: %s", api_url)

        # Fetch all data (this returns a list of parsed records)
        all_results = fetch_all_data(api_url, per_page=10)
        df = pd.DataFrame(all_results)
        logging.debug("Data loaded into DataFrame with %s records.", len(df))

        df.to_csv("openalex_data.csv", index=False)

        # Optionally, save the DataFrame to a CSV file:
        # df.to_csv("openalex_data.csv", index=False)
        logging.debug("Program completed successfully.")
    except Exception as e:
        logging.exception("An error occurred during execution: %s", e)
        raise
    finally:
        driver.quit()
