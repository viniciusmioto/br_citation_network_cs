import re
import json
import requests
import pandas as pd
import concurrent.futures
from time import time

# Global session to reuse HTTP connections.
session = requests.Session()


# Load API token from file and set up HTTP headers.
def load_token(token_file: str = "../.config/token.json") -> dict:
    try:
        with open(token_file) as f:
            token_data = json.load(f)
            token = token_data.get("token")
            if token:
                print(f"[INFO] Loaded API token from {token_file}")
                return {"authorization": token}
            else:
                print(
                    f"[WARNING] No token found in {token_file}. Proceeding without authorization header."
                )
                return {}
    except Exception as e:
        print(f"[ERROR] Error loading token from {token_file}: {e}")
        return {}


HTTP_HEADERS = load_token()


def extract_doi(doi_str: str) -> str:
    """
    Extract a single DOI from a given string.

    Handles URLs (e.g. "https://doi.org/10.xxx/yyy") or "doi:" prefixes.
    """
    doi_str = doi_str.strip()
    if doi_str.lower().startswith("https://doi.org/"):
        return doi_str.split("https://doi.org/")[-1]
    match = re.search(r"doi:(\S+)", doi_str, re.IGNORECASE)
    if match:
        return match.group(1)
    return doi_str


def extract_dois(doi_field: str) -> list:
    """
    Extract all DOIs from a string containing one or more identifiers.
    Only tokens starting with "doi:" or "https://doi.org/" are considered.
    """
    tokens = doi_field.split()
    dois = []
    for token in tokens:
        token = token.strip()
        if token.lower().startswith("doi:"):
            dois.append(token.split("doi:")[-1].strip())
        elif token.lower().startswith("https://doi.org/"):
            dois.append(token.split("https://doi.org/")[-1].strip())
    return dois


def get_references(doi: str) -> list:
    """
    Retrieve outgoing reference edges for the given DOI via the OpenCitations API.

    Uses the "require=cited" filter.
    For each reference, the publication is the origin (which cites others)
    and each extracted DOI in the "cited" field becomes the target.
    """
    url = f"https://opencitations.net/index/api/v2/references/doi:{doi}?require=cited"
    print(f"[DEBUG] Fetching references for DOI {doi}\n URL: {url}")
    try:
        response = session.get(url, timeout=10, headers=HTTP_HEADERS)
        print(f"[DEBUG] Received status {response.status_code} for references of {doi}")
    except Exception as e:
        print(f"[ERROR] Request error (references) for {doi}: {e}")
        return []

    edges = []
    if response.status_code == 200:
        try:
            data = response.json()
        except Exception as e:
            print(f"[ERROR] JSON parse error for references of {doi}: {e}")
            return []
        # Each record might contain multiple DOIs in the "cited" field.
        for record in data:
            cited_raw = record.get("cited")
            if cited_raw:
                for candidate in extract_dois(cited_raw):
                    if candidate != doi:
                        edges.append(
                            {
                                "origin_doi": doi,
                                "target_doi": candidate,
                                "origin_sub_area": None,  # to be set later
                                "target_sub_area": None,
                            }
                        )
                    else:
                        print(f"[INFO] Skipping self-reference for {doi}")
    else:
        print(
            f"[ERROR] Error {response.status_code} when fetching references for {doi}"
        )
    return edges


def get_citations(doi: str) -> list:
    """
    Retrieve incoming citation edges for the given DOI via the OpenCitations API.

    Uses the "require=citing" filter.
    For each citation, the given DOI is the target and each extracted DOI from the "citing" field becomes the origin.
    """
    url = f"https://opencitations.net/index/api/v2/citations/doi:{doi}?require=citing"
    print(f"[DEBUG] Fetching citations for DOI {doi}\n URL: {url}")
    try:
        response = session.get(url, timeout=10, headers=HTTP_HEADERS)
        print(f"[DEBUG] Received status {response.status_code} for citations of {doi}")
    except Exception as e:
        print(f"[ERROR] Request error (citations) for {doi}: {e}")
        return []

    edges = []
    if response.status_code == 200:
        try:
            data = response.json()
        except Exception as e:
            print(f"[ERROR] JSON parse error for citations of {doi}: {e}")
            return []
        # Each record might contain multiple DOIs in the "citing" field.
        for record in data:
            citing_raw = record.get("citing")
            if citing_raw:
                for candidate in extract_dois(citing_raw):
                    if candidate != doi:
                        edges.append(
                            {
                                "origin_doi": candidate,
                                "target_doi": doi,
                                "origin_sub_area": None,
                                "target_sub_area": None,
                            }
                        )
                    else:
                        print(f"[INFO] Skipping self-citation for {doi}")
    else:
        print(f"[ERROR] Error {response.status_code} when fetching citations for {doi}")
    return edges


def process_doi(doi: str, sub_area: str) -> list:
    """
    Process a single publication by retrieving its references and citations.

    For references: sets the publication as origin.
    For citations: sets the publication as target.
    """
    print(f"[INFO] Processing DOI {doi} for sub-area {sub_area}")
    # Outgoing: publication is origin.
    refs = get_references(doi)
    for edge in refs:
        edge["origin_sub_area"] = sub_area  # known sub-area for the citing publication
        # target_sub_area remains None (will be filled later if available)
    # Incoming: publication is target.
    cits = get_citations(doi)
    for edge in cits:
        edge["target_sub_area"] = sub_area  # known sub-area for the cited publication
        # origin_sub_area remains None (will be filled later if available)
    total_edges = len(refs) + len(cits)
    print(
        f"[INFO] Completed DOI {doi}: {len(refs)} reference(s) and {len(cits)} citation(s) found (total {total_edges})"
    )
    return refs + cits


def load_publications_for_area(file_url: str, sub_area: str) -> list:
    """
    Load publication data from a CSV file at the given URL and tag each publication
    with the provided sub-area. Assumes the CSV file (without headers) has the DOI in column index 5.
    """
    print(f"[INFO] Loading publications for sub-area '{sub_area}' from {file_url}")
    try:
        df = pd.read_csv(file_url, header=None)
    except Exception as e:
        print(f"[ERROR] Error loading CSV from {file_url}: {e}")
        return []
    doi_series = df.iloc[:, 5].dropna().astype(str)
    publications = []
    for doi in doi_series:
        if doi.strip().lower() != "null":
            cleaned = extract_doi(doi)
            publications.append((cleaned, sub_area))
    unique_publications = list(set(publications))
    print(
        f"[INFO] Sub-area '{sub_area}': loaded {len(unique_publications)} unique publication(s)"
    )
    return unique_publications


def main():
    base_url = (
        "https://raw.githubusercontent.com/aserg-ufmg/CSIndex/refs/heads/master/data/"
    )
    areas = [
        "ai",
        "arch",
        "bio",
        "chi",
        "cse",
        "data",
        "dbis",
        "ds",
        "formal",
        "graphics",
        "hardware",
        "ir",
        "net",
        "or",
        "pl",
        "robotics",
        "se",
        "security",
        "theory",
        "vision",
    ]

    all_publications = []
    print("[START] Loading publication data for each area...")
    for area in areas:
        file_url = f"{base_url}{area}-out-papers.csv"
        pubs = load_publications_for_area(file_url, area)
        all_publications.extend(pubs)

    # Log total number of publications from GitHub repository.
    print(
        f"[SUMMARY] Total unique publications loaded from repository: {len(all_publications)}"
    )

    # Build a dictionary mapping each publication DOI to its sub-area.
    repo_dict = {doi: sub_area for doi, sub_area in all_publications}
    print(
        f"[INFO] Repository dictionary contains {len(repo_dict)} unique publication(s)"
    )

    all_edges = []
    start_time = time()

    print("[START] Processing citations and references concurrently...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_pub = {
            executor.submit(process_doi, doi, sub_area): (doi, sub_area)
            for doi, sub_area in all_publications
        }
        for future in concurrent.futures.as_completed(future_to_pub):
            doi, sub_area = future_to_pub[future]
            try:
                edges = future.result()
                all_edges.extend(edges)
            except Exception as e:
                print(f"[ERROR] Error processing {doi} ({sub_area}): {e}")

    elapsed = time() - start_time
    print(
        f"[SUMMARY] Processed {len(all_publications)} publication(s) in {elapsed:.2f} seconds."
    )
    print(
        f"[INFO] Total citation/reference edges collected (before filtering): {len(all_edges)}"
    )

    # Create final DataFrame with desired column order.
    df_edges = pd.DataFrame(all_edges)

    before_drop = len(df_edges)
    df_edges.drop_duplicates(inplace=True)
    print(f"[INFO] Dropped {before_drop - len(df_edges)} duplicate edge(s).")
    print(
        f"[SUMMARY] Unique citation/reference edges in the repository-only network: {len(df_edges)}"
    )

    output_file = "open_citations_edge_list.csv"
    df_edges.to_csv(output_file, index=False)
    print(f"[COMPLETE] Filtered citation edge list saved to '{output_file}'.")


if __name__ == "__main__":
    main()
