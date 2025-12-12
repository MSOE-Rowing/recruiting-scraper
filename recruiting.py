"""Rowing Athlete Recruitment Data Scraper

This module scrapes regatta results from RegattaCentral.com to gather data about rowers and their
performances. It fetches event results, parses athlete lineups, and aggregates race information
to create a CSV file suitable for recruitment analysis.

The scraper:
1. Fetches a main results page containing links to individual events
2. For each event, retrieves JSON data with race results
3. Fetches athlete lineups (in parallel) for each boat in the races
4. Aggregates race data by athlete name
5. Exports flattened athlete data to CSV format

Typical usage:
    python recruiting.py

This will generate an 'athletes.csv' file containing all scraped athlete race data.
"""

import cloudscraper
import re
import json
from collections import defaultdict
from bs4 import BeautifulSoup
import concurrent.futures
import csv
import time
from nameparser import HumanName

# Create a cloudscraper instance to bypass Cloudflare protection
scraper = cloudscraper.create_scraper()

base_url = "https://www.regattacentral.com"
main_results_url = "https://www.regattacentral.com/regatta/results2?job_id=9168"


def normalize_name(name):
    """Normalize an athlete's name using HumanName parser.
    
    Parses a name string into components and extracts only the first and last name.
    This allows matching the same person across multiple entries where their name
    might be formatted differently (e.g., with initials, middle names, suffixes, etc.).
    
    The normalized form is "FirstName LastName" which is used as the aggregation key.
    This ensures that "John Smith", "J. Smith", and "John Q. Smith" all map to the
    same athlete record.
    
    Args:
        name (str): Raw name string from the scraper (e.g., "John Q. Smith", "J Smith")
        
    Returns:
        str: Normalized name in "FirstName LastName" format for consistent grouping.
             Falls back to original string if parsing fails.
    """
    try:
        parsed = HumanName(name.strip())
        # Extract only first and last name, ignore middle names/initials/suffixes
        first = parsed.first.strip()
        last = parsed.last.strip()
        if first and last:
            return f"{first} {last}"
        elif first:
            return first
        elif last:
            return last
        else:
            return name.strip()
    except Exception:
        # Fallback to simple strip if parsing fails
        return name.strip()


def get_regatta_metadata(html):
    """Extract regatta metadata from the main results page HTML.
    
    Parses the regatta header section to extract information about the regatta
    including name, dates, venue, location, host, and statistics.
    
    Args:
        html (str): HTML content of the main results page
        
    Returns:
        dict: Dictionary containing regatta metadata:
              - name (str): Regatta name
              - start_date (str): Start date of the regatta
              - end_date (str): End date of the regatta
              - race_type (str): Type of race (e.g., "sprint")
              - venue (str): Venue name
              - location (str): City, state, country
              - host (str): Hosting organization
              - sanctioned (bool): Whether it's a USRowing sanctioned regatta
              - entries (str): Number of entries
              - clubs (str): Number of participating clubs
    """
    soup = BeautifulSoup(html, "html.parser")
    metadata = {
        "name": "",
        "start_date": "",
        "end_date": "",
        "race_type": "",
        "venue": "",
        "location": "",
        "host": "",
        "sanctioned": False,
        "entries": "",
        "clubs": "",
    }
    
    # Find the regatta header
    header = soup.select_one(".rc-regatta-header")
    if not header:
        return metadata
    
    # Get regatta name from h2
    name_el = header.select_one("h2[itemprop='name']")
    if name_el:
        metadata["name"] = name_el.get_text(strip=True)
    
    # Get dates
    start_date = header.select_one("span[itemprop='startDate']")
    if start_date:
        metadata["start_date"] = start_date.get_text(strip=True)
    
    end_date = header.select_one("span[itemprop='endDate']")
    if end_date:
        metadata["end_date"] = end_date.get_text(strip=True)
    
    # Get location
    location_el = header.select_one("li[itemprop='location']")
    if location_el:
        metadata["location"] = location_el.get_text(strip=True)
    
    # Get venue (from the link to venue page)
    venue_link = header.select_one("a[href*='/venues/venue.jsp']")
    if venue_link:
        metadata["venue"] = venue_link.get_text(strip=True)
    
    # Parse the details lists
    details = header.select(".rc-regatta-details li")
    for li in details:
        text = li.get_text(strip=True)
        # Check for race type (sprint, head, etc.)
        if text.lower() in ["sprint", "head", "dual"]:
            metadata["race_type"] = text.lower()
    
    # Get host from the second details list
    details2 = header.select(".rc-regatta-details-2 li")
    for li in details2:
        text = li.get_text(strip=True)
        if text.startswith("Hosted By:"):
            metadata["host"] = text.replace("Hosted By:", "").strip()
        if "USRowing Sanctioned" in text:
            metadata["sanctioned"] = True
    
    # Get stats (entries and clubs)
    stats = header.select(".rc-regatta-stat")
    for stat in stats:
        value_el = stat.select_one("span[itemprop='value']")
        h4_el = stat.select_one("h4")
        if value_el and h4_el:
            label = h4_el.get_text(strip=True).lower()
            value = value_el.get_text(strip=True)
            if label == "entries":
                metadata["entries"] = value
            elif label == "clubs":
                metadata["clubs"] = value
    
    return metadata



def get_event_links(html):
    """Extract event result links from the main results page HTML.
    
    Parses the HTML to find all anchor tags that link to individual event results,
    converts relative URLs to absolute URLs.
    
    Args:
        html (str): HTML content of the main results page
        
    Returns:
        list: Absolute URLs to individual event result pages
    """
    soup = BeautifulSoup(html, "html.parser")
    event_links = []
    for a in soup.select('a[href^="/regatta/results2/eventResults.jsp"]'):
        href = a.get("href")
        if href.startswith("/"):
            href = base_url + href
        event_links.append(href)
    return event_links


def fetch_event_results_json(job_id, event_id):
    """Fetch JSON race results for a specific event from RegattaCentral.
    
    Makes an HTTP request to the DisplayRacesResults servlet with the specified
    job and event IDs. Includes error handling for network failures.
    
    Args:
        job_id (str): The RegattaCentral job ID for the event
        event_id (str): The RegattaCentral event ID
        
    Returns:
        str: JSON string containing race results, or None if the request fails
    """
    url = f"https://www.regattacentral.com/servlet/DisplayRacesResults?Method=getResults&job_id={job_id}&event_id={event_id}"
    try:
        resp = scraper.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"Error fetching event results for job_id={job_id}, event_id={event_id}: {e}")
    return None


def fetch_lineup(job_id, boat_id):
    """Fetch and parse the athlete lineup for a specific boat.
    
    Retrieves the HTML lineup from RegattaCentral's LineupServlet and parses it
    to extract athlete information (name, seat, age, club).
    
    Args:
        job_id (str): The RegattaCentral job ID
        boat_id (str): The boat ID for which to fetch the lineup
        
    Returns:
        list: List of athlete dictionaries with keys: seat, name, age, club.
              Empty list if the request fails.
    """
    url = f"https://www.regattacentral.com/servlet/LineupServlet?Method=getLineupHtml&job_id={job_id}&boat_id={boat_id}"
    try:
        resp = scraper.get(url, timeout=10)
        if resp.status_code == 200:
            return parse_lineup_html(resp.text)
    except Exception as e:
        print(f"Error fetching lineup for job_id={job_id}, boat_id={boat_id}: {e}")
    return []


def parse_lineup_html(html):
    """
    Desc: 
        Parse HTML lineup data to extract athlete information.
        Uses regex to extract athlete details from the LineupServlet HTML response.
        Expected format: "seat: name - age (club)"
    Args:
        html (str): HTML content from the LineupServlet response
    Returns:
        list: List of dictionaries containing athlete information:
              - seat (str): The rower's seat position in the boat
              - name (str): Athlete's name
              - age (str): Athlete's age
              - club (str): Club/organization name
    """
    soup = BeautifulSoup(html, "html.parser")
    athletes = []
    for line in soup.get_text(separator="\n").splitlines():
        # Pattern: "1: John Doe - 18 (Rowing Club)"
        m = re.match(r"(\d+):\s*(.+?)\s*-\s*(\d+)\s*\((.+?)\)", line.strip())
        if m:
            seat, name, age, club = m.groups()
            athletes.append({
                "seat": seat,
                "name": name.strip(),
                "age": age,
                "club": club.strip(),
            })
    return athletes

def parse_event_results_json(json_str, job_id=None, event_name=None):
    """Parse JSON race results and associate athletes with race finishes.
    
    Processes event results JSON from RegattaCentral, extracts race information,
    fetches athlete lineups in parallel, and combines them into result records.
    Handles missing data gracefully by trying multiple field names for the same data.
    
    Args:
        json_str (str): JSON string containing race results from RegattaCentral
        job_id (str, optional): The job ID (used to fetch lineups)
        event_name (str, optional): Event name to use in results. If not provided,
                                    extracted from the JSON data.
        
    Returns:
        list: List of race result dictionaries with keys:
              - event (str): Event name
              - race (str): Race name (e.g., 'Heat 1', 'Final')
              - place (str): Finishing place
              - bow (str): Boat lane/bow position
              - club (str): Boat club/organization
              - athletes (list): Athletes in the boat (with seat, name, age, club)
              - finish (str): Finish time
              - margin (str): Margin to next boat
              - num_boats (int): Number of boats in the race
    """
    data = json.loads(json_str)
    if not event_name:
        event_name = data.get("long_desc") or data.get("event_label") or ""
    results = []
    races = data.get("races", [])

    # Collect all unique (job_id, boat_id) pairs for lineup fetching
    boat_ids = set()
    race_rows = []
    for race in races:
        # Try multiple field names to find race name (API schema may vary)
        race_name = (
            race.get("stageName", "")
            or race.get("displayNumber", "")
            or race.get("raceName", "")
            or "Final"
        )

        num_boats = 0
        rows = []
        # Process each boat's result in this race
        for result in race.get("results", []):
            boat_id = str(result.get("boatId")) if result.get("boatId") else None
            boat_label = result.get("boatLabel") or ""
            club_name = result.get("orgName") or result.get("longName") or ""
            # Try multiple field names for finishing place
            place = (
                result.get("place")
                or result.get("orderOfFinishPlace")
                or result.get("finishPlace")
                or result.get("officialPlace")
                or ""
            )
            if place != "": num_boats += 1

            bow = result.get("lane", "")
            # Try multiple field names for finish time
            finish = (
                result.get("finishTimeString")
                or result.get("adjustedTimeString")
                or result.get("rawTimeString")
                or result.get("officialTimeString")
                or ""
            )
            # Try multiple field names for time margin
            margin = (
                result.get("marginString")
                or result.get("adjustedTimeDeltaString")
                or result.get("officialMarginString")
                or ""
            )
            rows.append({
                "boat_id": boat_id,
                "boat_label": boat_label,
                "club_name": club_name,
                "place": place,
                "bow": bow,
                "finish": finish,
                "margin": margin,
                "race_name": race_name,
            })
            if boat_id and job_id:
                boat_ids.add((job_id, boat_id))

        for row in rows: row["num_boats"] = num_boats
        race_rows.extend(rows)

    # Fetch all lineups in parallel for efficiency (8 concurrent requests)
    boat_lineups = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Create futures for each unique boat
        future_to_boat = {
            executor.submit(fetch_lineup, job_id, boat_id): (job_id, boat_id)
            for (job_id, boat_id) in boat_ids
        }
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_boat):
            _, boat_id_b = future_to_boat[future]
            try:
                boat_lineups[boat_id_b] = future.result()
            except Exception:
                boat_lineups[boat_id_b] = []

    # Build complete result records by matching race data with athlete lineups
    for info in race_rows:
        boat_id = info["boat_id"]
        club_name = info["club_name"]
        athletes = []
        if boat_id and boat_id in boat_lineups and boat_lineups[boat_id]:
            # Filter athletes by club match, as a boat may compete for multiple clubs
            for a in boat_lineups[boat_id]:
                if club_name and a["club"] and club_name.lower() in a["club"].lower():
                    athletes.append(a)
                elif not club_name:
                    athletes.append(a)
            # If no club match found, use all athletes from the boat
            if not athletes:
                athletes = boat_lineups[boat_id]
        elif info["boat_label"]:
            # Fallback: create a pseudo-athlete entry from the boat label if no lineup available
            athletes.append({
                "name": info["boat_label"],
                "seat": None,
                "club": club_name,
            })
        # Skip results with no athlete information
        if not athletes:
            continue
        results.append({
            "event": event_name,
            "race": info["race_name"],
            "place": info["place"],
            "bow": info["bow"],
            "club": club_name,
            "athletes": athletes,
            "finish": info["finish"],
            "margin": info["margin"],
            "num_boats": info["num_boats"],
        })
    return results


def aggregate_athletes(event_results):
    """Aggregate race results by athlete name using intelligent name matching.
    
    Processes a list of race results and groups them by athlete using name normalization.
    This intelligently matches the same person across multiple entries even if their name
    is formatted differently (e.g., with initials, middle names, or suffixes). Only the
    first and last name are used for matching and output.
    
    Skips entries without finish times or with place code 999 (DNS/DNF - Did Not Start/Finish).
    
    Args:
        event_results (list): List of race result dictionaries from parse_event_results_json()
        
    Returns:
        dict: Dictionary mapping normalized athlete names (FirstName LastName) to aggregated data:
              - races (list): List of race results for this athlete
              - clubs (set): Set of clubs the athlete competed for
              - names (set): Set of name variations seen for this athlete (for reference)
              - age (str): The athlete's age
    """
    athletes = defaultdict(
        lambda: {
            "races": [],
            "clubs": set(),
            "names": set(),
        }
    )
    for result in event_results:
        # Skip results without finish times or with place code 999 (DNS/DNF)
        if not result.get("finish") or str(result.get("place")).strip() == "999":
            continue
        # Track which names we've already processed in this result to avoid duplicates
        seen_names = set()
        for athlete in result["athletes"]:
            # Normalize the athlete's name for consistent grouping
            key = normalize_name(athlete["name"])
            # Create a unique identifier for this race to detect exact duplicates
            race_id = (
                result["event"],
                result["race"],
                result["place"],
                result["bow"],
                result["club"],
                result["finish"],
            )
            if (key, race_id) in seen_names:
                continue
            seen_names.add((key, race_id))
            athletes[key]["names"].add(athlete["name"])
            athletes[key]["clubs"].add(athlete.get("club", ""))
            athletes[key]["age"] = athlete.get("age", "")
            athletes[key]["races"].append(
                {
                    "event": result["event"],
                    "race": result["race"],
                    "place": result["place"],
                    "bow": result["bow"],
                    "club": result["club"],
                    "finish": result["finish"],
                    "margin": result["margin"],
                    "seat": athlete.get("seat", ""),
                    "num_boats": result["num_boats"],
                }
            )
    return athletes


def write_athletes_to_csv(athletes, filename="athletes.csv"):
    """Write aggregated athlete data to a CSV file.
    
    Flattens the nested athlete/race data structure into a single CSV where each row
    represents one race participation. This format is suitable for analysis and filtering.
    
    Args:
        athletes (dict): Aggregated athlete data from aggregate_athletes()
        filename (str, optional): Output CSV file path. Defaults to "athletes.csv"
    """
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        fieldnames = [
            "athlete_name", "age", "club", "seat", "event", "race", "place", "bow", "finish", "margin", "num_boats"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        # Flatten: one row per race participation per athlete
        for name, info in athletes.items():
            for race in info["races"]:
                writer.writerow({
                    "athlete_name": name,
                    "age": info.get("age", ""),
                    "club": ", ".join(info["clubs"]),
                    "seat": race["seat"],
                    "event": race["event"],
                    "race": race["race"],
                    "place": race["place"],
                    "bow": race["bow"],
                    "finish": race["finish"],
                    "margin": race["margin"],
                    "num_boats": race["num_boats"],
                })


def scrape_athletes_from_url(main_url):
    """Scrape and aggregate athlete data from a RegattaCentral main results URL.
    
    Fetches all event results from a main results page and aggregates the race data
    by athlete name. Also extracts regatta metadata from the page header.
    
    Args:
        main_url (str): URL to the main results page (e.g., a RegattaCentral regatta results page)
        
    Returns:
        tuple: (athletes, regatta_metadata) where:
            - athletes (dict): Dictionary mapping athlete names to aggregated data:
                - races (list): List of race results for this athlete
                - clubs (set): Set of clubs the athlete competed for
                - names (set): Set of name variations for this athlete
                - age (str): The athlete's age
            - regatta_metadata (dict): Dictionary containing regatta information:
                - name, start_date, end_date, race_type, venue, location, host, sanctioned, entries, clubs
    """
    # Fetch the main results page and extract event links
    html = scraper.get(main_url).text
    event_links = get_event_links(html)
    
    # Extract regatta metadata from the page
    regatta_metadata = get_regatta_metadata(html)

    # Fetch and parse each event's JSON results
    # Sequential processing to avoid overwhelming the server
    all_event_results = []
    for link in event_links:
        # Extract job_id and event_id from the URL
        m = re.search(r'job_id=(\d+)&?.*&event_id=(\d+)', link)
        if not m:
            continue
        job_id, event_id = m.group(1), m.group(2)
        # Retry up to 3 times for network resilience
        json_str = None
        for attempt in range(3):
            json_str = fetch_event_results_json(job_id, event_id)
            if json_str:
                break
            print(f"Attempt {attempt+1} failed for job_id={job_id}, event_id={event_id}, retrying...")
            time.sleep(2)
        if not json_str:
            print(f"Skipping event {job_id=} {event_id=}: no data returned")
            continue
        # Small delay to be respectful to the server
        time.sleep(1)
        event_results = parse_event_results_json(json_str, job_id=job_id)
        all_event_results.extend(event_results)

    # Aggregate race results by athlete name
    athletes = aggregate_athletes(all_event_results)
    return athletes, regatta_metadata


def main():
    """Main execution function for the scraper.
    
    Orchestrates the entire scraping process:
    1. Scrapes athlete data and regatta metadata from the default main results URL
    2. Prints regatta information and a summary of athletes with their race results
    3. Exports the flattened data to CSV
    """
    # Scrape athletes and regatta metadata from the default URL
    athletes, regatta_metadata = scrape_athletes_from_url(main_results_url)
    
    # Print regatta info
    print(f"Regatta: {regatta_metadata['name']}")
    print(f"  Dates: {regatta_metadata['start_date']} - {regatta_metadata['end_date']}")
    print(f"  Venue: {regatta_metadata['venue']}, {regatta_metadata['location']}")
    print(f"  Host: {regatta_metadata['host']}")
    print(f"  Type: {regatta_metadata['race_type']}")
    print(f"  Entries: {regatta_metadata['entries']}, Clubs: {regatta_metadata['clubs']}")
    print(f"  USRowing Sanctioned: {regatta_metadata['sanctioned']}")
    print()
    
    # Print summary to console
    for name, info in athletes.items():
        print(f"Athlete: {name}")
        print(f"  Clubs: {', '.join(info['clubs'])}")
        print(f"  Races:")
        for race in info["races"]:
            print(
                f"    - Event: {race['event']}, Race: {race['race']}, Place: {race['place']}, Club: {race['club']}, Finish: {race['finish']}, Seat: {race['seat']}, Boat Count: {race['num_boats']}"
            )
        print()
    
    # Export to CSV for further analysis
    write_athletes_to_csv(athletes)
    print("Wrote athlete data to athletes.csv")
if __name__ == "__main__":
    main()

