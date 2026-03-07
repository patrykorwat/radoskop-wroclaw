#!/usr/bin/env python3
"""
Scraper danych głosowań Rady Miasta Wrocławia.

Źródło: bip.um.wroc.pl
BIP Wrocławia to HTML z linkami do protokołów PDF. Głosowania imienne parsujemy z PDF-ów (PyMuPDF).

Struktura BIP Wrocławia:
  1. Lista sesji: bip.um.wroc.pl/artykuly/769/sesje-rady
  2. Sesja: bip.um.wroc.pl/artykul/1179/XXXXX/sesja-rady-miejskiej-wroclawia-nr-...
  3. Protokół PDF: bip.um.wroc.pl/attachments/download/XXXXX

Podejście:
  1. Pobierz listę sesji IX kadencji
  2. Dla każdej sesji — pobierz stronę i znajdź linki do protokołów PDF
  3. Pobierz i sparsuj każdy PDF → wyniki głosowań imiennych
  4. Zbuduj data.json w formacie Radoskop

Użycie:
    pip install requests beautifulsoup4 lxml pymupdf
    python scrape_wroclaw.py [--output docs/data.json] [--profiles docs/profiles.json]

UWAGA: Uruchom lokalnie — sandbox Cowork blokuje domeny *.wroc.pl
"""

import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from itertools import combinations
from pathlib import Path
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Zainstaluj: pip install beautifulsoup4 lxml")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("Zainstaluj: pip install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    print("Zainstaluj: pip install pymupdf")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BIP_BASE = "https://bip.um.wroc.pl"
SESSIONS_URL = f"{BIP_BASE}/artykuly/769/sesje-rady"

KADENCJE = {
    "2024-2029": {"label": "IX kadencja (2024–2029)", "start": "2024-05-07"},
}

DELAY = 1.0

# PDF cache directory
PDF_DIR = None

# Reusable HTTP session
_session = None

# Councilor → Club mapping for 2024-2029 kadencja
# Based on composition: KO (~23), PiS (~8), Lewica (~3), BS (Bezpartyjni Samorządowcy, ~3)
# TODO: Fill in complete club mapping from official source
COUNCILOR_CLUBS = {
    # This is a skeleton — update with actual council member data from:
    # https://bip.um.wroc.pl/artykuly/768/sklad-osobowy-rady
    #
    # Example entries:
    # "Jacek Sutryk": "KO",
    # "Ewa Kopacz": "KO",
    # etc.
}


def init_session():
    """Create a requests session with proper headers."""
    global _session
    _session = requests.Session()
    _session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept-Language": "pl-PL,pl;q=0.9",
    })


def fetch(url: str) -> BeautifulSoup:
    """Fetch a page and return BeautifulSoup."""
    time.sleep(DELAY)
    print(f"  GET {url}")
    resp = _session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


# ---------------------------------------------------------------------------
# Polish month name → number mapping
# ---------------------------------------------------------------------------
MONTHS_PL = {
    "stycznia": 1, "lutego": 2, "marca": 3, "kwietnia": 4,
    "maja": 5, "czerwca": 6, "lipca": 7, "sierpnia": 8,
    "września": 9, "października": 10, "listopada": 11, "grudnia": 12,
    "luty": 2, "marzec": 3, "kwiecień": 4, "maj": 5,
    "czerwiec": 6, "lipiec": 7, "sierpień": 8, "wrzesień": 9,
    "październik": 10, "listopad": 11, "grudzień": 12, "styczeń": 1,
}


def parse_polish_date(text: str) -> str | None:
    """Parse '25 Lutego 2026 r.' or '25 lutego 2026' → '2026-02-25'."""
    text = text.strip().rstrip(".")
    # Remove trailing 'r' or 'r.'
    text = re.sub(r'\s*r\.?$', '', text)
    m = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', text)
    if not m:
        return None
    day = int(m.group(1))
    month_name = m.group(2).lower()
    year = int(m.group(3))
    month = MONTHS_PL.get(month_name)
    if not month:
        return None
    return f"{year}-{month:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# Step 1: Scrape session list
# ---------------------------------------------------------------------------

def scrape_session_list() -> list[dict]:
    """Fetch the session list page and extract all sessions.

    TODO: Verify actual HTML structure on bip.um.wroc.pl/artykuly/769/sesje-rady
    Sessions may be in article listings or downloadable PDF list.
    """
    soup = fetch(SESSIONS_URL)
    sessions = []

    # Strategy 1: Look for links to session pages
    # Pattern: bip.um.wroc.pl/artykul/1179/XXXXX/sesja-rady-miejskiej-wroclawia-nr-...
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)

        # Check if this looks like a session page link
        if "/artykul/1179/" not in href or "sesja" not in href.lower():
            continue

        if not href.startswith("http"):
            href = urljoin(BIP_BASE, href)

        # Try to extract session date from text
        # Typical format: "XLVI Sesja Rady Miasta Wrocławia z dnia 25 lutego 2026"
        # or just "25 lutego 2026"
        date_match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', text)
        if not date_match:
            # Try alternate format with dots: "25.02.2026"
            date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
            if date_match:
                day, month, year = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
                date = f"{year}-{month:02d}-{day:02d}"
            else:
                continue
        else:
            date = parse_polish_date(date_match.group(0))
            if not date:
                continue

        # Check if date is in IX kadencja (from 2024-05-07)
        if date < "2024-05-07":
            continue

        # Extract session number (Roman numeral) from text
        num_match = re.search(r'([IVXLCDM]+)\s+[Ss]esja', text)
        number = num_match.group(1) if num_match else "?"

        sessions.append({
            "number": number,
            "date": date,
            "url": href,
        })
        print(f"    Sesja {number} ({date})")

    if not sessions:
        print("    UWAGA: Brak sesji — sprawdź strukturę BIP")
        # TODO: Try alternate parsing strategies based on actual HTML structure

    print(f"  Znaleziono {len(sessions)} sesji")
    return sorted(sessions, key=lambda x: x["date"])


# ---------------------------------------------------------------------------
# Step 2: Fetch session page and extract PDF protocol links
# ---------------------------------------------------------------------------

def scrape_session_pdf_links(session: dict) -> list[dict]:
    """Fetch session page and find protocol PDF links.

    TODO: Verify actual link structure. PDFs may be:
      - Direct links to attachments/download/XXXXX
      - Links in <a> tags with text like "Protokół"
      - Or accessible via another page structure
    """
    soup = fetch(session["url"])
    pdf_links = []

    # Strategy 1: Look for download links with "protokol" or "pdf" in href
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()

        if "attachments/download" not in href and "pdf" not in href.lower():
            continue

        if "protokol" not in text and "pdf" not in href.lower():
            continue

        if not href.startswith("http"):
            href = urljoin(BIP_BASE, href)

        # Extract attachment ID from URL like: .../attachments/download/12345
        # or construct full URL if just ID is present
        if "attachments/download/" in href:
            att_id = re.search(r'attachments/download/(\d+)', href)
            if att_id:
                att_id = att_id.group(1)
                href = f"{BIP_BASE}/attachments/download/{att_id}"

        pdf_links.append({
            "url": href,
            "text": text,
        })
        print(f"    Znaleziono protokół: {text[:60]}")

    return pdf_links


# ---------------------------------------------------------------------------
# Step 3: Download and parse protocol PDFs
# ---------------------------------------------------------------------------

def download_pdf(url: str, session_date: str) -> Path | None:
    """Download a PDF protocol to local cache."""
    global PDF_DIR
    if PDF_DIR is None:
        return None

    filename = f"protokol_{session_date}.pdf"
    filepath = PDF_DIR / filename

    if filepath.exists() and filepath.stat().st_size > 1000:
        print(f"    (cache) {filename}")
        return filepath

    try:
        time.sleep(DELAY)
        print(f"    Pobieranie PDF...")
        resp = _session.get(url, timeout=120, allow_redirects=True)
        resp.raise_for_status()

        if len(resp.content) < 500:
            print(f"    UWAGA: PDF za mały ({len(resp.content)} B) — pomijam")
            return None

        # Verify it's a PDF
        if b"%PDF" not in resp.content[:20]:
            print(f"    UWAGA: Nie PDF ({len(resp.content)} B)")
            return None

        filepath.write_bytes(resp.content)
        print(f"    Zapisano: {filename} ({len(resp.content) / 1024:.0f} KB)")
        return filepath
    except Exception as e:
        print(f"    BŁĄD pobierania: {e}")
        return None


def parse_protocol_pdf(pdf_path: Path, session: dict) -> list[dict]:
    """Parse a BIP Wrocławia protocol PDF to extract named votes.

    TODO: Verify actual PDF structure from sample protocol.
    Likely format similar to Gdynia:
      Głosowano w sprawie: [topic]
      ZA: X, PRZECIW: Y, WSTRZYMUJĘ SIĘ: Z, BRAK GŁOSU: W, NIEOBECNI: N
      ZA (X) Name1, Name2, ...
      PRZECIW (Y) Name1, Name2, ...
      etc.
    """
    try:
        doc = fitz.open(str(pdf_path))
        full_text = ""
        for page in doc:
            full_text += page.get_text() + "\n"
        doc.close()
    except Exception as e:
        print(f"    BŁĄD otwierania PDF: {e}")
        return []

    # Remove page headers/footers
    full_text = re.sub(
        r'\n\s*[IVXLCDM]+\s+Sesja\s+Rady\s+Miasta\s+Wrocławia.{0,50}\d{4}\s*r?\.\s*\n\s*Strona\s*\d+\s*\n?',
        ' ',
        full_text,
        flags=re.IGNORECASE
    )
    full_text = re.sub(r'\nStrona\s*\d+\s*\n', '\n', full_text)

    votes = []

    # Split on "Głosowano w sprawie:" to find individual votes
    vote_sections = re.split(
        r'Głosowano\s+(?:wniosek\s+)?w\s+sprawie:\s*',
        full_text,
        flags=re.IGNORECASE
    )

    if len(vote_sections) > 1:
        for vi, section in enumerate(vote_sections[1:], 1):
            vote = _parse_vote_section_wroclaw(section, session, vi)
            if vote:
                votes.append(vote)
    else:
        # Fallback: try to find votes by "ZA:" pattern
        print(f"    UWAGA: Brak 'Głosowano w sprawie:' — próba alternatywnego parsowania")
        # TODO: Implement fallback parsing logic

    return votes


def _parse_vote_section_wroclaw(section_text: str, session: dict, vote_idx: int) -> dict | None:
    """Parse a single vote section from the protocol PDF text.

    Expected format:
      [topic text]
      ZA: N, PRZECIW: M, WSTRZYMUJĘ SIĘ: K, BRAK GŁOSU: B, NIEOBECNI: U
      ZA (N) Name1, Name2, ...
      PRZECIW (M) Name1, Name2, ...
      WSTRZYMUJĘ SIĘ (K) Name1, Name2, ...
      BRAK GŁOSU (B) Name1, Name2, ...
      NIEOBECNI (U) Name1, Name2, ...
    """
    # --- Topic: everything before first "ZA:" or "ZA (" ---
    first_za = re.search(r'ZA\s*(\(|\:)', section_text)
    topic_end = len(section_text)
    if first_za:
        topic_end = first_za.start()

    topic_raw = section_text[:topic_end].strip()
    topic = re.sub(r'\s+', ' ', topic_raw).strip()
    topic = topic[:500] if topic else f"Głosowanie {vote_idx}"

    # --- Parse named votes ---
    named_votes = {
        "za": [],
        "przeciw": [],
        "wstrzymal_sie": [],
        "brak_glosu": [],
        "nieobecni": [],
    }

    _parse_named_category_wroclaw(section_text, r'ZA\s*\((\d+)\)', "za", named_votes)
    _parse_named_category_wroclaw(section_text, r'PRZECIW\s*\((\d+)\)', "przeciw", named_votes)
    _parse_named_category_wroclaw(section_text, r'WSTRZYMUJĘ?\s+SIĘ\s*\((\d+)\)', "wstrzymal_sie", named_votes)
    _parse_named_category_wroclaw(section_text, r'BRAK\s+GŁOSU\s*\((\d+)\)', "brak_glosu", named_votes)
    _parse_named_category_wroclaw(section_text, r'NIEOBECNI\s*\((\d+)\)', "nieobecni", named_votes)

    total_named = sum(len(v) for v in named_votes.values())
    if total_named == 0:
        return None

    # --- Vote counts ---
    counts = {}
    za_m = re.search(r'ZA:\s*(\d+)', section_text)
    counts["za"] = int(za_m.group(1)) if za_m else len(named_votes["za"])
    przeciw_m = re.search(r'PRZECIW:\s*(\d+)', section_text)
    counts["przeciw"] = int(przeciw_m.group(1)) if przeciw_m else len(named_votes["przeciw"])
    wstrzymal_m = re.search(r'WSTRZYMUJĘ\s+SIĘ:\s*(\d+)', section_text, re.IGNORECASE)
    counts["wstrzymal_sie"] = int(wstrzymal_m.group(1)) if wstrzymal_m else len(named_votes["wstrzymal_sie"])
    brak_m = re.search(r'BRAK\s+GŁOSU:\s*(\d+)', section_text, re.IGNORECASE)
    counts["brak_glosu"] = int(brak_m.group(1)) if brak_m else len(named_votes["brak_glosu"])
    nieobecni_m = re.search(r'NIEOBECNI:\s*(\d+)', section_text, re.IGNORECASE)
    counts["nieobecni"] = int(nieobecni_m.group(1)) if nieobecni_m else len(named_votes["nieobecni"])

    # Deduplicate names across categories
    _deduplicate_named_votes_wroclaw(named_votes, counts)

    # --- Resolution status ---
    resolution = None
    if re.search(r'Uchwała\s+została\s+podjęta', section_text, re.IGNORECASE):
        resolution = "przyjęta"
    elif re.search(r'Uchwała\s+nie\s+została\s+podjęta', section_text, re.IGNORECASE):
        resolution = "odrzucona"

    vote_id = f"{session['date']}_{vote_idx:03d}"

    return {
        "id": vote_id,
        "source_url": session.get("url", ""),
        "session_date": session["date"],
        "session_number": session["number"],
        "topic": topic,
        "druk": None,
        "resolution": resolution,
        "counts": counts,
        "named_votes": named_votes,
    }


def _parse_named_category_wroclaw(text: str, pattern: str, category: str, named_votes: dict):
    """Extract names from a category like 'ZA (26) Name1, Name2, ...'"""
    m = re.search(pattern, text, re.IGNORECASE)
    if not m:
        return

    # Get the position after the category header
    start_pos = m.end()
    # Find the next category header or end of text
    next_category_patterns = [
        r'PRZECIW\s*\(',
        r'WSTRZYMUJĘ\s+SIĘ\s*\(',
        r'BRAK\s+GŁOSU\s*\(',
        r'NIEOBECNI\s*\(',
        r'Uchwała\s+została',
        r'Uchwała\s+nie',
    ]
    end_pos = len(text)
    for pat in next_category_patterns:
        next_m = re.search(pat, text[start_pos:], re.IGNORECASE)
        if next_m:
            end_pos = start_pos + next_m.start()
            break

    category_text = text[start_pos:end_pos]

    # Parse comma-separated names
    # Handle formats like: "Name1, Name2, Name3"
    # or "Name1,Name2, Name3" (inconsistent spacing)
    names_raw = category_text.split(',')
    for name_raw in names_raw:
        name = name_raw.strip()
        # Skip empty and non-name artifacts
        if len(name) < 3:
            continue
        if re.match(r'^\d+\s*\)', name):
            # Leftover vote count like "26) Name" — skip
            continue

        # Clean up any remaining markers
        name = re.sub(r'^\d+\s+', '', name)  # Remove leading numbers
        name = re.sub(r'\s+$', '', name)  # Trailing spaces

        if len(name) >= 3 and name not in named_votes[category]:
            named_votes[category].append(name)


def _deduplicate_named_votes_wroclaw(named_votes: dict, counts: dict):
    """Remove duplicate names appearing in multiple categories."""
    seen_names = set()
    for cat in ["za", "przeciw", "wstrzymal_sie", "brak_glosu", "nieobecni"]:
        filtered = []
        for name in named_votes[cat]:
            if name not in seen_names:
                seen_names.add(name)
                filtered.append(name)
        named_votes[cat] = filtered


# ---------------------------------------------------------------------------
# Step 4: Build output structures
# ---------------------------------------------------------------------------

def load_profiles(profiles_path: str) -> dict:
    """Load profiles.json with councilor → club mapping."""
    path = Path(profiles_path)
    if not path.exists():
        print(f"  UWAGA: Brak {profiles_path} — kluby będą oznaczone jako '?'")
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    for p in data.get("profiles", []):
        name = p["name"]
        kadencje = p.get("kadencje", {})
        if kadencje:
            latest = list(kadencje.values())[-1]
            result[name] = {
                "name": name,
                "club": latest.get("club", "?"),
                "district": latest.get("okręg"),
            }
    return result


def compute_club_majority(vote: dict, profiles: dict) -> dict[str, str]:
    """For each club, compute the majority position in a given vote."""
    club_votes = defaultdict(lambda: {"za": 0, "przeciw": 0, "wstrzymal_sie": 0})
    for cat in ["za", "przeciw", "wstrzymal_sie"]:
        for name in vote["named_votes"].get(cat, []):
            club = profiles.get(name, {}).get("club", "?")
            if club != "?":
                club_votes[club][cat] += 1

    majority = {}
    for club, counts in club_votes.items():
        best = max(counts, key=counts.get)
        majority[club] = best
    return majority


def build_councilors(all_votes: list[dict], sessions: list[dict], profiles: dict) -> list[dict]:
    """Build councilor statistics from vote data."""
    all_names = set()
    for v in all_votes:
        for cat_names in v["named_votes"].values():
            all_names.update(cat_names)

    councilors = {}
    for name in sorted(all_names):
        prof = profiles.get(name, {})
        councilors[name] = {
            "name": name,
            "club": prof.get("club", "?"),
            "district": prof.get("district"),
            "votes_za": 0,
            "votes_przeciw": 0,
            "votes_wstrzymal": 0,
            "votes_brak": 0,
            "votes_nieobecny": 0,
            "sessions_present": set(),
            "votes_with_club": 0,
            "votes_against_club": 0,
            "rebellions": [],
        }

    for v in all_votes:
        club_majority = compute_club_majority(v, profiles)

        for name in v["named_votes"].get("za", []):
            if name in councilors:
                councilors[name]["votes_za"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
                _check_rebellion(councilors[name], "za", club_majority, v)
        for name in v["named_votes"].get("przeciw", []):
            if name in councilors:
                councilors[name]["votes_przeciw"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
                _check_rebellion(councilors[name], "przeciw", club_majority, v)
        for name in v["named_votes"].get("wstrzymal_sie", []):
            if name in councilors:
                councilors[name]["votes_wstrzymal"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
                _check_rebellion(councilors[name], "wstrzymal_sie", club_majority, v)
        for name in v["named_votes"].get("brak_glosu", []):
            if name in councilors:
                councilors[name]["votes_brak"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
        for name in v["named_votes"].get("nieobecni", []):
            if name in councilors:
                councilors[name]["votes_nieobecny"] += 1

    # Only count sessions that have vote data for frekwencja calculation
    sessions_with_votes = set(v["session_date"] for v in all_votes if v.get("session_date"))
    total_sessions = len(sessions_with_votes)
    total_votes = len(all_votes)

    result = []
    for c in councilors.values():
        present_votes = c["votes_za"] + c["votes_przeciw"] + c["votes_wstrzymal"] + c["votes_brak"]
        frekwencja = (len(c["sessions_present"]) / total_sessions * 100) if total_sessions > 0 else 0
        aktywnosc = (present_votes / total_votes * 100) if total_votes > 0 else 0
        total_club_votes = c["votes_with_club"] + c["votes_against_club"]
        zgodnosc = (c["votes_with_club"] / total_club_votes * 100) if total_club_votes > 0 else 0

        result.append({
            "name": c["name"],
            "club": c["club"],
            "district": c["district"],
            "frekwencja": round(frekwencja, 1),
            "aktywnosc": round(aktywnosc, 1),
            "zgodnosc_z_klubem": round(zgodnosc, 1),
            "votes_za": c["votes_za"],
            "votes_przeciw": c["votes_przeciw"],
            "votes_wstrzymal": c["votes_wstrzymal"],
            "votes_brak": c["votes_brak"],
            "votes_nieobecny": c["votes_nieobecny"],
            "votes_total": total_votes,
            "rebellion_count": len(c["rebellions"]),
            "rebellions": c["rebellions"][:20],
            "has_activity_data": False,
            "activity": None,
        })

    return sorted(result, key=lambda x: x["name"])


def _check_rebellion(councilor: dict, vote_cat: str, club_majority: dict, vote: dict):
    """Check if councilor voted differently from their club majority."""
    club = councilor["club"]
    if club == "?" or club not in club_majority:
        return
    majority_cat = club_majority[club]
    if vote_cat == majority_cat:
        councilor["votes_with_club"] += 1
    else:
        councilor["votes_against_club"] += 1
        councilor["rebellions"].append({
            "vote_id": vote["id"],
            "session": vote["session_date"],
            "topic": vote["topic"][:120],
            "their_vote": vote_cat,
            "club_majority": majority_cat,
        })


def compute_similarity(all_votes: list[dict], councilors_list: list[dict]) -> tuple[list, list]:
    """Compute councilor pairs with highest/lowest voting similarity."""
    name_to_club = {c["name"]: c["club"] for c in councilors_list}
    vectors = defaultdict(dict)
    for v in all_votes:
        for cat in ["za", "przeciw", "wstrzymal_sie"]:
            for name in v["named_votes"].get(cat, []):
                vectors[name][v["id"]] = cat

    names = sorted(vectors.keys())
    pairs = []
    for a, b in combinations(names, 2):
        common = set(vectors[a].keys()) & set(vectors[b].keys())
        if len(common) < 10:
            continue
        same = sum(1 for vid in common if vectors[a][vid] == vectors[b][vid])
        score = round(same / len(common) * 100, 1)
        pairs.append({
            "a": a,
            "b": b,
            "club_a": name_to_club.get(a, "?"),
            "club_b": name_to_club.get(b, "?"),
            "score": score,
            "common_votes": len(common),
        })

    pairs.sort(key=lambda x: x["score"], reverse=True)
    top = pairs[:20]
    bottom = pairs[-20:][::-1]
    return top, bottom


def build_sessions(sessions_raw: list[dict], all_votes: list[dict]) -> list[dict]:
    """Build session data with attendee info."""
    votes_by_key = defaultdict(list)
    for v in all_votes:
        key = (v["session_date"], v.get("session_number", ""))
        votes_by_key[key].append(v)

    votes_by_date = defaultdict(list)
    for v in all_votes:
        votes_by_date[v["session_date"]].append(v)

    date_counts = Counter(s["date"] for s in sessions_raw)

    result = []
    for s in sessions_raw:
        date = s["date"]
        number = s.get("number", "")

        if date_counts[date] > 1:
            session_votes = votes_by_key.get((date, number), [])
        else:
            session_votes = votes_by_date.get(date, [])

        attendees = set()
        for v in session_votes:
            for cat in ["za", "przeciw", "wstrzymal_sie", "brak_glosu"]:
                attendees.update(v["named_votes"].get(cat, []))

        result.append({
            "date": date,
            "number": number,
            "vote_count": len(session_votes),
            "attendee_count": len(attendees),
            "attendees": sorted(attendees),
            "speakers": [],
        })

    return sorted(result, key=lambda x: (x["date"], x["number"]))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scraper Rady Miasta Wrocławia (BIP)")
    parser.add_argument("--output", default="docs/data.json", help="Plik wyjściowy")
    parser.add_argument("--delay", type=float, default=1.0, help="Opóźnienie między requestami (s)")
    parser.add_argument("--max-sessions", type=int, default=0, help="Maks. sesji (0=wszystkie)")
    parser.add_argument("--dry-run", action="store_true", help="Tylko lista sesji, bez głosowań")
    parser.add_argument("--profiles", default="docs/profiles.json", help="Plik profiles.json")
    parser.add_argument("--explore", action="store_true", help="Pobierz 1 sesję i pokaż strukturę")
    args = parser.parse_args()

    global DELAY, PDF_DIR
    DELAY = args.delay

    print("=== Radoskop Scraper: Rada Miasta Wrocławia (BIP) ===")
    print(f"Backend: requests + BeautifulSoup + PyMuPDF")
    print()

    # Setup PDF cache
    pdf_cache = Path(args.output).parent / "pdfs"
    pdf_cache.mkdir(parents=True, exist_ok=True)
    PDF_DIR = pdf_cache

    init_session()

    total_steps = 3

    # 1. Session list
    print(f"[1/{total_steps}] Pobieranie listy sesji...")
    all_sessions = scrape_session_list()

    if not all_sessions:
        print("BŁĄD: Nie znaleziono sesji.")
        print(f"Sprawdź ręcznie: {SESSIONS_URL}")
        sys.exit(1)

    if args.max_sessions > 0:
        all_sessions = all_sessions[:args.max_sessions]
        print(f"  (ograniczono do {args.max_sessions} sesji)")

    if args.dry_run:
        print("\nZnalezione sesje:")
        for s in all_sessions:
            print(f"  {s['number']:>8} | {s['date']} | {s['url']}")
        return

    if args.explore:
        s0 = all_sessions[-1]  # latest session
        print(f"\n[explore] Sesja {s0['number']} ({s0['date']})")
        print(f"  URL: {s0['url']}")
        soup = fetch(s0["url"])
        print("\n--- Linki na stronie sesji ---")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)[:120]
            if text and not href.startswith("javascript") and not href.startswith("#"):
                print(f"  [{text}]")
                print(f"    -> {href}")
        return

    # 2. Fetch PDFs and parse votes from each session
    print(f"\n[2/{total_steps}] Pobieranie i parsowanie protokołów ({len(all_sessions)} sesji)...")
    all_votes = []
    for si, session in enumerate(all_sessions):
        print(f"\n  Sesja {session['number']} ({session['date']}) [{si+1}/{len(all_sessions)}]")

        pdf_links = scrape_session_pdf_links(session)
        if not pdf_links:
            print(f"    Brak protokołów")
            continue

        for pdf_link in pdf_links:
            pdf_path = download_pdf(pdf_link["url"], session["date"])
            if not pdf_path:
                continue

            votes = parse_protocol_pdf(pdf_path, session)
            all_votes.extend(votes)
            print(f"    Sparsowano {len(votes)} głosowań")

    if not all_votes:
        print("UWAGA: Nie znaleziono głosowań.")
        sys.exit(1)

    print(f"  Razem: {len(all_votes)} głosowań z {len(all_sessions)} sesji")

    # 3. Build output
    print(f"\n[3/{total_steps}] Budowanie pliku wyjściowego...")
    profiles = load_profiles(args.profiles)
    if profiles:
        print(f"  Załadowano profile: {len(profiles)} radnych")

    kid = "2024-2029"
    councilors = build_councilors(all_votes, all_sessions, profiles)
    sessions_data = build_sessions(all_sessions, all_votes)
    sim_top, sim_bottom = compute_similarity(all_votes, councilors)

    club_counts = defaultdict(int)
    for c in councilors:
        club_counts[c["club"]] += 1

    print(f"  {len(sessions_data)} sesji, {len(all_votes)} głosowań, {len(councilors)} radnych")
    print(f"  Kluby: {dict(club_counts)}")

    kad_output = {
        "id": kid,
        "label": KADENCJE[kid]["label"],
        "clubs": {club: count for club, count in sorted(club_counts.items())},
        "sessions": sessions_data,
        "total_sessions": len(sessions_data),
        "total_votes": len(all_votes),
        "total_councilors": len(councilors),
        "councilors": councilors,
        "votes": all_votes,
        "similarity_top": sim_top,
        "similarity_bottom": sim_bottom,
    }

    output = {
        "generated": datetime.now().isoformat(),
        "default_kadencja": kid,
        "kadencje": [kad_output],
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nGotowe! Zapisano do {out_path}")
    total_v = len(all_votes)
    named_v = sum(1 for v in all_votes if sum(len(nv) for nv in v["named_votes"].values()) > 0)
    print(f"  {len(sessions_data)} sesji, {total_v} głosowań ({named_v} z imiennymi), {len(councilors)} radnych")


if __name__ == "__main__":
    main()
