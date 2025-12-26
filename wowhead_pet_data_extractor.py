"""
Wowhead Pet Data Extractor
Pulls data only from Wowhead
- Step 1: Extract families from https://www.wowhead.com/hunter-pets
- Step 2: Extract tameable NPCs from each family page
- Step 3: Fetch display IDs from individual NPC pages

Output files and skip logic stay the same
"""

import asyncio
import csv
import json
import os
import re
import time
import logging
from argparse import ArgumentParser

import requests
from bs4 import BeautifulSoup

import sys

if sys.version_info < (3, 7):
    print("Python 3.7 or higher is required.")
    sys.exit(1)

# Centralized constants and settings
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
NPC_CONCURRENCY = 25  # Reduced to avoid rate limiting
BATCH_SIZE = 100

WOWHEAD_HUNTER_PETS_URL = 'https://www.wowhead.com/hunter-pets'

CACHE_DIR = 'Cache'
OUTPUT_DIR = 'Output'
FAMILIES_FILE = os.path.join(CACHE_DIR, 'families.json')
WOWHEAD_PROGRESS_FILE = os.path.join(CACHE_DIR, 'wowhead_data.json')
DISPLAY_IDS_PROGRESS_FILE = os.path.join(CACHE_DIR, 'display_ids_progress.json')

# Optional: NPC IDs to skip visiting on individual NPC pages
SKIP_NPC_IDS = set()
SKIP_NPC_IDS_FILE = os.path.join(CACHE_DIR, 'skip_npc_ids.json')

# Optional: Manual mapping for display IDs to correct known issues
DISPLAY_ID_MAPPING_FILE = os.path.join(CACHE_DIR, 'correct_display_ids.json')

# Optional: Display IDs to skip from final output
SKIP_DISPLAY_IDS = set()
SKIP_DISPLAY_IDS_FILE = os.path.join(CACHE_DIR, 'skip_display_ids.json')


# Helper functions for JSON operations
def ensure_parent_dir(file_path: str) -> None:
    """Ensure parent directory exists before writing a file"""
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def save_json(path, data):
    """Helper to save JSON with proper error handling"""
    try:
        ensure_parent_dir(path)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.debug(f"Saved JSON to {path}")
    except Exception as e:
        logging.error(f"Failed to save {path}: {e}")


def load_json(path):
    """Helper to load JSON with fallback to legacy path"""
    if not os.path.exists(path):
        legacy = os.path.basename(path)
        if os.path.exists(legacy):
            path = legacy
            logging.debug(f"Using legacy path: {legacy}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        logging.error(f"Failed to load {path}: {e}")
        return None


def load_skip_npc_ids():
    """Load extra NPC IDs to skip from a JSON file.
    Accepted formats:
      - ["235095", 123456]
      - {"skip": ["235095", 123456]}
    Returns a set of string NPC IDs.
    """
    try:
        primary = SKIP_NPC_IDS_FILE
        legacy = os.path.basename(SKIP_NPC_IDS_FILE)
        path = primary if os.path.exists(primary) else (legacy if os.path.exists(legacy) else None)
        if path:
            data = load_json(path)
            if data is None:
                return set()
            if isinstance(data, list):
                return {str(x) for x in data}
            if isinstance(data, dict) and isinstance(data.get('skip'), list):
                return {str(x) for x in data.get('skip', [])}
            logging.warning('Skip file format not recognized, expected a JSON array or {"skip": [...]}')
            return set()
        else:
            logging.info("No skip list file found")
            return set()
    except Exception as e:
        logging.error(f"Failed to load skip list: {e}")
        return set()


def load_display_id_mapping():
    """Load manual display ID mappings from a JSON file.
    Format: {"npc_id": ["display_id1", "display_id2"], ...}
    Returns a dict of {str(npc_id): list of str(display_ids)}
    """
    try:
        primary = DISPLAY_ID_MAPPING_FILE
        legacy = os.path.basename(DISPLAY_ID_MAPPING_FILE)
        path = primary if os.path.exists(primary) else (legacy if os.path.exists(legacy) else None)
        if path:
            data = load_json(path)
            if data is None:
                return {}
            if isinstance(data, dict):
                # Normalize to strings
                normalized = {}
                for npc_id, display_ids in data.items():
                    npc_str = str(npc_id)
                    if isinstance(display_ids, list):
                        ids = [str(did) for did in display_ids if str(did) and str(did) != '0']
                        if ids:
                            normalized[npc_str] = sorted(set(ids), key=int)
                    elif display_ids is not None:
                        did_str = str(display_ids)
                        if did_str and did_str != '0':
                            normalized[npc_str] = [did_str]
                return normalized
            logging.warning('Display ID mapping file format not recognized, expected {"npc_id": ["did1", "did2"], ...}')
            return {}
        else:
            logging.info("No display ID mapping file found")
            return {}
    except Exception as e:
        logging.error(f"Failed to load display ID mapping: {e}")
        return {}


def load_skip_display_ids():
    """Load display IDs to skip from a JSON file.
    Accepted formats:
      - ["12345", 67890]
      - {"skip": ["12345", 67890]}
    Returns a set of string display IDs.
    """
    try:
        primary = SKIP_DISPLAY_IDS_FILE
        legacy = os.path.basename(SKIP_DISPLAY_IDS_FILE)
        path = primary if os.path.exists(primary) else (legacy if os.path.exists(legacy) else None)
        if path:
            data = load_json(path)
            if data is None:
                return set()
            if isinstance(data, list):
                return {str(x) for x in data}
            if isinstance(data, dict) and isinstance(data.get('skip'), list):
                return {str(x) for x in data.get('skip', [])}
            logging.warning('Skip display IDs file format not recognized, expected a JSON array or {"skip": [...]}')
            return set()
        else:
            logging.info("No skip display IDs file found")
            return set()
    except Exception as e:
        logging.error(f"Failed to load skip display IDs: {e}")
        return set()


class ProgressManager:
    """Handles all progress saving and loading operations"""

    def __init__(self, progress_file: str = DISPLAY_IDS_PROGRESS_FILE):
        self.progress_file = progress_file

    def save_progress(self, successful_npcs: set, failed_npcs: set, found_display_ids: dict) -> None:
        """Save current progress"""
        progress = {
            'successful_npcs': list(successful_npcs),
            'failed_npcs': list(failed_npcs),
            'found_display_ids': found_display_ids
        }
        save_json(self.progress_file, progress)
        logging.info(f"Progress saved to {self.progress_file}")

    def load_progress(self) -> tuple[set, set, dict]:
        """Load previous progress"""
        progress = load_json(self.progress_file)
        if progress is None:
            logging.info("No previous progress file found, starting fresh")
            return set(), set(), {}

        logging.info(f"Progress loaded from {self.progress_file}")
        return (set(progress.get('successful_npcs', [])),
                set(progress.get('failed_npcs', [])),
                progress.get('found_display_ids', {}))

    def reset_failed_npcs(self) -> None:
        """Reset failed NPCs so they can be retried"""
        progress = load_json(self.progress_file)
        if progress is None:
            logging.warning("No progress file found")
            return

        cleared_count = len(progress.get('failed_npcs', []))
        progress['failed_npcs'] = []
        logging.info(f"Cleared {cleared_count} failed NPCs for retry")

        save_json(self.progress_file, progress)
        logging.info(f"Failed NPCs reset in {self.progress_file}")


async def extract_families(url):
    """
    Extract pet families from Wowhead hunter-pets page
    Returns dict of {family_id: family_name}
    """
    logging.info(f"Fetching families from {url}...")

    # Fetch the hunter-pets page with timeout and retries
    max_retries = 3
    response = None
    for attempt in range(max_retries):
        try:
            logging.debug(f"Attempting request to {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, timeout=30, headers={'User-Agent': USER_AGENT})
            response.raise_for_status()
            logging.debug(f"Request to {url} successful")
            break
        except requests.Timeout:
            logging.warning(f"Request to {url} timed out after 30s (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in 2 seconds...")
                time.sleep(2)
            else:
                raise
        except requests.RequestException as e:
            logging.warning(f"Request to {url} failed: {e} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in 2 seconds...")
                time.sleep(2)
            else:
                raise

    if response is None:
        raise requests.RequestException("Failed to fetch data after all retries")

    html_content = response.text

    # Extract families from JavaScript data
    data_match = re.search(r'data:\s*(\[.*\])\s*\}\);\s*//\]\]', html_content, re.DOTALL)
    if not data_match:
        logging.error("Could not find pet data in HTML")
        return {}

    data_json = data_match.group(1)
    try:
        pet_data = json.loads(data_json)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse pet data JSON: {e}")
        return {}

    # Each item is a family: id and name
    families = {}
    for item in pet_data:
        family_id = str(item.get('id'))
        family_name = item.get('name')
        if family_id and family_name:
            families[family_id] = family_name
            logging.debug(f"Found family: {family_id} - {family_name}")

    logging.info(f"Found {len(families)} families")
    return families


async def extract_npcs_from_families(families):
    """
    Extract tameable NPCs from each family page
    Returns list of {'family': str, 'npc_id': str, 'npc_name': str}
    """
    logging.info("Extracting NPCs from family pages...")

    # Step 2: Extract NPCs from each family page concurrently
    data = []
    semaphore = asyncio.Semaphore(NPC_CONCURRENCY)

    async def process_family(family_id, family_name):
        nonlocal data
        async with semaphore:
            family_url = f"https://www.wowhead.com/pet={family_id}"
            try:
                logging.debug(f"Fetching family {family_id} from {family_url}")
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(family_url, timeout=30, headers={'User-Agent': USER_AGENT})
                )
                response.raise_for_status()
                family_html = response.text
                logging.debug(f"Successfully fetched family {family_id}")

                # Extract data from JavaScript - find the tameable listview
                data_match = re.search(r'id:\s*[\'"]tameable[\'"].*?data:\s*(\[.*?\])\s*\}\);', family_html, re.DOTALL)
                if not data_match:
                    logging.warning(f"Could not find NPC data in family {family_id} HTML")
                    return

                data_json = data_match.group(1)
                # Fix unquoted keys for JSON parsing - quote unquoted keys after commas
                data_json = re.sub(r',(\w+):', r',"\1":', data_json)
                try:
                    npc_data = json.loads(data_json)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse NPC data JSON for family {family_id}: {e}")
                    logging.debug(f"Data snippet: {data_json[:500]}")
                    return

                logging.debug(f"Found {len(npc_data)} NPCs in family {family_name}")

                for item in npc_data:
                    npc_id = str(item.get('id'))
                    npc_name = item.get('displayName') or item.get('name')
                    if npc_id and npc_name:
                        data.append({
                            'family': family_name,
                            'npc_id': npc_id,
                            'npc_name': npc_name
                        })

            except Exception as e:
                logging.error(f"Failed to fetch family {family_id}: {e}")

    print(f"Starting concurrent family processing with {NPC_CONCURRENCY} workers...\n")
    tasks = [process_family(family_id, family_name) for family_id, family_name in families.items()]
    await asyncio.gather(*tasks)

    # Remove duplicates (key: family + npc_id + npc_name)
    logging.info(f"Found {len(data)} total records before deduplication")

    grouped = {}
    for record in data:
        key = (record['family'], record['npc_id'], record['npc_name'])

        if key not in grouped:
            grouped[key] = {
                'family': record['family'],
                'npc_id': record['npc_id'],
                'npc_name': record['npc_name']
            }

    # Convert to simplified records
    simplified_data = list(grouped.values())

    logging.info(f"Extracted {len(simplified_data)} unique NPC records from Wowhead")
    return simplified_data


async def extract_wowhead_data(url):
    """
    Extract NPC data from Wowhead hunter-pets and family pages
    Step 1: Parse pet types data from hunter-pets page to get families
    Step 2: Get NPCs from each family page
    Returns list of {'family': str, 'npc_id': str, 'npc_name': str}
    """
    families = await extract_families(url)
    npcs = await extract_npcs_from_families(families)
    return npcs


def export_to_lua(records):
    """Export records to Lua format grouped by Family -> NPCs"""
    logging.info("Exporting to Lua format with hierarchical grouping...")

    # Group by family -> npc_id -> display_ids
    family_data = {}
    for record in records:
        family = record['family']
        npc_id = record['npc_id']
        npc_name = record['npc_name']
        display_id = record['display_id']

        if family not in family_data:
            family_data[family] = {}

        if npc_id not in family_data[family]:
            family_data[family][npc_id] = {
                'name': npc_name,
                'display_ids': set()
            }

        if display_id:
            family_data[family][npc_id]['display_ids'].add(display_id)

    # Generate Lua content
    lua_content = """-- Models Data Export
-- Generated automatically
-- Hierarchical format: Family -> NPCs

ModelsData = {
"""

    for family in sorted(family_data.keys()):
        lua_content += f'    ["{family}"] = {{\n'

        for npc_id in sorted(family_data[family].keys(), key=int):
            npc_data = family_data[family][npc_id]

            display_ids = sorted([int(did) for did in npc_data['display_ids'] if did])
            display_ids_str = ", ".join(str(did) for did in display_ids) if display_ids else ""

            lua_content += f"""        [{npc_id}] = {{
            name = "{npc_data['name'].replace('"', '\\"')}",
            display_ids = {{{display_ids_str}}}
        }},
"""

        lua_content += '    },\n'

    lua_content += "}\n"

    # Write to file
    lua_path = os.path.join(OUTPUT_DIR, 'ModelsData.lua')
    ensure_parent_dir(lua_path)
    with open(lua_path, 'w', encoding='utf-8') as f:
        f.write(lua_content)

    logging.info("Hierarchical Lua data exported to Output/ModelsData.lua")
    total_npcs = sum(len(family_data[family]) for family in family_data)
    logging.info(f"Total NPCs: {total_npcs}")


def apply_found_display_ids_to_records(merged_records, found_display_ids):
    """Apply previously found display IDs to current merged records"""
    normalized = {}
    for k, v in (found_display_ids or {}).items():
        key = str(k)
        if isinstance(v, list):
            ids = [str(x) for x in v if str(x) and str(x) != '0']
        elif v is None:
            ids = []
        else:
            ids = [str(v)] if str(v) and str(v) != '0' else []
        if ids:
            normalized[key] = sorted(set(ids), key=int)

    if not normalized:
        return merged_records

    new_records = []
    for rec in merged_records:
        npc_id = str(rec.get('npc_id'))
        if not rec.get('display_id') and npc_id in normalized:
            ids = normalized[npc_id]
            base = rec.copy()
            base['display_id'] = ids[0]
            new_records.append(base)
            for did in ids[1:]:
                add = base.copy()
                add['display_id'] = did
                new_records.append(add)
        else:
            new_records.append(rec)
    return new_records


def get_effective_skip():
    """Compute effective skip list (code-defined + file-defined)"""
    file_skips = load_skip_npc_ids()
    return {str(x) for x in SKIP_NPC_IDS} | file_skips


async def fetch_display_ids(merged_records):
    """
    Fetch display IDs for NPCs using optimized requests-based approach
    FINAL OPTIMIZATIONS: Session pooling, multiple strategies, real-time stats
    Returns: (updated_records, failed_npc_details)
    """
    progress_manager = ProgressManager()

    # Load existing progress
    successful_npcs, failed_npcs, found_display_ids = progress_manager.load_progress()

    # Track failed NPC details for reporting
    failed_npc_details = {}  # {npc_id: {'name': str, 'reason': str, 'family': str}}

    # Apply any previously found display IDs
    merged_records[:] = apply_found_display_ids_to_records(merged_records, found_display_ids)

    # Option to reset failed NPCs
    if failed_npcs:
        print(f"Found {len(failed_npcs)} previously failed NPCs")
        try:
            retry_failed = input("Retry failed NPCs (y) or skip them (n)? ").strip().lower()
        except EOFError:
            retry_failed = 'n'
            logging.info("Non-interactive mode: skipping retry of failed NPCs")
        if retry_failed == 'y':
            progress_manager.reset_failed_npcs()
            failed_npcs.clear()

    # Load skip list
    file_skips = load_skip_npc_ids()
    effective_skip = get_effective_skip()
    if file_skips:
        logging.info(f"Loaded {len(file_skips)} NPC IDs to skip from {SKIP_NPC_IDS_FILE}")

    # Find NPCs to fetch
    npcs_to_fetch = []
    for record in merged_records:
        npc_id = str(record['npc_id'])
        if npc_id in effective_skip:
            logging.info(f"Skipping NPC {npc_id} due to skip list")
            continue
        if not record['display_id'] and npc_id not in successful_npcs and npc_id not in failed_npcs:
            npcs_to_fetch.append(record)

    if not npcs_to_fetch:
        logging.info("All NPCs already have display IDs or have been attempted!")
        return merged_records, {}

    print(f"\n{'='*80}")
    print(f"FETCHING DISPLAY IDs FOR {len(npcs_to_fetch)} NPCs")
    print(f"Concurrency: {NPC_CONCURRENCY} | Method: HTTP Requests (Fast!)")
    print(f"{'='*80}\n")

    # Create session for connection pooling (MAJOR PERFORMANCE BOOST!)
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})

    # Track progress
    processed_count = 0
    total_npcs_to_fetch = len(npcs_to_fetch)
    start_time = time.time()
    semaphore = asyncio.Semaphore(NPC_CONCURRENCY)
    successful_updates = []
    save_counter = 0

    async def process_npc(record):
        nonlocal processed_count, save_counter
        async with semaphore:
            npc_id = str(record['npc_id'])
            url = f"https://www.wowhead.com/npc={npc_id}"

            max_retries = 3
            display_ids = set()

            for attempt in range(max_retries):
                try:
                    logging.debug(f"Fetching NPC {npc_id} from {url} (attempt {attempt + 1}/{max_retries})")
                    # Use session with connection pooling for speed
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: session.get(url, timeout=30)
                    )
                    response.raise_for_status()
                    html_content = response.text
                    logging.debug(f"Successfully fetched NPC {npc_id}")

                    # STRATEGY 1: Look for npcmodel (PRIMARY - Most reliable!)
                    matches = re.findall(r'"npcmodel"\s*:\s*(\d+)', html_content)
                    if matches:
                        for did in matches:
                            if did != '0':
                                display_ids.add(did)

                    # STRATEGY 2: Look for displayId in various formats
                    if not display_ids:
                        patterns = [
                            r'["\']?displayId["\']?\s*:\s*(\d+)',
                            r'data-mv-display-id="(\d+)"',
                            r'"display_id"\s*:\s*(\d+)',
                            r'displayid:\s*(\d+)',
                            r'data-display-id="(\d+)"',
                        ]
                        for pattern in patterns:
                            matches = re.findall(pattern, html_content)
                            for did in matches:
                                if did != '0':
                                    display_ids.add(did)

                    if display_ids:
                        # Success!
                        found_display_ids[npc_id] = sorted(display_ids, key=int)
                        successful_npcs.add(npc_id)
                        successful_updates.append((record, sorted(display_ids, key=int)))

                        logging.info(f"✓ NPC {npc_id}: Found {len(display_ids)} display ID(s) - {sorted(display_ids, key=int)}")

                        processed_count += 1
                        save_counter += 1

                        # Save progress every BATCH_SIZE NPCs
                        if save_counter % BATCH_SIZE == 0:
                            progress_manager.save_progress(successful_npcs, failed_npcs, found_display_ids)
                            logging.info(f"💾 Progress saved ({save_counter} NPCs processed)")

                        # Real-time stats every 10 NPCs
                        if processed_count % 10 == 0:
                            elapsed = time.time() - start_time
                            npcs_per_sec = processed_count / elapsed if elapsed > 0 else 0
                            remaining = total_npcs_to_fetch - processed_count
                            eta_minutes = (remaining / npcs_per_sec / 60) if npcs_per_sec > 0 else 0

                            logging.info(f"📊 Progress: {processed_count}/{total_npcs_to_fetch} ({processed_count*100//total_npcs_to_fetch}%) | Speed: {npcs_per_sec:.1f} NPCs/sec | ETA: {eta_minutes:.1f} min")

                        break
                    else:
                        # No display IDs found
                        if attempt == 0:
                            # Save debug HTML on first failure
                            debug_file = f"debug_npc_{npc_id}.html"
                            try:
                                with open(debug_file, 'w', encoding='utf-8') as f:
                                    f.write(html_content)
                                logging.debug(f"💾 Saved {debug_file} for inspection")
                            except:
                                pass

                        logging.warning(f"⚠ NPC {npc_id}: No display IDs found (attempt {attempt + 1}/{max_retries})")

                        if attempt == max_retries - 1:
                            failed_npcs.add(npc_id)
                            # Track failed NPC details for reporting
                            failed_npc_details[npc_id] = {
                                'name': record.get('npc_name', 'Unknown'),
                                'family': record.get('family', 'Unknown'),
                                'reason': f'No display IDs found after {max_retries} attempts'
                            }
                            processed_count += 1
                            save_counter += 1
                        else:
                            await asyncio.sleep(1)

                except Exception as e:
                    if attempt == max_retries - 1:
                        failed_npcs.add(npc_id)
                        # Track failed NPC details for reporting
                        failed_npc_details[npc_id] = {
                            'name': record.get('npc_name', 'Unknown'),
                            'family': record.get('family', 'Unknown'),
                            'reason': f'Error after {max_retries} attempts: {str(e)[:100]}'
                        }
                        logging.error(f"✗ NPC {npc_id}: Error after {max_retries} attempts - {e}")
                        processed_count += 1
                        save_counter += 1
                    else:
                        logging.debug(f"⚠ NPC {npc_id}: Retrying after error (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(1)

    # Process all NPCs concurrently
    print(f"Starting concurrent processing with {NPC_CONCURRENCY} workers...\n")
    tasks = [process_npc(record) for record in npcs_to_fetch]
    await asyncio.gather(*tasks)

    # Close session
    session.close()

    # Apply successful updates to merged_records
    print(f"\nApplying {len(successful_updates)} successful updates to records...")
    for record, display_ids in successful_updates:
        base_idx = merged_records.index(record)
        base = record.copy()
        first_did = display_ids[0]
        base['display_id'] = first_did
        merged_records[base_idx] = base
        for did in display_ids[1:]:
            new_rec = base.copy()
            new_rec['display_id'] = did
            merged_records.insert(base_idx + 1, new_rec)
            base_idx += 1

    # Final progress save
    progress_manager.save_progress(successful_npcs, failed_npcs, found_display_ids)

    # Final stats
    total_time = time.time() - start_time
    avg_speed = processed_count / total_time if total_time > 0 else 0

    print(f"\n{'='*80}")
    print(f"EXTRACTION COMPLETE!")
    print(f"{'='*80}")
    print(f"✓ Successful: {len(successful_npcs)} NPCs")
    print(f"✗ Failed: {len(failed_npcs)} NPCs")
    print(f"⏱ Total time: {total_time/60:.1f} minutes")
    print(f"🚀 Average speed: {avg_speed:.1f} NPCs/second")
    print(f"{'='*80}\n")

    logging.info(f"Completed fetching display IDs for {len(found_display_ids)} NPCs.")
    logging.info(f"Successful: {len(successful_npcs)}, Failed: {len(failed_npcs)}")

    return merged_records, failed_npc_details


async def main():
    logging.info("Wowhead Pet Data Extractor")
    logging.info("=" * 60)

    # Handle reset progress
    if args.reset_progress:
        if os.path.exists(DISPLAY_IDS_PROGRESS_FILE):
            os.remove(DISPLAY_IDS_PROGRESS_FILE)
            logging.info("Progress file reset as requested.")

    # Step 1: Extract families from Wowhead
    print("\n" + "="*80)
    print("EXTRACTING FAMILIES FROM WOWHEAD")
    print("="*80)

    families = load_json(FAMILIES_FILE)
    if families:
        logging.info(f"Loaded existing families data with {len(families)} families")
        try:
            refresh_families = input("Refresh families (y) or use cached (n)? ").strip().lower()
        except EOFError:
            refresh_families = 'n'
            logging.info("Non-interactive mode: using cached families data")
        if refresh_families == 'y':
            families = None

    if families is None:
        families = await extract_families(WOWHEAD_HUNTER_PETS_URL)
        save_json(FAMILIES_FILE, families)

    # Step 2: Extract NPCs from families
    print("\n" + "="*80)
    print("EXTRACTING NPCs FROM FAMILIES")
    print("="*80)

    wowhead_data = load_json(WOWHEAD_PROGRESS_FILE)
    if wowhead_data:
        logging.info(f"Loaded existing NPC data with {len(wowhead_data)} NPCs")
        try:
            refresh_npcs = input("Refresh NPC data (y) or use cached (n)? ").strip().lower()
        except EOFError:
            refresh_npcs = 'n'
            logging.info("Non-interactive mode: using cached NPC data")
        if refresh_npcs == 'y':
            wowhead_data = None

    if wowhead_data is None:
        wowhead_data = await extract_npcs_from_families(families)
        save_json(WOWHEAD_PROGRESS_FILE, wowhead_data)

    # Create unique NPC IDs list
    unique_npc_ids = list(set(record['npc_id'] for record in wowhead_data))
    logging.info(f"Found {len(unique_npc_ids)} unique NPC IDs to process")

    # Create records for display ID fetching (include name and family for failed details)
    npc_records_for_fetching = [{'npc_id': record['npc_id'], 'display_id': '', 'npc_name': record['npc_name'], 'family': record['family']} for record in wowhead_data]

    # Step 2: Fetch display IDs
    print("\n" + "="*80)
    print("FETCHING DISPLAY IDs")
    print("="*80)

    # Check if display IDs progress exists and offer refresh option
    progress_manager = ProgressManager()
    successful_npcs, failed_npcs, found_display_ids = progress_manager.load_progress()
    if found_display_ids:
        try:
            refresh_display = input("Refresh display IDs (y) or use cached (n)? ").strip().lower()
        except EOFError:
            refresh_display = 'n'
            logging.info("Non-interactive mode: using cached display IDs")
        if refresh_display == 'y':
            # Reset progress to start fresh
            successful_npcs.clear()
            failed_npcs.clear()
            found_display_ids.clear()
            progress_manager.save_progress(successful_npcs, failed_npcs, found_display_ids)
            logging.info("Display IDs progress reset. Starting fresh extraction.")

    updated_npc_records, failed_npc_details = await fetch_display_ids(npc_records_for_fetching)

    # Create mapping of npc_id to display_ids
    # The updated_npc_records already has multiple records per NPC (one per display ID)
    # We need to collect ALL display IDs per NPC
    display_ids_map = {}
    for record in updated_npc_records:
        npc_id = str(record['npc_id'])
        display_id = record.get('display_id', '')

        if display_id and display_id != '':
            if npc_id not in display_ids_map:
                display_ids_map[npc_id] = []
            # Avoid duplicates
            if display_id not in display_ids_map[npc_id]:
                display_ids_map[npc_id].append(display_id)

    logging.info(f"Display ID map created for {len(display_ids_map)} NPCs")

    # Apply manual display ID corrections if available
    mapping = load_display_id_mapping()
    if mapping:
        corrected_count = 0
        for npc_id, correct_ids in mapping.items():
            if npc_id in display_ids_map and display_ids_map[npc_id] != correct_ids:
                logging.info(f"Correcting display IDs for NPC {npc_id}: {display_ids_map[npc_id]} -> {correct_ids}")
                corrected_count += 1
            display_ids_map[npc_id] = correct_ids
        if corrected_count:
            logging.info(f"Applied corrections to {corrected_count} NPCs from display ID mapping")
        logging.info(f"Loaded display ID mapping for {len(mapping)} NPCs")

    # Debug: Show NPCs with multiple display IDs
    multi_display_npcs = {npc_id: ids for npc_id, ids in display_ids_map.items() if len(ids) > 1}
    if multi_display_npcs:
        logging.info(f"Found {len(multi_display_npcs)} NPCs with multiple display IDs")

    # Merge display IDs back into wowhead records
    merged_records = []
    for record in wowhead_data:
        npc_id = str(record['npc_id'])
        display_ids = display_ids_map.get(npc_id, [])

        if display_ids:
            # Create one record per display ID
            for display_id in display_ids:
                new_record = record.copy()
                new_record['display_id'] = display_id
                merged_records.append(new_record)
        else:
            # No display IDs found, add with empty display_id
            new_record = record.copy()
            new_record['display_id'] = ''
            merged_records.append(new_record)

    logging.info(f"Merged into {len(merged_records)} records with display IDs")

    # Exclude records with skipped display IDs
    skip_display_ids = load_skip_display_ids()
    if skip_display_ids:
        before = len(merged_records)
        merged_records = [r for r in merged_records if r.get('display_id', '') not in skip_display_ids]
        removed = before - len(merged_records)
        if removed:
            logging.info(f"Excluded {removed} record(s) with skipped display IDs")

    # Exclude NPCs from skip list
    effective_skip = get_effective_skip()
    updated_records = merged_records
    if effective_skip:
        before = len(updated_records)
        updated_records = [r for r in updated_records if str(r.get('npc_id')) not in effective_skip]
        removed = before - len(updated_records)
        if removed:
            logging.info(f"Excluded {removed} NPC record(s) from final output due to skip list")

    # Step 3: Export final data
    print("\n" + "="*80)
    print("EXPORTING FINAL DATA")
    print("="*80)

    # Export CSV
    fieldnames = ['family', 'npc_name', 'npc_id', 'display_id']
    csv_path = os.path.join(OUTPUT_DIR, 'final_pet_data.csv')
    ensure_parent_dir(csv_path)
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_records)

    logging.info(f"✓ CSV exported to {csv_path}")
    logging.info(f"  Total records: {len(updated_records)}")
    logging.info(f"  Unique NPCs: {len(set(record['npc_id'] for record in updated_records))}")

    # Export Lua
    export_to_lua(updated_records)

    # Report failed NPCs if any
    if failed_npc_details:
        print(f"\n❌ FAILED NPCs ({len(failed_npc_details)} total):")
        print("-" * 50)
        for npc_id, details in list(failed_npc_details.items())[:10]:  # Show first 10
            print(f"   NPC {npc_id}: {details['name']} ({details['family']}) - {details['reason']}")
        if len(failed_npc_details) > 10:
            print(f"   ... and {len(failed_npc_details) - 10} more failed NPCs")
        print(f"   💾 Full failed NPC list saved to: {os.path.join(OUTPUT_DIR, 'failed_npcs.json')}")

        # Save failed NPCs to a JSON file for reference
        failed_npcs_file = os.path.join(OUTPUT_DIR, 'failed_npcs.json')
        save_json(failed_npcs_file, failed_npc_details)
        print(f"   ✅ Failed NPCs list saved to: {failed_npcs_file}")

    print("\n" + "="*80)
    print("EXTRACTION COMPLETE!")
    print("="*80)
    print(f"Output files created in '{OUTPUT_DIR}' directory")
    if failed_npc_details:
        print(f"Failed NPCs report available in failed_npcs.json")
    print("="*80)


if __name__ == '__main__':
    parser = ArgumentParser(description='Wowhead Pet Data Extractor')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    parser.add_argument('--concurrency', type=int, default=NPC_CONCURRENCY, help='Number of concurrent requests')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Batch size for progress saving')
    parser.add_argument('--output-dir', default=OUTPUT_DIR, help='Output directory')
    parser.add_argument('--cache-dir', default=CACHE_DIR, help='Cache directory')
    parser.add_argument('--reset-progress', action='store_true', help='Reset progress and start fresh')
    args = parser.parse_args()

    # Update constants based on arguments
    NPC_CONCURRENCY = args.concurrency
    BATCH_SIZE = args.batch_size
    OUTPUT_DIR = args.output_dir
    CACHE_DIR = args.cache_dir

    # Update file paths
    FAMILIES_FILE = os.path.join(CACHE_DIR, 'families.json')
    WOWHEAD_PROGRESS_FILE = os.path.join(CACHE_DIR, 'wowhead_data.json')
    DISPLAY_IDS_PROGRESS_FILE = os.path.join(CACHE_DIR, 'display_ids_progress.json')
    SKIP_NPC_IDS_FILE = os.path.join(CACHE_DIR, 'skip_npc_ids.json')
    DISPLAY_ID_MAPPING_FILE = os.path.join(CACHE_DIR, 'correct_display_ids.json')
    SKIP_DISPLAY_IDS_FILE = os.path.join(CACHE_DIR, 'skip_display_ids.json')

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting gracefully.")
        sys.exit(1)