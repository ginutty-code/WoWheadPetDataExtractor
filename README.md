# Wowhead Pet Data Extractor

A Python script that extracts pet data from Wowhead, including families, NPCs, and display IDs.

## Features

- Extracts pet families from Wowhead hunter-pets page
- Fetches tameable NPCs from each family page
- Retrieves display IDs for individual NPCs
- Exports data to CSV and Lua formats
- Supports caching and progress resumption
- Configurable concurrency and retry logic

## Requirements

- Python 3.7+
- requests
- beautifulsoup4

Install dependencies:
```bash
pip install requests beautifulsoup4
```

## Usage

### Basic Usage

```bash
python wowhead_pet_data_extractor.py
```

### Options

- `--verbose`: Enable debug logging
- `--concurrency CONCURRENCY`: Number of concurrent requests (default: 25)
- `--batch-size BATCH_SIZE`: Batch size for progress saving (default: 100)
- `--output-dir OUTPUT_DIR`: Output directory (default: Output)
- `--cache-dir CACHE_DIR`: Cache directory (default: Cache)
- `--reset-progress`: Reset progress and start fresh

### Example

```bash
python wowhead_pet_data_extractor.py --verbose --concurrency 5
```

## Output

The script generates:

- `Output/final_pet_data.csv`: CSV file with pet data
- `Output/ModelsData.lua`: Lua file with hierarchical pet data
- `Output/failed_npcs.json`: Report of failed NPCs (if any)

## Cache Files

- `Cache/families.json`: Cached pet families
- `Cache/wowhead_data.json`: Cached NPC data
- `Cache/display_ids_progress.json`: Progress and found display IDs

## Configuration

### Skip Lists

Create JSON files in the Cache directory to skip certain NPCs or display IDs:

- `skip_npc_ids.json`: NPCs to skip (array of IDs or {"skip": [ids]})
- `skip_display_ids.json`: Display IDs to exclude from output (array of IDs or {"skip": [ids]})

### Manual Display ID Mappings

- `correct_display_ids.json`: Manual corrections for display IDs ({"npc_id": ["display_id1", "display_id2"]})

## Non-Interactive Mode

The script handles non-interactive environments (e.g., IDEs) by defaulting to using cached data and skipping refreshes.

## Troubleshooting

- If you encounter timeouts, reduce `--concurrency`
- Check network connectivity to wowhead.com
- Use `--verbose` for detailed logging
- Reset progress with `--reset-progress` if needed

## License

This project is for educational purposes. Respect Wowhead's terms of service.
