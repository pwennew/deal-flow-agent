# Deal Flow Agent v3

Automated carve-out and spin-off intelligence scanner.

## Changes in v3
- Fixed deduplication: now queries Notion database at start of each run
- Normalizes titles before comparison (strips PE buyer suffixes)
- Uses requests library directly for more reliable API calls

## Setup
1. Upload all files to GitHub repo
2. Add secrets in GitHub repo settings:
   - NOTION_API_KEY
   - NOTION_DATABASE_ID  
   - ANTHROPIC_API_KEY

## Manual run
Actions tab → Daily Deal Flow Scan → Run workflow
