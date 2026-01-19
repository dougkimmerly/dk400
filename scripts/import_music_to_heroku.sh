#!/bin/bash
# Import music_library dump into Heroku's MUSICLIB schema
#
# Usage:
#   1. Export from Synology:
#      ssh doug@192.168.20.16 "sudo /usr/local/bin/docker exec music-library-postgres pg_dump -U music music_library" > music_library_backup.sql
#
#   2. Transform and import to Heroku:
#      ./scripts/import_music_to_heroku.sh music_library_backup.sql
#
# This script:
#   - Replaces 'public' schema references with 'musiclib'
#   - Imports into your Heroku database

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <music_library_backup.sql>"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="${INPUT_FILE%.sql}_musiclib.sql"

echo "Transforming $INPUT_FILE -> $OUTPUT_FILE"

# Transform schema references from public to musiclib
sed -e 's/public\./musiclib./g' \
    -e 's/SCHEMA public/SCHEMA musiclib/g' \
    -e 's/TO public/TO musiclib/g' \
    -e 's/search_path = public/search_path = musiclib/g' \
    "$INPUT_FILE" > "$OUTPUT_FILE"

echo "Created $OUTPUT_FILE"
echo ""
echo "To import to Heroku:"
echo "  heroku pg:psql -a YOUR_APP_NAME < $OUTPUT_FILE"
echo ""
echo "Or to import to local PostgreSQL:"
echo "  psql -h localhost -U dk400 -d dk400 < $OUTPUT_FILE"
