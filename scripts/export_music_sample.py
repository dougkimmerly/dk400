#!/usr/bin/env python3
"""
Export sample music data from Synology PostgreSQL to SQL file for Heroku import.

Usage:
    python scripts/export_music_sample.py [--limit 1000] [--output data/musiclib_sample.sql]

Source: Synology PostgreSQL (192.168.20.16:5433, music_library database)
Target: MUSICLIB schema in DK/400 Heroku deployment
"""
import os
import sys
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor

# Source database (Synology music-library-postgres)
SOURCE_CONFIG = {
    'host': os.environ.get('MUSIC_DB_HOST', '192.168.20.16'),
    'port': int(os.environ.get('MUSIC_DB_PORT', 5433)),
    'dbname': os.environ.get('MUSIC_DB_NAME', 'music_library'),
    'user': os.environ.get('MUSIC_DB_USER', 'music'),
    'password': os.environ.get('MUSIC_DB_PASSWORD', 'musiclib123'),
}


def escape_sql_string(s):
    """Escape a string for SQL insertion."""
    if s is None:
        return 'NULL'
    # Escape single quotes by doubling them
    escaped = str(s).replace("'", "''")
    return f"'{escaped}'"


def export_tracks(cursor, limit: int) -> list[str]:
    """Export tracks table with a sample of records."""
    cursor.execute(f"""
        SELECT id, title, artist, album, genre, year, duration_ms,
               play_count, rating, date_added, last_played, file_path, source
        FROM tracks
        ORDER BY RANDOM()
        LIMIT {limit}
    """)

    rows = cursor.fetchall()
    statements = []

    for row in rows:
        values = [
            str(row['id']),
            escape_sql_string(row['title']),
            escape_sql_string(row['artist']),
            escape_sql_string(row['album']),
            escape_sql_string(row['genre']),
            str(row['year']) if row['year'] else 'NULL',
            str(row['duration_ms']) if row['duration_ms'] else 'NULL',
            str(row['play_count']) if row['play_count'] else '0',
            str(row['rating']) if row['rating'] else 'NULL',
            escape_sql_string(row['date_added']),
            escape_sql_string(row['last_played']),
            escape_sql_string(row['file_path']),
            escape_sql_string(row['source']),
        ]
        statements.append(
            f"INSERT INTO musiclib.tracks (id, title, artist, album, genre, year, duration_ms, "
            f"play_count, rating, date_added, last_played, file_path, source) VALUES "
            f"({', '.join(values)});"
        )

    return statements, [row['id'] for row in rows]


def export_artists(cursor, track_ids: list[int]) -> list[str]:
    """Export artists that have tracks in our sample."""
    # Get unique artists from our track sample
    placeholders = ','.join(['%s'] * len(track_ids))
    cursor.execute(f"""
        SELECT DISTINCT artist FROM tracks WHERE id IN ({placeholders}) AND artist IS NOT NULL
    """, track_ids)

    artist_names = [row['artist'] for row in cursor.fetchall()]

    if not artist_names:
        return []

    # Get artist records
    placeholders = ','.join(['%s'] * len(artist_names))
    cursor.execute(f"""
        SELECT id, name, track_count, total_plays, genres
        FROM artists
        WHERE name IN ({placeholders})
    """, artist_names)

    rows = cursor.fetchall()
    statements = []

    for row in rows:
        values = [
            str(row['id']),
            escape_sql_string(row['name']),
            str(row['track_count']) if row['track_count'] else '0',
            str(row['total_plays']) if row['total_plays'] else '0',
            escape_sql_string(row['genres']),
        ]
        statements.append(
            f"INSERT INTO musiclib.artists (id, name, track_count, total_plays, genres) VALUES "
            f"({', '.join(values)});"
        )

    return statements


def export_playlists(cursor, limit: int = 20) -> tuple[list[str], list[int]]:
    """Export playlists."""
    cursor.execute(f"""
        SELECT id, name, description, is_smart, created_at, updated_at
        FROM playlists
        ORDER BY id
        LIMIT {limit}
    """)

    rows = cursor.fetchall()
    statements = []
    playlist_ids = []

    for row in rows:
        playlist_ids.append(row['id'])
        values = [
            str(row['id']),
            escape_sql_string(row['name']),
            escape_sql_string(row['description']),
            'TRUE' if row['is_smart'] else 'FALSE',
            escape_sql_string(row['created_at']),
            escape_sql_string(row['updated_at']),
        ]
        statements.append(
            f"INSERT INTO musiclib.playlists (id, name, description, is_smart, created_at, updated_at) VALUES "
            f"({', '.join(values)});"
        )

    return statements, playlist_ids


def export_playlist_tracks(cursor, playlist_ids: list[int], track_ids: list[int]) -> list[str]:
    """Export playlist_tracks for our sample playlists and tracks."""
    if not playlist_ids or not track_ids:
        return []

    pl_placeholders = ','.join(['%s'] * len(playlist_ids))
    tr_placeholders = ','.join(['%s'] * len(track_ids))

    cursor.execute(f"""
        SELECT id, playlist_id, track_id, position, added_at
        FROM playlist_tracks
        WHERE playlist_id IN ({pl_placeholders}) AND track_id IN ({tr_placeholders})
    """, playlist_ids + track_ids)

    rows = cursor.fetchall()
    statements = []

    for row in rows:
        values = [
            str(row['id']),
            str(row['playlist_id']),
            str(row['track_id']),
            str(row['position']),
            escape_sql_string(row['added_at']),
        ]
        statements.append(
            f"INSERT INTO musiclib.playlist_tracks (id, playlist_id, track_id, position, added_at) VALUES "
            f"({', '.join(values)});"
        )

    return statements


def export_audio_features(cursor, track_ids: list[int]) -> list[str]:
    """Export audio features for our sample tracks."""
    if not track_ids:
        return []

    placeholders = ','.join(['%s'] * len(track_ids))
    cursor.execute(f"""
        SELECT id, track_id, energy, danceability, valence, tempo, acousticness, instrumentalness
        FROM audio_features
        WHERE track_id IN ({placeholders})
    """, track_ids)

    rows = cursor.fetchall()
    statements = []

    for row in rows:
        values = [
            str(row['id']),
            str(row['track_id']),
            str(row['energy']) if row['energy'] is not None else 'NULL',
            str(row['danceability']) if row['danceability'] is not None else 'NULL',
            str(row['valence']) if row['valence'] is not None else 'NULL',
            str(row['tempo']) if row['tempo'] is not None else 'NULL',
            str(row['acousticness']) if row['acousticness'] is not None else 'NULL',
            str(row['instrumentalness']) if row['instrumentalness'] is not None else 'NULL',
        ]
        statements.append(
            f"INSERT INTO musiclib.audio_features (id, track_id, energy, danceability, valence, tempo, acousticness, instrumentalness) VALUES "
            f"({', '.join(values)});"
        )

    return statements


def main():
    parser = argparse.ArgumentParser(description='Export music sample for Heroku')
    parser.add_argument('--limit', type=int, default=1000, help='Number of tracks to export')
    parser.add_argument('--output', type=str, default='data/musiclib_sample.sql', help='Output SQL file')
    args = parser.parse_args()

    print(f"Connecting to {SOURCE_CONFIG['host']}:{SOURCE_CONFIG['port']}/{SOURCE_CONFIG['dbname']}...")

    try:
        conn = psycopg2.connect(**SOURCE_CONFIG, cursor_factory=RealDictCursor)
        cursor = conn.cursor()

        print(f"Exporting {args.limit} tracks...")
        track_stmts, track_ids = export_tracks(cursor, args.limit)
        print(f"  - {len(track_stmts)} tracks")

        print("Exporting related artists...")
        artist_stmts = export_artists(cursor, track_ids)
        print(f"  - {len(artist_stmts)} artists")

        print("Exporting playlists...")
        playlist_stmts, playlist_ids = export_playlists(cursor)
        print(f"  - {len(playlist_stmts)} playlists")

        print("Exporting playlist tracks...")
        playlist_track_stmts = export_playlist_tracks(cursor, playlist_ids, track_ids)
        print(f"  - {len(playlist_track_stmts)} playlist tracks")

        print("Exporting audio features...")
        feature_stmts = export_audio_features(cursor, track_ids)
        print(f"  - {len(feature_stmts)} audio features")

        # Write output file
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, 'w') as f:
            f.write("-- MUSICLIB Sample Data Export\n")
            f.write(f"-- Exported {len(track_ids)} tracks from music_library\n")
            f.write("-- Source: Synology PostgreSQL (192.168.20.16:5433)\n\n")

            f.write("-- Clear existing data\n")
            f.write("TRUNCATE musiclib.playlist_tracks, musiclib.audio_features, musiclib.tracks, musiclib.artists, musiclib.playlists RESTART IDENTITY CASCADE;\n\n")

            f.write("-- Artists\n")
            for stmt in artist_stmts:
                f.write(stmt + "\n")

            f.write("\n-- Tracks\n")
            for stmt in track_stmts:
                f.write(stmt + "\n")

            f.write("\n-- Playlists\n")
            for stmt in playlist_stmts:
                f.write(stmt + "\n")

            f.write("\n-- Playlist Tracks\n")
            for stmt in playlist_track_stmts:
                f.write(stmt + "\n")

            f.write("\n-- Audio Features\n")
            for stmt in feature_stmts:
                f.write(stmt + "\n")

            f.write("\n-- Reset sequences\n")
            f.write("SELECT setval('musiclib.tracks_id_seq', (SELECT MAX(id) FROM musiclib.tracks));\n")
            f.write("SELECT setval('musiclib.artists_id_seq', (SELECT MAX(id) FROM musiclib.artists));\n")
            f.write("SELECT setval('musiclib.playlists_id_seq', (SELECT MAX(id) FROM musiclib.playlists));\n")
            f.write("SELECT setval('musiclib.playlist_tracks_id_seq', (SELECT MAX(id) FROM musiclib.playlist_tracks));\n")
            f.write("SELECT setval('musiclib.audio_features_id_seq', (SELECT MAX(id) FROM musiclib.audio_features));\n")

        print(f"\nExported to {args.output}")
        print(f"Total: {len(track_stmts) + len(artist_stmts) + len(playlist_stmts) + len(playlist_track_stmts) + len(feature_stmts)} records")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
