#!/usr/bin/env python

# MBUtil: a tool for MBTiles files
# Supports importing, exporting, and more
#
# (c) Development Seed 2012
# Licensed under BSD

# for additional reference on schema see:
# https://github.com/mapbox/node-mbtiles/blob/master/lib/schema.sql

import sqlite3, sys, logging, time, os, json, zlib, gzip, re, StringIO, math

logger = logging.getLogger(__name__)

def flip_y(zoom, y):
    return (2**zoom-1) - y

def mbtiles_setup(cur):
    cur.execute("""
        create table tiles (
            zoom_level integer,
            tile_column integer,
            tile_row integer,
            tile_data blob);
            """)
    cur.execute("""create table metadata
        (name text, value text);""")
    cur.execute("""create unique index tiles_index on tiles
        (zoom_level, tile_column, tile_row);""")

def mbtiles_connect(mbtiles_file, silent):
    try:
        con = sqlite3.connect(mbtiles_file)
        return con
    except Exception as e:
        if not silent:
            logger.error("Could not connect to database")
            logger.exception(e)
        sys.exit(1)

def optimize_connection(cur):
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("""PRAGMA journal_mode=DELETE""")

def optimize_database(cur, silent):
    if not silent: 
        logger.debug('analyzing db')
    cur.execute("""ANALYZE;""")
    if not silent: 
        logger.debug('cleaning db')

    # Workaround for python>=3.6.0,python<3.6.2
    # https://bugs.python.org/issue28518
    cur.isolation_level = None
    cur.execute("""VACUUM;""")
    cur.isolation_level = ''  # reset default value of isolation_level

def get_dirs(path):
    return [name for name in os.listdir(path)
        if os.path.isdir(os.path.join(path, name))]

def read_tiles(zoom_level, depth, base_tile_id, directory_path, image_format, silent, cur):
    if depth > 0:
        for sub_dir in get_dirs(directory_path):
            idx = int(sub_dir)
            tile_id = base_tile_id + (idx * (10**(depth * 3)))
            read_tiles(zoom_level, depth - 1, tile_id, os.path.join(directory_path, sub_dir), image_format, silent, cur)
    else:
        maxx = 180
        minx = -180
        tile_size = 4
        if zoom_level == 1:
            tile_size = 1
        elif zoom_level == 2:
            tile_size = 0.25
        n_columns = math.ceil((maxx - minx) / tile_size)

        for current_file in os.listdir(directory_path):
            file_name, ext = current_file.split('.', 1)

            if (ext != image_format):
                pass

            f = open(os.path.join(directory_path, current_file), 'rb')
            file_content = f.read()
            f.close()
            tile_id = base_tile_id + int(file_name)
            tile_row = round(tile_id / n_columns)
            tile_col = tile_id % n_columns

            if not silent:
                logger.debug(' Read tile %i with zoom %i (%i, %i)' % (tile_id, zoom_level, tile_col, tile_row))

            blob = StringIO.StringIO()
            with gzip.GzipFile(fileobj=blob, mode="w", compresslevel=6) as f:
                f.write(file_content)
            cur.execute("""insert into tiles (zoom_level,
                tile_column, tile_row, tile_data) values
                (?, ?, ?, ?);""",
                (zoom_level, tile_col, tile_row, sqlite3.Binary(blob.getvalue())))

def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):

    silent = kwargs.get('silent')

    if not silent:
        logger.info("Importing disk to MBTiles")
        logger.debug("%s --> %s" % (directory_path, mbtiles_file))

    con = mbtiles_connect(mbtiles_file, silent)
    cur = con.cursor()
    optimize_connection(cur)
    mbtiles_setup(cur)
    #~ image_format = 'gph'
    image_format = kwargs.get('format', 'gph')

    try:
        metadata = json.load(open(os.path.join(directory_path, 'metadata.json'), 'r'))
        image_format = kwargs.get('format')
        for name, value in metadata.items():
            cur.execute('insert into metadata (name, value) values (?, ?)',
                (name, value))
        if not silent: 
            logger.info('metadata from metadata.json restored')
    except IOError:
        if not silent: 
            logger.warning('metadata.json not found')

    for zoom_dir in get_dirs(directory_path):
        z = int(zoom_dir)
        depth = 2 if (z>=2) else 1
        read_tiles(z, depth, 0, os.path.join(directory_path, zoom_dir), image_format, silent, cur)

    if not silent:
        logger.debug('tiles (and grids) inserted.')

    optimize_database(con, silent)

def mbtiles_metadata_to_disk(mbtiles_file, **kwargs):
    silent = kwargs.get('silent')
    if not silent:
        logger.debug("Exporting MBTiles metatdata from %s" % (mbtiles_file))
    con = mbtiles_connect(mbtiles_file, silent)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    if not silent:
        logger.debug(json.dumps(metadata, indent=2))

def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    silent = kwargs.get('silent')
    if not silent:
        logger.debug("Exporting MBTiles to disk")
        logger.debug("%s --> %s" % (mbtiles_file, directory_path))
    con = mbtiles_connect(mbtiles_file, silent)
    os.mkdir("%s" % directory_path)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)
    count = con.execute('select count(zoom_level) from tiles;').fetchone()[0]
    done = 0
    base_path = directory_path
    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    # if interactivity
    formatter = metadata.get('formatter')
    if formatter:
        layer_json = os.path.join(base_path, 'layer.json')
        formatter_json = {"formatter":formatter}
        open(layer_json, 'w').write(json.dumps(formatter_json))

    tiles = con.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        if kwargs.get('scheme') == 'xyz':
            y = flip_y(z,y)
            if not silent:
                logger.debug('flipping')
            tile_dir = os.path.join(base_path, str(z), str(x))
        elif kwargs.get('scheme') == 'wms':
            tile_dir = os.path.join(base_path,
                "%02d" % (z),
                "%03d" % (int(x) / 1000000),
                "%03d" % ((int(x) / 1000) % 1000),
                "%03d" % (int(x) % 1000),
                "%03d" % (int(y) / 1000000),
                "%03d" % ((int(y) / 1000) % 1000))
        else:
            tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)
        if kwargs.get('scheme') == 'wms':
            tile = os.path.join(tile_dir,'%03d.%s' % (int(y) % 1000, kwargs.get('format', 'png')))
        else:
            tile = os.path.join(tile_dir,'%s.%s' % (y, kwargs.get('format', 'png')))
        f = open(tile, 'wb')
        f.write(t[3])
        f.close()
        done = done + 1
        if not silent:
            logger.info('%s / %s tiles exported' % (done, count))
        t = tiles.fetchone()

    # grids
    callback = kwargs.get('callback')
    done = 0