import io
import logging
import numpy as np
import os
import rasterio
import threading
import sys
import time
import traceback

from random import randint
from gevent.pywsgi import WSGIServer
from PIL import Image, ImageDraw, ImageFont
from flask import Flask, make_response, abort, request


def respond_with_image(image):
    bio = io.BytesIO()
    image.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    return response

def make_tile_server(port_coordination, fn):
    '''
    Makes a tile server and starts it on the given port, using a function
    that takes z, x, y as the tile route.
    '''
    app = Flask(__name__)
    http_server = None

    sys.stderr = open(os.devnull, "w")

    def shutdown1():
        time.sleep(0.5)
        http_server.stop()

    @app.route('/shutdown')
    def shutdown2():
        try:
            t = threading.Thread(target=shutdown1)
            t.start()
            # Do not return a response, as this causes odd issues.
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    @app.route('/handshake')
    def handshake():
        return port_coordination['handshake']

    @app.route("/tile/<int:z>/<int:x>/<int:y>.png")
    def tile(z, x, y):
        try:
            return fn(z, x, y)
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    port_coordination['port'] = -1
    while port_coordination['port'] < 0:
        try:
            port_coordination['port'] = randint(49152, 65535)
            http_server = WSGIServer(('', port_coordination['port']), app)
            return http_server.serve_forever()
        except:
            port_coordination['port'] = -1

def rdd_server(port, pyramid, render_tile):
    def tile(z, x, y):

        # fetch data
        rdd = pyramid[z]
        tile = rdd.lookup(col=x, row=y)

        arr = tile[0]['data']

        if arr == None:
            abort(404)

        image = render_tile(arr)

        return respond_with_image(image)

    return make_tile_server(port, tile)

def catalog_layer_server(port, value_reader, layer_name, key_type, render_tile):
    from geopyspark.geotrellis.protobufcodecs import multibandtile_decoder

    def tile(z, x, y):
        tile = value_reader.readTile(key_type,
                                     layer_name,
                                     z,
                                     x,
                                     y,
                                     "")
        if not tile:
            abort(404)

        arr = multibandtile_decoder(tile)['data']
        image = render_tile(arr)

        return respond_with_image(image)

    return make_tile_server(port, tile)

def catalog_multilayer_server(port, value_reader, layer_names, key_type, render_tile):
    from geopyspark.geotrellis.protobufcodecs import multibandtile_decoder

    def tile(z, x, y):
        tiles = []

        for layer_name in layer_names:
            value = value_reader.readTile(key_type,
                                          layer_name,
                                          z,
                                          x,
                                          y,
                                          "")
            if not value:
                abort(404)

            tile = multibandtile_decoder(value)['data']
            tiles.append(tile)

        image = render_tile(tiles)

        return respond_with_image(image)

    return make_tile_server(port, tile)

