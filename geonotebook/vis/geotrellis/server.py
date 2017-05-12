import io
import numpy as np
import rasterio
import threading
import time

from PIL import Image, ImageDraw, ImageFont

def make_image(arr):
    return Image.fromarray(arr.astype('uint8')).convert('L')

def clamp(x):
    if (x < 0.0):
        x = 0
    elif (x >= 1.0):
        x = 255
    else:
        x = (int)(x * 255)
    return x

def alpha(x):
    if ((x <= 0.0) or (x > 1.0)):
        return 0
    else:
        return 255

clamp = np.vectorize(clamp)
alpha = np.vectorize(alpha)
lock = threading.Lock()

def moop(png, port):
    from flask import Flask, make_response, abort

    app = Flask(__name__)
    app.reader = None

    @app.route("/time")
    def ping():
        return time.strftime("%H:%M:%S") + "\n"

    @app.route("/tile/<layer_name>/<int:x>/<int:y>/<int:zoom>.png")
    def tile(layer_name, x, y, zoom):

        # fetch data
        try:
            img = png.lookup(x, y, zoom)
        except:
            img = None

        if img == None or len(img) == 0:
            if png.debug:
                image = Image.new('RGBA', (256,256))
                draw = ImageDraw.Draw(image)
                draw.rectangle([0, 0, 255, 255], outline=(255,0,0,255))
                draw.line([(0,0),(255,255)], fill=(255,0,0,255))
                draw.line([(0,255),(255,0)], fill=(255,0,0,255))
                draw.text((136,122), str(x) + ', ' + str(y) + ', ' + str(zoom), fill=(255,0,0,255))
                del draw
                bio = io.BytesIO()
                image.save(bio, 'PNG')
                img = [bio.getvalue()]
            else:
                abort(404)

        response = make_response(img[0])
        response.headers['Content-Type'] = 'image/png'
        return response

    app.run(host='0.0.0.0', port=port)
