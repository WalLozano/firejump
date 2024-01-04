import os
from flask import Flask
from config import config

#Routes 
from routes import Movie 

app = Flask(__name__)

def page_not_found(error):
    return "<h1>Oops, nothing here</h1>", 404

if __name__ == '__main__':
    app.config.from_object(config['development'])

    # Blueprints
    app.register_blueprint(Movie.main, url_prefix='/api/movies')

    # Error handler
    app.register_error_handler(404, page_not_found)


    app.run()