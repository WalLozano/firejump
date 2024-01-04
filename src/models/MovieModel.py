from database.db import get_connection
from .entities.Movie import Movie


class MovieModel():

    # Get all movies

    @classmethod
    def get_movies(self):
        try:
            connection = get_connection()
            movies = []

            with connection.cursor() as cursor:
                cursor.execute(
                    'SELECT id, title, duration, released FROM public."Movies" ORDER BY title ASC')
                resultset = cursor.fetchall()

                for row in resultset:
                    movie = Movie(row[0], row[1], row[2], row[3])
                    movies.append(movie.to_JSON())

            connection.close()
            return movies

        except Exception as ex:
            raise Exception(ex)

# Get one movie

    @classmethod
    def get_movie(self, id):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute(
                    'SELECT id, title, duration, released FROM public."Movies" WHERE id = %s', (id,))
                row = cursor.fetchone()

                movie = None
                if row != None:
                    movie = Movie(row[0], row[1], row[2], row[3])
                    movie = movie.to_JSON()
            connection.close()
            return movie
        except Exception as ex:
            raise Exception(ex)

# Add one movie
    @classmethod
    def add_movie(self, movie):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO public."Movies" (id, title, duration, released)
                            VALUES (%s, %s, %s, %s)""", (movie.id, movie.title, movie.duration, movie.released))
                affected_rows = cursor.rowcount
                connection.commit()

            connection.close()
            return affected_rows
        except Exception as ex:
            raise Exception(ex)

# Update one movie
    @classmethod
    def update_movie(self, movie):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute("""UPDATE public."Movies" SET title = %s, duration = %s, released = %s
                            WHERE id = %s""", (movie.title, movie.duration, movie.released, movie.id,))
                affected_rows = cursor.rowcount
                connection.commit()

            connection.close()
            return affected_rows
        except Exception as ex:
            raise Exception(ex)

# Delete one movie
    @classmethod
    def delete_movie(self, movie):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute(
                    'DELETE FROM public."Movies" WHERE id = %s', (movie.id,))
                affected_rows = cursor.rowcount
                connection.commit()

            connection.close()
            return affected_rows
        except Exception as ex:
            raise Exception(ex)
