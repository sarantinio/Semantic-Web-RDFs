from Services.DBPedia import DBPedia
from Services.Stardog import Stardog
from Services.TheMovieDatabase import TheMovieDatabase
import logging
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    action = "lmdb_actor_to_dbpedia"

    if action == "discover":
        with TheMovieDatabase() as tmdb:
            tmdb.discover("movie", "2000", "2018")

    if action == "clean_movie_dbpedia_references":
        stardog = Stardog()
        uris = stardog.get_movies()
        cleaned_uris = stardog.clean_movie_uris(uris)

    if action == "clean_actor_dbpedia_references":
        stardog = Stardog()
        uris = stardog.get_actors()
        cleaned_uris = stardog.clean_actor_uris(uris)

    if action == "remove_bad_movie_refs":
        stardog = Stardog()
        stardog.remove_movie_bad_uris()

    if action == "remove_bad_actor_refs":
        stardog = Stardog()
        stardog.remove_actor_bad_uris()

    if action == "lmdb_movies_to_dbpedia":
        stardog = Stardog()
        dbpedia = DBPedia()
        uris = stardog.get_movies()
        response = dbpedia.match_movies(uris)

    if action == "lmdb_actor_to_dbpedia":
        stardog = Stardog()
        dbpedia = DBPedia()
        uris = stardog.get_actors()
        response = dbpedia.match_actors(uris)
