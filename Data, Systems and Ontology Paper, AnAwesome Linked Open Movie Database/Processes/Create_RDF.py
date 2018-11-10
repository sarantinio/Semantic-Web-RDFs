def create_rdf(movie_name, movie_year,avg_vote,popularity,vote_count,tmdb_id,tmdb_title,apikey="2630f1b6",prev_graph=0):
    #Make the request for each film
    import requests
    import logging
    import sys

    omdb_api_url = "http://www.omdbapi.com/?apikey=" + apikey;

    params = {
        't': movie_name,
        'plot': 'short',
        'r': 'json',
        'y': movie_year
    }
    try:
        server_response = requests.get(omdb_api_url, params=params)
        response = server_response.json()

        if (response['Response'] == "False"):
            return 0
    except:
        e = sys.exc_info()[0]
        print(e);
        logging.info(e)
        return 0



    ######################################
    # create an RDF graph

    import rdflib
    from rdflib import Graph, Namespace, URIRef, Literal, XSD, RDF
    g = Graph()

    #Type of IMDB
    IMDB = Namespace("http://imdb.com/")
    g.bind('imdb', IMDB)

    #Type of TMDB
    TMDB = Namespace("https://www.themoviedb.org/movie/")
    g.bind('tmdb', TMDB)

    ######################################

    # Initiliaze URI

    fear_URI = IMDB[response['imdbID']]

    tmdb_URI = TMDB[tmdb_id]


    # print "Created {}".format(fear_URI)

    #print(fear_URI)

    ######################################

    # Add the triples.
    # Every result we get is of type 'imdb:Movie'
    g.add((fear_URI, RDF.type, IMDB['Movie']))

    #and tmdb type
    g.add((tmdb_URI, RDF.type, TMDB['movie']))

    g.add((tmdb_URI, TMDB['avg_vote'], Literal(avg_vote)))
    g.add((tmdb_URI, TMDB['popularity'], Literal(popularity)))

    g.add((tmdb_URI, TMDB['vote_count'], Literal(vote_count)))
    g.add((tmdb_URI, TMDB['title'], Literal(tmdb_title)))


    # The value for imdb:title is an english language literal (mind the double brackets)
    g.add((fear_URI, IMDB['title'], Literal(response['Title'], lang='en')))

    # The value for imdb:actor, is for each actor a Literal string
    # (though we could have generated URIs for every actore)

    # Split the 'Actors' value by comma, and then strip every element of trailing spaces:
    actors = [a.strip() for a in response['Actors'].split(',')]

    # Iterate over the 'actors' we found
    for actor in actors:
        g.add((fear_URI, IMDB['actor'], Literal(actor)))

    # The language is again an english language literal
    g.add((fear_URI, IMDB['language'], Literal(response['Language'], lang='en')))

    # The runtime should be an XSD duration, but we have to strip the 'min' part, and replace it with 'M'
    # See e.g. <http://www.w3schools.com/xml/schema_dtypes_date.asp>
    duration = response['Runtime'].replace(' min', 'M')
    g.add((fear_URI, IMDB['runtime'], Literal(duration, datatype=XSD['duration'])))

    # The genres are again a comma separated list. They could be
    genres = [genre.strip() for genre in response['Genre'].split(',')]
    for genre in genres:
        g.add((fear_URI, IMDB['genre'], Literal(genre, lang='en')))

    # The rating is a literal value
    g.add((fear_URI, IMDB['rated'], Literal(response['Rated'])))

    # The writers are a comma separated list, so here we go again:
    writers = [w.strip() for w in response['Writer'].split(',')]
    for writer in writers:
        g.add((fear_URI, IMDB['writer'], Literal(writer)))

    # Split the 'Actors' value by comma, and then strip every element of trailing spaces:
    directors = [a.strip() for a in response['Director'].split(',')]

    # Iterate over the 'actors' we found
    for director in directors:
        # The director is a single literal in this case (but perhaps there could be more???):
        g.add((fear_URI, IMDB['director'], Literal(director)))

    # The plot is an english literal
    g.add((fear_URI, IMDB['plot'], Literal(response['Plot'], lang='en')))

    # The year is an XSD gYear
    g.add((fear_URI, IMDB['year'], Literal(response['Year'], datatype=XSD.gYear)))

    # The IMDB rating is a double
    g.add((fear_URI, IMDB['rating'], Literal(response['imdbRating'], datatype=XSD.double)))

    # The poster is a URL
    g.add((fear_URI, IMDB['poster'], URIRef(response['Poster'])))

    ######################################

    #print(g.serialize(format='turtle'))

    return g


