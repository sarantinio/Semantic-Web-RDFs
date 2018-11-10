from SPARQLWrapper import SPARQLWrapper, JSON, POST
from typing import Dict, Tuple
from SPARQL import Queries
from rdflib import Graph, URIRef, namespace
import urllib.parse


class Stardog:
    __endpoint = None
    __sparql = None

    def __init__(self):
        self.__endpoint = "http://localhost:5820/BestLinkedMovieDatabase/query"
        self.__sparql = SPARQLWrapper(self.__endpoint)
        self.__sparql.setReturnFormat(JSON)
        self.__sparql.setMethod(POST)

    def get_movies(self) -> Tuple:
        self.__sparql.setQuery(Queries.lmdb_movies_to_dbpedia_uris())
        response = self.__sparql.query().convert()
        uris = list(map(Stardog.__to_uri, response["results"]["bindings"]))

        return tuple(tuple(uris[i:i + 50]) for i in range(0, len(uris), 50))

    def get_actors(self) -> Tuple:
        self.__sparql.setQuery(Queries.lmdb_actors_to_dbpedia_uris())
        response = self.__sparql.query().convert()
        uris = list(map(Stardog.__to_uri, response["results"]["bindings"]))

        return tuple(tuple(uris[i:i + 100]) for i in range(0, len(uris), 100))

    def clean_movie_uris(self, uris) -> Graph or None:
        cleaned_graph = Graph()
        self.__sparql.setQuery(Queries.lmdb_movies_to_dbpedia_movie_uri_pairs())
        response = self.__sparql.query().convert()
        pairs = list(map(Stardog.__to_movie_clean_uri_pair, response["results"]["bindings"]))

        if len(pairs) == 0:
            return None

        for lmdb_resource, dbp_uri in pairs:
            lmdb_resource = URIRef(lmdb_resource)
            dbp_uri = URIRef(dbp_uri.replace('"', "").replace("'", "").replace("`", ""))
            cleaned_graph.add((lmdb_resource, namespace.OWL.sameAs, dbp_uri))
        cleaned_graph.serialize('Data/CleanedLMDBMovieSameAs.ttl', format='turtle')

        return cleaned_graph # It is now safe to import the cleaned data using stardog.

    def clean_actor_uris(self, uris) -> Graph or None:
        cleaned_graph = Graph()
        self.__sparql.setQuery(Queries.lmdb_actors_to_dbpedia_movie_uri_pairs())
        response = self.__sparql.query().convert()
        pairs = list(map(Stardog.__to_movie_clean_uri_pair, response["results"]["bindings"]))

        if len(pairs) == 0:
            return None

        for lmdb_resource, dbp_uri in pairs:
            lmdb_resource = URIRef(lmdb_resource)
            dbp_uri = URIRef(dbp_uri.replace('"', "").replace("'", "").replace("`", ""))
            cleaned_graph.add((lmdb_resource, namespace.OWL.sameAs, dbp_uri))
        cleaned_graph.serialize('Data/CleanedLMDBActorSameAs.ttl', format='turtle')

        return cleaned_graph # It is now safe to import the cleaned data using stardog.

    def remove_movie_bad_uris(self):
        self.__sparql = SPARQLWrapper('http://localhost:5820/BestLinkedMovieDatabase/update')
        self.__sparql.setMethod(POST)
        self.__sparql.setReturnFormat(JSON)
        self.__sparql.setQuery(Queries.remove_bad_movie_dbp_refs())
        self.__sparql.query()

    def remove_actor_bad_uris(self):
        self.__sparql = SPARQLWrapper('http://localhost:5820/BestLinkedMovieDatabase/update')
        self.__sparql.setMethod(POST)
        self.__sparql.setReturnFormat(JSON)
        self.__sparql.setQuery(Queries.remove_bad_actor_dbp_refs())
        self.__sparql.query()

    @staticmethod
    def __to_uri(resource: Dict) -> str:
        return """<""" + urllib.parse.unquote(resource['uri']['value'], encoding='utf8', errors='strict') + """>"""

    @staticmethod
    def __to_movie_clean_uri_pair(resource: Dict) -> Tuple:
        return (urllib.parse.unquote(resource['sub']['value'], encoding='utf8', errors='strict'),
                urllib.parse.unquote(resource['uri']['value'], encoding='utf8', errors='strict'))
