import os
import pickle
from typing import Iterable

import rdflib
import logging
from Services.ParallelExecutor import ParallelExecutor

logging.basicConfig(level='INFO')


class LMDB:

    def __init__(self, data_path: str, reset: bool = False, graph_only: bool = False):
        self._data_path = data_path
        self._original_file_path = os.path.join(data_path, 'Original.nt')
        self._cleaned_file_path = os.path.join(data_path, 'Cleaned.nt')
        self._malformed_file_path = os.path.join(data_path, 'Malformed.nt')
        self._graph_file_path = os.path.join(data_path, 'Graph.pkl')
        self.reset = reset
        self.graph_only = graph_only

        if os.path.isfile(self._original_file_path):
            if not not self.graph_only:
                self._original_file = open(self._original_file_path, 'r')
                self._original_file_size = sum([1 for _ in self._original_file.readlines()])
                self._original_file.close()
                self._original_file = open(self._original_file_path, 'r')
        else:
            raise Exception(
                'Original file {} does not exist'.format(os.path.join(os.getcwd(), self._original_file_path)))

        if self.reset:
            logging.info('Removing previously processed data.')
            os.remove(self._cleaned_file_path)
            os.remove(self._malformed_file_path)

        self._cleaned_file_size = 0
        if os.path.isfile(self._cleaned_file_path) and not self.graph_only:
            self._cleaned_file = open(self._cleaned_file_path, 'r+')
            self._cleaned_file_size = sum(1 for _ in self._cleaned_file.readlines())
            self._cleaned_file.close()
        self._cleaned_file = open(self._cleaned_file_path, 'ab+')

        self._malformed_file_size = 0
        if os.path.isfile(self._malformed_file_path) and not self.graph_only:
            self._malformed_file = open(self._malformed_file_path, 'r+')
            self._malformed_file_size = sum(1 for _ in self._malformed_file.readlines())
            self._malformed_file.close()
        self._malformed_file = open(self._malformed_file_path, 'ab+')

        if os.path.isfile(self._graph_file_path):
            self._graph_file = open(self._graph_file_path, 'rb')
            self._graph: rdflib.Graph = pickle.load(self._graph_file)
        else:
            logging.info('No graph file to load.')
            self._graph_file = open(self._graph_file_path, 'wb+')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.graph_only:
            self._graph_file.close()
        else:
            self._original_file.close()
            self._cleaned_file.close()
            self._malformed_file.close()
            self._graph_file.close()

    def build_valid_graph(self) -> rdflib.Graph:
        """Build and save a valid graph from the n-tuples dump. Processing is done asynchronously.

        Returns:
            The valid RDFLIB graph.
        """
        valid_graph = rdflib.Graph()
        malformed_triples = []
        logging.info('Chunking {} triples.'.format(self._original_file_size))
        triple_chunks = ParallelExecutor.chunk(100000, self._original_file.readlines())
        logging.info('Finished chunking')
        for part_graph, part_mal_triples in ParallelExecutor.execute(LMDB.clean_triples, triple_chunks):
            valid_graph += part_graph
            malformed_triples += part_mal_triples
            logging.info('Processed {} and skipped {} of {} triples.'.format(len(valid_graph),
                                                                             len(malformed_triples),
                                                                             self._original_file_size))

        valid_graph.serialize(destination=self._cleaned_file_path, format='nt')
        pickle.dump(valid_graph, self._graph_file)
        for triple in malformed_triples:
            self._malformed_file.write(bytes(triple, encoding='utf-8'))

        return valid_graph

    def extract_graph(self, cls: str) -> rdflib.Graph:
        """Extract all triples of a class from the graph to a new graph

        Args:
            cls: The class to extract.

        Returns:
            The new graph.
        """
        results = self._graph.query("""
                    PREFIX lmdbm: <http://data.linkedmdb.org/resource/movie/>
                    SELECT ?sub ?pred ?obj WHERE {{
                        ?sub ?pred ?obj .
                        ?sub rdf:type lmdbm:{} .
                    }}""".format(cls))
        new_graph = rdflib.Graph()
        for result in results:
            new_graph.add(result)

        if cls == 'film':
            pickle.dump(new_graph, open(os.path.join(self._data_path, 'FilmGraph.pkl'), 'wb+'))
        elif cls == 'actor':
            pickle.dump(new_graph, open(os.path.join(self._data_path, 'ActorGraph.pkl'), 'wb+'))

        return new_graph

    @staticmethod
    def clean_triples(triples: Iterable[str]):
        """Parse a list of triples, ignoring any malformed triples.

        Args:
            triples: An iterable of triples.

        Returns:
            The valid graph built from the triples.
        """
        partial_graph = rdflib.Graph()
        partial_malformed_triples = []
        for triple in triples:
            triple = triple.rstrip('\n')
            try:
                valid_triple = rdflib.Graph().parse(data=triple, format='nt')
                partial_graph += valid_triple
            except Exception as e:
                partial_malformed_triples.append(triple + '\n')

        return partial_graph, partial_malformed_triples


if __name__ == '__main__':
    with LMDB(data_path=os.path.join('..', 'Data', 'LMDB'), graph_only=True) as lmdb:
        # lmdb.build_valid_graph()
        lmdb.extract_graph('actor')
