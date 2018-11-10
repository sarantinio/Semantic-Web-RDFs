import logging
import os
import pickle
import time

import rdflib
import requests

logging.basicConfig(level='INFO')


class LodALot:
    __file_path: str
    __base_url: str
    __graph: rdflib.Graph

    def __init__(self, file_path: str, graph_path: str):
        self.__file_path = file_path
        self.__graph_path = graph_path
        if os.path.isfile(self.__graph_path):
            self.__graph = pickle.load(open(self.__graph_path, 'rb'))

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def build_graph_from_triples(self, sub: str = None, pred: str = None, obj: str = None) -> None:
        """Get all the triples from LOD-A-LOT that match a specific pattern. Write the results to a file.

        Args:
            sub: The subject to match as a URI. If None, will match all subjects.
            pred: The predicate to match as URI. If None, will match all predicates.
            obj: The object to match as URI. If None, will match all objects.
        """
        page = 1
        page_size = 1000
        headers = {'Accept': 'application/n-triples'}
        url = 'https://hdt.lod.labs.vu.nl/triple'
        count = self.count_triples(str(obj))
        max_pages = round(int(count) / page_size)
        print(max_pages)
        payload = {
            'page': str(page),
            'page_size': '1000',
            's': str(sub),
            'p': str(pred),
            'o': str(obj),
            'g': '<https://hdt.lod.labs.vu.nl/graph/LOD-a-lot>'
        }

        status_code = 0
        req_triples = None
        while status_code != 200:
            r = requests.head(url=url, params=payload, headers=headers)
            req_triples = requests.get(r.url, headers=headers)
            status_code = int(req_triples.status_code)
            print("FIRST REQUEST: ", req_triples.status_code)
            time.sleep(.2)

        with open(self.__file_path, 'a+') as triple_file:
            triple_file.write(req_triples.text)
        counter = 1
        while True:
            page += 1
            payload['page'] = str(page)

            status_code = 0
            while status_code != 200:
                req_triples = requests.get(url, params=payload, headers=headers)
                status_code = int(req_triples.status_code)
                print("SUBSEQUENT REQUEST:", req_triples.status_code)
                time.sleep(.2)

            with open(self.__file_path, 'a+') as triple_file:
                triple_file.write(req_triples.text)
                counter += 1
                if counter >= max_pages:
                    break

        return self.__convert_to_rdf_graph()

    def __convert_to_rdf_graph(self) -> None:
        self.__graph = rdflib.Graph()
        with open(self.__file_path, 'r') as triples:
            for tr in triples:
                try:
                    self.__graph += rdflib.Graph().parse(data=tr, format='nt')
                except Exception as e:
                    logging.error(e)
        pickle.dump(self.__graph, open(self.__graph_path, 'wb+'))

    @staticmethod
    def count_triples(obj: str = None) -> int:
        """Count all the triples from LOD-A-LOT that match a specific pattern.

        Args:
            obj: The object to match as URI. If None, will match all objects.

        Returns:
            Number of triples matching patterns
        """
        headers = {'Accept': 'application/json'}
        payload = {'o': str(obj), 'g': '<https://hdt.lod.labs.vu.nl/graph/LOD-a-lot>'}
        url = 'https://hdt.lod.labs.vu.nl/triple/count'
        status_code = 0
        count_triples = 0
        while status_code != 200:
            count_triples = requests.get(url, headers=headers, params=payload)
            status_code = count_triples.status_code
            print("COUNT REQUEST:", status_code)
            time.sleep(.2)

        return count_triples.text


if __name__ == '__main__':
    # GET ALL FILM INSTANCES FROM DBPEDIA
    # lal = LodALot(os.path.join('..', 'Data', 'LodALot', 'DBPediaFilmTriples.nt'),
    #               os.path.join('..', 'Data', 'LodALot', 'DBPediaFilmGraph.pkl'))
    # lal.build_graph_from_triples(sub="",
    #                              pred="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
    #                              obj='<http://dbpedia.org/ontology/Film>')

    # GET ALL ACTOR INSTANCES FROM DBPEDIA
    lal = LodALot(os.path.join('..', 'Data', 'LodALot', 'DBPediaActorTriples.nt'),
                  os.path.join('..', 'Data', 'LodALot', 'DBPediaActorGraph.pkl'))
    lal.build_graph_from_triples(sub="",
                                 pred="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                                 obj='<http://dbpedia.org/ontology/Actor>')
