class InstanceMatcher:
    first_instances: map
    first_set: set

    second_instances: map
    second_set: set

    def __init__(self, first_instances: map, second_instances: map):
        self.first_instances = first_instances
        self.second_instances = second_instances
        self.__load_instances()

    def __load_instances(self):
        """Process a dictionary of instances into a default dict of the form (URI: boolean)

        Returns:
            The instances loaded as a defaultdict(bool)
        """

        self.first_instances = list(self.first_instances)
        self.second_instances = list(self.second_instances)

        set_a = set(self.first_instances)
        set_b = set(self.second_instances)

        self.first_set = set_a
        self.second_set = set_b

    def __get_conjunction(self) -> set:
        """ Compute the conjunctive set of the 2 collections.

        Returns:
            The conjunctive set and its cardinality.
        """
        con_set = self.first_set.intersection(self.second_set)

        print("con cardinality ", len(con_set))

        return con_set

    def __get_disjunction(self) -> set:
        """ Compute the disjunctive set of the 2 collections.

            Returns:
                The union set and its cardinality.
            """
        dis_set = self.first_set.union(self.second_set)

        print("dis cardinality ", len(dis_set))

        return dis_set

    def __get_difference(self, first: set, second: set):
        return first.difference(second)

    def get_similarity(self, sim_type: str) -> float:
        """ Compute the similarity of 2 classes.
        Args:
            sim_type: The type of similarity to compute.
        Returns:
            The Jaccard similarity between 2 measures.
        """

        if sim_type == "jaccard":
            conjunction_set = self.__get_conjunction()
            disjunction_set = self.__get_disjunction()
            first_difference = self.__get_difference(self.first_set, self.second_set)
            print('LMDB - DBPEDIA:', len(first_difference))
            second_difference = self.__get_difference(self.second_set, self.first_set)
            print('DBPEDIA - LMDB:', len(second_difference))

            jaccard_sim = len(conjunction_set) / len(disjunction_set)
        else:
            raise Exception('Similarity measure not implemented: {}'.format(sim_type))

        print("Jaccard Similarity ", jaccard_sim)

        return jaccard_sim


if __name__ == '__main__':
    pass
