import os

from Services.InstanceMatcher import InstanceMatcher
from Services.LMDB import LMDB
from Services.LodALot import LodALot

if __name__ == '__main__':
    with LMDB(os.path.join('..', 'Data', 'LMDB')) as lmdb:
        lmdb_instances = lmdb.get_instances()
    with LodALot() as lol:
        lol_instances = lol.get_instances()

    with InstanceMatcher(lmdb_instances, 'owl:sameAs', lol_instances, 'subject') as im:
        im.get_similarity("jaccard")
