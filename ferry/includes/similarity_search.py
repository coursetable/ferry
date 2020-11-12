"""
Methods used for finding similar vectors. Used by /ferry/embed/compute_similars.py
"""

from typing import Dict, List, Set, Union

import numpy as np
from annoy import AnnoyIndex
from sklearn.preprocessing import StandardScaler


class SimilaritySearchError(Exception):
    """
    Object for similarity search exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


def get_nearest_neighbors(
    node_ids: List[Union[int, str]], embeddings: np.array, num_nearest: int
) -> Dict[Union[int, str], Set[Union[int, str]]]:

    """
    Compute similar nodes among a set of embeddings.

    Parameters
    ----------
    node_ids:
                names of each embedding sample

    embeddings:
                embedding vectors of shape (num_total, embed_dim)

    num_nearest:
        number of nearest-neighbors to find

    Returns
    -------
    dict mapping node_id's to nearest-neighbors
    """

    num_total = embeddings.shape[0]
    embed_dim = embeddings.shape[1]

    if num_total != len(node_ids):
        raise SimilaritySearchError("Lengths of node_ids and embeddings must match")

    # normalize embeddings
    embeddings = StandardScaler().fit_transform(embeddings)

    # initialize annoy index w/ angular distance metric
    annoy_index = AnnoyIndex(embed_dim, metric="angular")

    # add items to index
    for i in range(num_total):
        annoy_index.add_item(node_ids[i], embeddings[i])

    # build index
    annoy_index.build(n_trees=16, n_jobs=-1)

    nodes_neighbors = {}

    for node_id in node_ids:
        neighbors = annoy_index.get_nns_by_item(node_id, num_nearest + 1)

        # remove course itself
        neighbors = [x for x in neighbors if x != node_id]

        # set neighbors
        nodes_neighbors[node_id] = neighbors

    # symmmmetric filtering
    for node_id, neighbors in nodes_neighbors.items():

        filter_nodes = [x for x in neighbors if node_id in nodes_neighbors[x]]

        nodes_neighbors[node_id] = filter_nodes

    nodes_neighbors = {
        node_id: list(zip(neighbors, range(len(neighbors))))
        for node_id, neighbors in nodes_neighbors.items()
        if len(neighbors) > 0
    }

    return nodes_neighbors
