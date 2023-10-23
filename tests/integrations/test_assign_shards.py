import pytest
from src.assign_shards import BlancedShardAssigner, main
from unittest.mock import patch
from argparse import Namespace
import logging


@pytest.fixture
def shards():
    return [
        {"collection": "coll_0", "shard": "shard1", "size": 3000},
        {"collection": "coll_0", "shard": "shard2", "size": 2000},
        {"collection": "coll_1", "shard": "shard1", "size": 9000},
        {"collection": "coll_1", "shard": "shard2", "size": 1000},
        {"collection": "coll_2", "shard": "shard1", "size": 1500},
        {"collection": "coll_2", "shard": "shard2", "size": 600},
    ]

@pytest.fixture
def large_shards():
    return [
        {"collection": "coll_0", "shard": "shard1", "size": 30000},
        {"collection": "coll_0", "shard": "shard2", "size": 20000},
        {"collection": "coll_1", "shard": "shard1", "size": 90000},
        {"collection": "coll_1", "shard": "shard2", "size": 10000},
        {"collection": "coll_2", "shard": "shard1", "size": 15000},
        {"collection": "coll_2", "shard": "shard2", "size": 60000},
    ]



@pytest.fixture
def full_nodes():
    return [
        {"used_space": 10000, "total_space": 10000, "id": "nodeA"},
        {"used_space": 10000, "total_space": 10000, "id": "nodeB"},
        {"used_space": 10000, "total_space": 10000, "id": "nodeC"},
        {"used_space": 10000, "total_space": 10000, "id": "nodeD"},
    ]


@pytest.fixture
def nodes():
    return [
        {"used_space": 2000, "total_space": 10000, "id": "nodeA"},
        {"used_space": 4000, "total_space": 10000, "id": "nodeB"},
        {"used_space": 10000, "total_space": 10000, "id": "nodeC"},
        {"used_space": 3000, "total_space": 10000, "id": "nodeD"},
    ]


@pytest.fixture
def BSA(shards, nodes):
    return BlancedShardAssigner(shards, nodes)


@pytest.fixture
def BSA_result():
    return [{'collection': 'coll_0', 'id': 'nodeA', 'shard': 'shard1'}, # nodeA: 2000+3000+600=5600
            {'collection': 'coll_0', 'id': 'nodeD', 'shard': 'shard2'}, # nodeD: 3000+2000+1000=6000
            {'collection': 'coll_2', 'id': 'nodeB', 'shard': 'shard1'}, # nodeB: 4000+1500=5500
            {'collection': 'coll_2', 'id': 'nodeA', 'shard': 'shard2'},
            {'collection': 'coll_1', 'id': 'nodeD', 'shard': 'shard2'}]


def test_no_node_available(full_nodes, shards, caplog):
    with caplog.at_level(logging.INFO):
        BSA = BlancedShardAssigner(shards=shards, nodes=full_nodes)
        assert "no available nodes, stop assigning shards" in caplog.text



def test_replica_larger_than_nodes(nodes, shards):
    mock_args = Namespace(replica=5, shards="data/shards.json", nodes="data/nodes.json")
    patch("src.assign_shards.load_data", return_value=shards)
    patch("src.assign_shards.load_data", return_value=nodes)
    with pytest.raises(ValueError):
        main(mock_args)


def test_no_shard_available(nodes, large_shards, caplog):
    mock_args = Namespace(replica=1, shards="data/shards.json", nodes="data/nodes.json")
    patch("src.assign_shards.load_data", return_value=large_shards)
    patch("src.assign_shards.load_data", return_value=nodes)
    
    with caplog.at_level(logging.INFO):
        main(mock_args)
        assert "All shards are assigned, stop assigning shards" in caplog.text
