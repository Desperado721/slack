import pytest
from src.assign_shards import BlancedShardAssigner, main
from unittest.mock import patch
from argparse import Namespace


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
    return [
        {"id": "nodeA", "collection": "coll_0", "shard": "shard1"},
        {"id": "nodeD", "collection": "coll_2", "shard": "shard2"},
        {"id": "nodeB", "collection": "coll_0", "shard": "shard2"},  # 4000+2000=6000
        {
            "id": "nodeA",
            "collection": "coll_2",
            "shard": "shard1",
        },  # 2000+1500+3000=6500
        {"id": "nodeD", "collection": "coll_1", "shard": "shard2"},
    ]  # 3000+1000+600=4600


def test_no_node_available(full_nodes, shards):

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        BSA = BlancedShardAssigner(shards=shards, nodes=full_nodes)

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1


def test_replica_larger_than_nodes(BSA_result, nodes, shards):
    mock_args = Namespace(replica=5, shards="data/shards.json", nodes="data/nodes.json")
    patch("src.assign_shards.load_data", return_value=shards)
    patch("src.assign_shards.load_data", return_value=nodes)
    patch("src.assign_shards.BlancedShardAssigner.balance", return_value=BSA_result)
    with pytest.raises(ValueError):
        main(mock_args)


def test_no_node_available():
    mock_args = Namespace(replica=1, shards="data/shards.json", nodes="data/nodes.json")
    patch("src.assign_shards.load_data", return_value=shards)
    patch("src.assign_shards.load_data", return_value=nodes)
    main(mock_args)
