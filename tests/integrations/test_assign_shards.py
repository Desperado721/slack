import pytest
from src.assign_shards import BlancedShardAssigner, main
from argparse import Namespace


@pytest.fixture
def shards():
    return [
        {"id": "0", "collection": "coll_0", "shard": "shard1", "size": 3000},
        {"id": "1", "collection": "coll_0", "shard": "shard2", "size": 2000},
        {"id": "2", "collection": "coll_1", "shard": "shard1", "size": 9000},
        {"id": "3", "collection": "coll_1", "shard": "shard2", "size": 1000},
        {"id": "4", "collection": "coll_2", "shard": "shard1", "size": 1500},
        {"id": "5", "collection": "coll_2", "shard": "shard2", "size": 600},
    ]


@pytest.fixture
def large_shards():
    return [
        {"id": "0", "collection": "coll_0", "shard": "shard1", "size": 30000},
        {"id": "1", "collection": "coll_0", "shard": "shard2", "size": 20000},
        {"id": "2", "collection": "coll_1", "shard": "shard1", "size": 90000},
        {"id": "3", "collection": "coll_1", "shard": "shard2", "size": 10000},
        {"id": "4", "collection": "coll_2", "shard": "shard1", "size": 15000},
        {"id": "5", "collection": "coll_2", "shard": "shard2", "size": 60000},
    ]


@pytest.fixture
def full_nodes():
    return [
        {"num_id": "0", "used_space": 10000, "total_space": 10000, "id": "nodeA"},
        {"num_id": "1", "used_space": 10000, "total_space": 10000, "id": "nodeB"},
        {"num_id": "2", "used_space": 10000, "total_space": 10000, "id": "nodeC"},
        {"num_id": "3", "used_space": 10000, "total_space": 10000, "id": "nodeD"},
    ]


@pytest.fixture
def nodes():
    return [
        {"num_id": "0", "used_space": 2000, "total_space": 10000, "id": "nodeA"},
        {"num_id": "1", "used_space": 4000, "total_space": 10000, "id": "nodeB"},
        {"num_id": "2", "used_space": 10000, "total_space": 10000, "id": "nodeC"},
        {"num_id": "3", "used_space": 3000, "total_space": 10000, "id": "nodeD"},
    ]


@pytest.fixture
def BSA(shards, nodes):
    return BlancedShardAssigner(shards, nodes)


@pytest.fixture
def BSA_result():
    return [
        {
            "collection": "coll_0",
            "id": "nodeA",
            "shard": "shard1",
        },  # nodeA: 2000+3000+600=5600
        {
            "collection": "coll_0",
            "id": "nodeD",
            "shard": "shard2",
        },  # nodeD: 3000+2000+1000=6000
        {
            "collection": "coll_2",
            "id": "nodeB",
            "shard": "shard1",
        },  # nodeB: 4000+1500=5500
        {"collection": "coll_2", "id": "nodeA", "shard": "shard2"},
        {"collection": "coll_1", "id": "nodeD", "shard": "shard2"},
    ]


def test_no_node_available(full_nodes, shards):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        BSA = BlancedShardAssigner(shards=shards, nodes=full_nodes)

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1


def test_replica_larger_than_nodes():
    mock_args = Namespace(replica=5, shards="data/shards.json", nodes="data/nodes.json")
    with pytest.raises(ValueError):
        main(mock_args)


def test_no_shard_available(nodes, large_shards):

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        BSA = BlancedShardAssigner(shards=large_shards, nodes=nodes)

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1
