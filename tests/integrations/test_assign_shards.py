import pytest
from src.assign_shards import BlancedShardAssigner, main
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
