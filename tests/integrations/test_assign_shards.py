import pytest

@pytest.fixture
def shards():
    [
        {
            "collection": "coll_0",
            "shard": "shard1",
            "size": 3000
        },
        {
            "collection": "coll_0",
            "shard": "shard2",
            "size": 2000
        },
        {
            "collection": "coll_1",
            "shard": "shard1",
            "size": 9000
        },
        {
            "collection": "coll_1",
            "shard": "shard2",
            "size": 1000
        },
                {
            "collection": "coll_2",
            "shard": "shard1",
            "size": 3000
        },
        {
            "collection": "coll_2",
            "shard": "shard2",
            "size": 600
        }
]

@pytest.fixture
def nodes():
    return [
                {
                "used_space": 2000,
                "total_space": 10000,
                "id": "nodeA"
                },
                {
                "used_space": 4000,
                "total_space": 10000,
                "id": "nodeB"
                },
                {
                "used_space": 10000,
                "total_space": 10000,
                "id": "nodeB"
                }
            ]    


def test_node_available(shards, nodes):
    

def test_replica_larger_than_nodes(shards, nodes):
    pass

def test_no_node_available(shards, nodes):
    pass
