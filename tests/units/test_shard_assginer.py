import pytest
from src.assign_shards import BlancedShardAssigner


@pytest.fixture
def shards():
    return [
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
            "size": 1500
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
                "id": "nodeC"
                }
            ]    

@pytest.fixture
def BSA(shards, nodes):
    return BlancedShardAssigner(shards, nodes)


def test_can_allocate(BSA):
    available_node = BSA.nodes[0]
    full_node = BSA.nodes[2]
    shard = BSA.shards[0]
    assert BSA.can_allocate(available_node, shard) == True
    assert BSA.can_allocate(full_node, shard) == False

def test_get_available_nodes(BSA):
    availble_nodes = BSA.get_available_nodes()
    expect_availble_nodes = [BSA.nodes[0], BSA.nodes[1]]
    assert availble_nodes == expect_availble_nodes

def test_get_available_shards(BSA):
    availble_shards = BSA.get_available_shards(BSA.nodes[0])
    expect_availble_shards = 5
    assert len(availble_shards) == expect_availble_shards

def test_update_nodes_usage(BSA):
    BSA.update_nodes_usage(BSA.nodes[0], BSA.shards[0])
    expect_usage = 5000
    expect_balanced_usage = 3850 #(2000+4000+3000+2000+9000+1000+1500+600)/6
    assert BSA.nodes[0].used_space == expect_usage
    assert BSA.nodes[0].available_space == BSA.nodes[0].total_space - expect_usage
    assert BSA.nodes[0].balanced_usage == expect_balanced_usage #TODO check the calculation later

def test_find_cloest_shard(BSA):
    """
    
    """
    BSA.initialize()
    except_shard = BSA.shards[1]
    BSA.shards.sort(key=lambda x: x.size)
    shard = BSA.find_cloest_shard(BSA.shards,  BSA.nodes[0].balanced_usage -  BSA.nodes[0].used_space)
    assert shard ==  except_shard

def test_get_shard(BSA):
    

def test_assign_replica(BSA):
    pass








    



def test_no_node_available(shards, nodes):
    pass

def test_node_available(shards, nodes):
    pass
