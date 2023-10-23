import pytest
from src.assign_shards import BlancedShardAssigner, Node, Shard
from unittest.mock import patch
from argparse import Namespace

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
                },
                {
                "used_space": 3000,
                "total_space": 10000,
                "id": "nodeD"
                }
        ]    

@pytest.fixture
def BSA(shards, nodes):
    return BlancedShardAssigner(shards, nodes)

@pytest.fixture
def BSA_result():
    return [{'collection': 'coll_0', 'id': 'nodeA', 'shard': 'shard1'}, # 2000+3000+600=5600
            {'collection': 'coll_0', 'id': 'nodeD', 'shard': 'shard2'}, # 3000+2000+1000=6000
            {'collection': 'coll_2', 'id': 'nodeB', 'shard': 'shard1'}, # 4000+1500=5500
            {'collection': 'coll_2', 'id': 'nodeA', 'shard': 'shard2'},
            {'collection': 'coll_1', 'id': 'nodeD', 'shard': 'shard2'}]

def test_can_allocate(BSA, nodes, shards):
    BSA.initialize()
    available_node = Node(**nodes[0])
    full_node = Node(**nodes[2])
    shard = Shard(**shards[0])
    assert BSA.can_allocate(available_node, shard) == True
    assert BSA.can_allocate(full_node, shard) == False

def test_update_available_nodes(BSA, nodes):
    BSA.initialize()
    BSA.update_available_nodes()
    expect_availble_nodes = [Node(**nodes[0]).id, Node(**nodes[3]).id, Node(**nodes[1]).id]
    available_nodes = [node.id for node in BSA.available_nodes]
    assert available_nodes == expect_availble_nodes

def test_get_available_shards(BSA):
    BSA.initialize()
    availble_shards = BSA.get_available_shards(BSA.nodes[0])
    expect_availble_shards = 5
    assert len(availble_shards) == expect_availble_shards

def test_update_nodes_usage(BSA):
    BSA.initialize()
    # intial balanced_usage
    assert BSA.nodes[0].balanced_usage == 6775
    BSA.update_nodes_usage(BSA.nodes[0], BSA.shards[0])
    expect_usage = 5000
    # after update balanced_usage
    expect_balanced_usage = 4020 
    assert BSA.nodes[0].used_space == expect_usage
    assert BSA.nodes[0].available_space == BSA.nodes[0].total_space - expect_usage
    assert BSA.nodes[0].balanced_usage == expect_balanced_usage 

def test_find_cloest_shard(BSA):
    """
    
    """
    BSA.initialize()
    # BSA.nodes[0].balanced_usage = 5700
    except_shard_id = '0'
    BSA.unassigned_shards.sort(key=lambda x: x.size)
    shard = BSA.find_closest_shard(BSA.unassigned_shards,  BSA.nodes[0].balanced_usage -  BSA.nodes[0].used_space)
    assert shard.id ==  except_shard_id

def test_get_shard(BSA):
    BSA.initialize()
    shard = BSA.get_shard(BSA.nodes[0])
    expect_shard_id = '0'
    assert shard.id == expect_shard_id
    # shard 2 is removed because its size is larger than any other nodes' available space
    assert BSA.unassigned_shards == [BSA.shards[1], BSA.shards[3], BSA.shards[4], BSA.shards[5]]

def test_balance(BSA, nodes, shards, BSA_result):

    patch("src.assign_shards.load_data", return_value=shards)
    patch("src.assign_shards.load_data", return_value=nodes)
    res = BSA.balance()
    assert res == BSA_result

    
def test_assign_replica(BSA,BSA_result):
    """
    Test that replica and the primary shard are not assigned to the same node
    """
    mock_args = Namespace(replica=1, shards="data/shards.json", nodes="data/nodes.json")
    patch("src.assign_shards.load_data", return_value=shards)
    patch("src.assign_shards.load_data", return_value=nodes)
    patch("src.assign_shards.BlancedShardAssigner.balance", return_value=BSA_result)    
    res_replica = BSA.assign_replica(BSA_result, mock_args.replica)
    for primary_shard, replica in zip(BSA_result, res_replica):
        assert primary_shard["id"] != replica["replica_node_id"]
