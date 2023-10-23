"""
This is a script to assign shards to nodes in a balanced way.
"""
import json
import argparse
from typing import List, Union, Optional
from dataclasses import dataclass, field
import logging
import sys

logging.basicConfig(level=logging.INFO)

def load_data(file_name: str):
    """
    load the data from the json file

    param: file_name: the file name of the json file
    """
    with open(file_name) as f:
        return json.load(f, encoding="utf-8")

def write_to_file(output_file: str, data: Union[List[dict], dict]):
    """
    write the data to the output file

    param: output_file: the file name of the output file
    param: data: the data to be written to the output file
    """
    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)

@dataclass
class Shard(object):
    id: str = field(init=False, default="0")
    collection: str
    shard: str
    size: float


@dataclass
class Node(object):
    id: str
    num_id: str = field(init=False, default="0")
    total_space: float
    used_space: float
    available_space: float = field(init=False)
    balanced_usage: float = field(init=False, default=0.0)

    def __post_init__(self):
        """
        calulate the available spalce of each node
        """
        self.available_space = self.total_space - self.used_space


class BlancedShardAssigner(object):
    def __init__(self, shards, nodes):

        self.shards = [Shard(**shard) for shard in shards]
        self.nodes = [Node(**node) for node in nodes]
        self.deadnodes = []
        self.available_nodes = self.nodes.copy()
        self.num_collections = len(set([shard["collection"] for shard in shards]))
        self.unassigned_shards = self.shards.copy()
        self.cantassigned_shards = []
        self.initialize()


    def initialize(self):
        """
        Calulate the available space, balanced_usage of each node
        and give id to each shard

        Note: balanced_usage is the average usage of each node after unassigned shards are assigned
        """

        for i, shard in enumerate(self.shards):
            shard.id = str(i)
        
        self.update_unassigned_shards()

        for i, node in enumerate(self.nodes):
            node.num_id = str(i)
            if node.available_space <= 0:
                self.deadnodes.append(node)

        self.update_available_nodes()

        for i, node in enumerate(self.nodes):
            node.balanced_usage = (
                sum([node.used_space for node in self.nodes])
                + sum([shard.size for shard in self.unassigned_shards])
            ) / len(self.nodes)

    def can_allocate(self, node: Node, shard: Shard) -> bool:
        """
        Check if the shard can be allocated to the node
        """

        if node.available_space >= shard.size:
            return True
        return False

    def get_available_shards(self, node: Node) -> List[Shard]:
        """
        Update the available_shards to ensure that the shard can be allocated to the node 
        and is not assigned to any node
        """
        available_shards = [
            shard
            for shard in self.shards
            if self.can_allocate(node, shard) and shard in self.unassigned_shards
        ]
        
        available_shards.sort(key=lambda x: x.size)
        return available_shards

    def update_available_nodes(self):
        """
        Get a list of available nodes which have enough space to receive shards
        """
        self.available_nodes = [node for node in self.nodes if node not in self.deadnodes]
        if not self.available_nodes:
            logging.info("no available nodes, stop assigning shards")
            sys.exit(1)
        # sort the nodes by their available_space in descending order
        self.available_nodes.sort(key=lambda x: x.available_space, reverse=True)

    def update_nodes_usage(self, node: Node, shard: Shard) -> Node:
        """
        Update the usage of the node after the shard is assigned to the node
        """
        node.used_space += shard.size
        node.available_space -= shard.size
        self.update_available_nodes()
        if self.unassigned_shards:
            node.balanced_usage = (
                sum([node.used_space for node in self.available_nodes])
                + sum([shard.size for shard in self.unassigned_shards])
            ) / len(self.unassigned_shards)
        if node.available_space <= 0:
            self.deadnodes.add(node)
        return node

    def find_cloest_shard(
        self, shard_list: List[Shard], target: float
    ) -> Shard:
        """
        Use binary search to find the shard that is closest to the average usage
        """

        if not shard_list:
            return None

        if target < shard_list[0].size:
            return shard_list[0]
        
        if target> shard_list[-1].size:
            return shard_list[-1]
        
        lo, hi = 0, len(shard_list)

        while lo < hi:
            mid = (lo + hi) // 2
            if  target < shard_list[mid].size :
                if mid > 0 and shard_list[mid - 1].size < target:
                    return (
                        shard_list[mid]
                        if abs(shard_list[mid].size - target)
                        < abs(target - shard_list[mid - 1].size)
                        else shard_list[mid-1]
                    )
                hi=mid
            else:
                if mid < len(shard_list)-1 and shard_list[mid+1].size < target:
                    return (
                        shard_list[mid]
                        if abs(shard_list[mid].size - target)
                        < abs(target - shard_list[mid+1].size)
                        else shard_list[mid+1]
                    )
                lo = mid+1

        return shard_list[lo] 

    def get_shard(self, node: Node) -> Optional[Shard]:
        """
        Get the shard which fits the node best
        use binary search to find the shard that is closest to the average usage
        """
        shard_list = self.get_available_shards(node)
        closest_shard = self.find_cloest_shard(shard_list, node.balanced_usage - node.used_space)
        if closest_shard:
            self.unassigned_shards.remove(closest_shard)
        
        return closest_shard
    
    def update_unassigned_shards(self):
        for shard in self.unassigned_shards:
            if shard.size > max([node.available_space for node in self.nodes]):
                logging.warning("The size shard %s  is %s, which is larger than the maximum available \
                                space %s, it can't be allocated", shard.id, shard.size, \
                                      max([node.available_space for node in self.nodes]))
                self.unassigned_shards.remove(shard)
                self.cantassigned_shards.append(shard)


    def balance(self):
        """
        The main function to balance the shards
        """
        res = []
        self.update_available_nodes()
        for _ in range(self.num_collections):
            while self.unassigned_shards:
                for node in self.available_nodes:
                    shard = self.get_shard(node)
                    if shard:
                        self.update_nodes_usage(node, shard)
                        res.append(
                            {
                                "id": node.id,
                                "collection": shard.collection,
                                "shard": shard.shard,
                            }
                        )
                    self.update_unassigned_shards()
        return res

    def assign_replica(self, res, replica):
        """
        Assign the replica shards to the nodes
        replica should not be in the same node with the original shard
        we can use round robin to assign the replica shards
        """
        if replica >= len(self.nodes):
            raise ValueError(
                "The number of replica should  be smaller than the number of nodes"
            )

        res_replica = []
        self.shards.sort(key=lambda x: x.size, reverse=False)
        for i, record in enumerate(res):
            node_list = [node for node in self.nodes if node.id != record["id"]]
            # sort nodes by their available space reversely
            nodes = sorted(node_list, key=lambda x: x.available_space, reverse=True)
            # round robin to assign the replica shards
            for j in range(replica):
                replica_node_id = nodes[(i + j) % len(nodes)].id
                res_replica.append(
                    {
                        "id": record["shard"],
                        "collections": record["collection"],
                        "replica_node_id": replica_node_id,
                        "replica_id": str(j),
                    }
                )

        return res_replica


def main(args):
    shards = load_data(args.shards)
    nodes = load_data(args.nodes)
    bsa = BlancedShardAssigner(shards, nodes)
    res = bsa.balance()
    logging.info("The result of shard assignment is %s", res)
    logging.info("Writing assignment to file %s", "output/shard_assignment_results.json")
    write_to_file("output/shard_assignment_results.json", res)
    replica = bsa.assign_replica(res, args.replica)
    logging.info("The result of replica assignment is %s", replica) 
    logging.info("Writing replica assignment to file %s", "output/replica_assignment_results.json")
    write_to_file("output/replica_assignment_results.json", replica)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--shards",
        type=str,
        help="the file which contains the information of shards",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--nodes",
        type=str,
        help="the file which contains the information of nodes",
        required=True,
    )
    parser.add_argument(
        "-r",
        "--replica",
        type=int,
        help="the number of total assignments of the same shard",
        default=1,
    )
    args = parser.parse_args()
    main(args)
