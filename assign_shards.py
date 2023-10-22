import json
import argparse
import pdb

def load_data(file_name):
    with open(file_name) as f:
        return json.load(f, encoding='utf-8')


class blancedShardAssigner(object):
    def __init__(self, shards, nodes):
        self.shards = shards
        self.nodes = nodes
        self.num_collections = len(set([shard['collection'] for shard in shards]))
        self.used_shard = set()
        self.deadnode = set()
        self.initialize()
    
    def initialize(self):
        """
        calulate the available spalce of each node
        and give id to each shard
        """
        for i, node in enumerate(self.nodes):
            node['available_space'] = node['total_space'] - node['used_space']
            node['num_id'] = str(i)
            node['average_usage'] = (sum([node['used_space'] for node in self.nodes]) \
            +sum([shard['size'] for shard in self.shards])) / (len(self.nodes)*self.num_collections)
        
        for i, shard in enumerate(self.shards):
            shard['id'] = str(i)
        
    
    def can_allocate(self, node, shard):
        """
        check if the shard can be allocated to the node
        """

        if node['available_space'] >= shard['size']:
            return True
        return False

    def update_available_shard_list(self, node):
        """
        update the shard list
        """
        shard_list = [shard for shard in self.shards if self.can_allocate(node,shard) and shard['id'] not in self.used_shard]
        shard_list = sorted(shard_list, key=lambda x: x['size'])
        return shard_list
    
    def get_available_nodes(self, node, shard):
        availabe_nodes = [node for node in self.nodes if node not in self.deadnode]
        if not availabe_nodes:
            raise ValueError("no available nodes")
        availabe_nodes = sorted(availabe_nodes, key=lambda x: x['available_space'], reverse=True)
        
        return availabe_nodes

    def update_nodes_usage(self, node, shard):
        node['used_space'] += shard['size']
        node['available_space'] -= shard['size']
        node['average_usage'] = (sum([node['used_space'] for node in self.nodes]) \
            +sum([shard['size'] for shard in self.shards])) / (len(self.nodes)*self.num_collections)
        return node
            
    
    def find_cloest_shard(self,shard_list,available_space):
        """
        use binary search to find the shard that is closest to the average usage
        """
        lo, hi = 0, len(shard_list)
        if not shard_list:
            return None
        
        if hi ==1:
            return shard_list[0]
        if hi == 2:
            return max(shard_list, key=lambda x: x['size'])
        
        while lo < hi:
            mid = (lo + hi) // 2
            if shard_list[mid]['size'] < available_space:
                if mid > 0 and shard_list[mid-1]['size'] > available_space:
                    return shard_list[mid-1] if abs(shard_list[mid]['size'] - available_space) < abs(available_space - shard_list[mid-1]['size']) else shard_list[mid]
                lo = mid + 1
            else:
                if mid > 0 and shard_list[mid+1]['size'] < available_space:
                    return shard_list[mid+1] if abs(shard_list[mid]['size'] - available_space) < abs(available_space - shard_list[mid-1]['size']) else shard_list[mid]
                hi = mid 
        
        return shard_list[lo]

    
    def get_shard(self, node, average_usage):
        """
        get the shard which is assigned to the node
        use binary search to find the shard that is closest to the average usage
        """
        shard_list = self.update_available_shard_list(node)
        available_space = average_usage - node['used_space']
        closest_shard = self.find_cloest_shard(shard_list, available_space)
        if closest_shard:
            self.used_shard.add(closest_shard['id'])
        return closest_shard


    def balance(self, nodes, shards):
        """
        the main function to balance the shards
        """
        res = []
        # sort the nodes by their effective weight in descending order
        availabe_nodes = self.get_available_nodes(nodes, shards)
        for _ in range(self.num_collections):
            for node in availabe_nodes:
                shard = self.get_shard(node)
                if shard:
                    self.update_nodes_usage(node, shard)
                    res.append({'id': node['id'], 'collection': shard['collection'], 'shard': shard['shard']})

        return res
    
    def assign_replica(self, res):
        """
        assign the replica shards to the nodes
        replica should not be in the same node with the original shard
        we can use round robin to assign the replica shards
        """    
        if args.replica > len(self.nodes):
            raise ValueError("the number of replica should not be larger than the number of nodes")
        
        res_replica = []
        self.shards.sort(key=lambda x: x['size'], reverse=False)
        for i, record in enumerate(res):
            node_list = [node for node in self.nodes if node['id'] != record['id']]
            # sort nodes by their available space reversely
            nodes = sorted(node_list, key=lambda x: x['available_space'], reverse=True)
            # round robin to assign the replica shards
            for j in range(args.replica):
                replica_node_id = nodes[(i+j) % len(nodes)]['id']
                res_replica.append({'id': record['shard'], 'collections': record['collection'],'replica_node_id': replica_node_id, 'replica_id': str(j)})
        
        return res_replica

            
def main(args):
    shards = load_data(args.shards)
    nodes = load_data(args.nodes)
    bsa = blancedShardAssigner(shards, nodes)
    res = bsa.balance(nodes, shards)
    print("assignment", res)
    replica = bsa.assign_replica(res)
    print("replica", replica)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--shards', type=str, help='the file which contains the information of shards', required=True)
    parser.add_argument('-n', '--nodes', type=str, help='the file which contains the information of nodes', required=True)
    parser.add_argument('-r', '--replica', type=int, help='the number of total assignments of the same shard', default=1)
    args = parser.parse_args()
    main(args)




