import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, nodes=None, replicas=3):
        """
        Initializes the consistent hash ring.
        :param nodes: List of node names (e.g., ["node1", "node2"]).
        :param replicas: Number of virtual nodes per physical node.
        """
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        """Returns the MD5 hash of the key as an integer."""
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return int(m.hexdigest(), 16)

    def add_node(self, node):
        """Adds a physical node and its virtual nodes to the ring."""
        for i in range(self.replicas):
            virtual_node_name = f"{node}:{i}"
            key = self._hash(virtual_node_name)
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)

    def remove_node(self, node):
        """Removes a physical node and its virtual nodes from the ring."""
        for i in range(self.replicas):
            virtual_node_name = f"{node}:{i}"
            key = self._hash(virtual_node_name)
            if key in self.ring:
                del self.ring[key]
                self.sorted_keys.remove(key)

    def get_preference_list(self, key, n_nodes=3):
        """
        Gets the preference list of physical nodes for a given key.
        Returns up to n_nodes distinct physical nodes.
        """
        if not self.ring:
            return []

        hash_val = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hash_val)
        
        if idx == len(self.sorted_keys):
            idx = 0

        preference_list = []
        # Keep finding distinct nodes until we have n_nodes
        start_idx = idx
        while len(preference_list) < n_nodes and len(preference_list) < len(set(self.ring.values())):
            node = self.ring[self.sorted_keys[idx]]
            if node not in preference_list:
                preference_list.append(node)
            
            idx = (idx + 1) % len(self.sorted_keys)
            if idx == start_idx:
                break
                
        return preference_list
