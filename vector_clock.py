class VectorClock:
    @staticmethod
    def increment(clock, node):
        """Increments the clock for a specific node."""
        new_clock = clock.copy() if clock else {}
        new_clock[node] = new_clock.get(node, 0) + 1
        return new_clock

    @staticmethod
    def is_conflict(clock1, clock2):
        """
        Checks if two vector clocks are in conflict (concurrent).
        Returns True if they are concurrent, False otherwise.
        """
        if not clock1 or not clock2:
            return False
            
        c1_greater = False
        c2_greater = False
        
        all_nodes = set(clock1.keys()).union(set(clock2.keys()))
        
        for node in all_nodes:
            val1 = clock1.get(node, 0)
            val2 = clock2.get(node, 0)
            
            if val1 > val2:
                c1_greater = True
            elif val2 > val1:
                c2_greater = True
                
        return c1_greater and c2_greater

    @staticmethod
    def merge(clock1, clock2):
        """Merges two vector clocks by taking the maximum value for each node."""
        if not clock1: return clock2.copy() if clock2 else {}
        if not clock2: return clock1.copy() if clock1 else {}
        
        merged = {}
        all_nodes = set(clock1.keys()).union(set(clock2.keys()))
        for node in all_nodes:
            merged[node] = max(clock1.get(node, 0), clock2.get(node, 0))
        return merged
