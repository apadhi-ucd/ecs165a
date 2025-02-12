class BTreeNode:
    def __init__(self, leaf=True):
        self.leaf = leaf
        self.keys = []
        self.child_pointers = []
        self.value_pointers = []  # List of RIDs for each key
        
class BTree:
    def __init__(self, degree=3):
        self.root = BTreeNode()
        self.degree = degree  # Minimum degree of B-tree
        
    def insert(self, key, rid):
        root = self.root
        if len(root.keys) == (2 * self.degree - 1):
            new_root = BTreeNode(leaf=False)
            self.root = new_root
            new_root.child_pointers.append(root)
            self._split_child(new_root, 0)
            self._insert_non_full(new_root, key, rid)
        else:
            self._insert_non_full(root, key, rid)
            
    def _insert_non_full(self, node, key, rid):
        i = len(node.keys) - 1
        if node.leaf:
            # Insert key and RID into leaf node
            while i >= 0 and key < node.keys[i]:
                node.keys.insert(i + 1, node.keys[i])
                node.value_pointers.insert(i + 1, node.value_pointers[i])
                i -= 1
            node.keys.insert(i + 1, key)
            if not node.value_pointers or i + 1 >= len(node.value_pointers):
                node.value_pointers.append([rid])
            else:
                node.value_pointers[i + 1].append(rid)
        else:
            # Find appropriate child
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if len(node.child_pointers[i].keys) == (2 * self.degree - 1):
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            self._insert_non_full(node.child_pointers[i], key, rid)
            
    def _split_child(self, parent, index):
        degree = self.degree
        child = parent.child_pointers[index]
        new_node = BTreeNode(leaf=child.leaf)
        
        # Move keys and value pointers
        parent.keys.insert(index, child.keys[degree - 1])
        if not parent.value_pointers:
            parent.value_pointers = [[] for _ in range(len(parent.keys))]
        parent.value_pointers.insert(index, child.value_pointers[degree - 1])
        
        new_node.keys = child.keys[degree:]
        new_node.value_pointers = child.value_pointers[degree:]
        child.keys = child.keys[:degree - 1]
        child.value_pointers = child.value_pointers[:degree - 1]
        
        if not child.leaf:
            new_node.child_pointers = child.child_pointers[degree:]
            child.child_pointers = child.child_pointers[:degree]
            
        parent.child_pointers.insert(index + 1, new_node)
        
    def search(self, key):
        return self._search_recursive(self.root, key)
    
    def _search_recursive(self, node, key):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
            
        if i < len(node.keys) and key == node.keys[i]:
            return node.value_pointers[i]
        elif node.leaf:
            return []
        else:
            return self._search_recursive(node.child_pointers[i], key)
            
    def search_range(self, begin, end):
        result = []
        self._search_range_recursive(self.root, begin, end, result)
        return result
    
    def _search_range_recursive(self, node, begin, end, result):
        i = 0
        while i < len(node.keys) and begin > node.keys[i]:
            i += 1
            
        if not node.leaf:
            self._search_range_recursive(node.child_pointers[i], begin, end, result)
            
        while i < len(node.keys) and node.keys[i] <= end:
            if node.keys[i] >= begin:
                result.extend(node.value_pointers[i])
            i += 1
            if not node.leaf and i < len(node.child_pointers):
                self._search_range_recursive(node.child_pointers[i], begin, end, result)

class Index:
    def __init__(self, table):
        self.indices = [None] * table.num_columns
        # Create index for key column by default
        self.create_index(table.key)
        
    def locate(self, column, value):
        if self.indices[column] is None:
            return []
        return self.indices[column].search(value)
        
    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []
        return self.indices[column].search_range(begin, end)
        
    def create_index(self, column_number):
        self.indices[column_number] = BTree()
        
    def drop_index(self, column_number):
        self.indices[column_number] = None
        
    def insert_entry(self, column, key, rid):
        if self.indices[column] is not None:
            self.indices[column].insert(key, rid)
