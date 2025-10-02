# Tree Class
class SimpleTreeNode:
    def __init__(self):
        self.__value = set()
        self.__next: dict[str,SimpleTreeNode] = {}
        
    def size(self):
        own_size = len(self.__value)
        child_sizes = 0
        for child in self.__next.values():
            child_sizes += child.size()
        return own_size + child_sizes

    def setValue(self, value):
        self.__value = value

    def getValue(self):
        return self.__value

    def createNode(self, key):
        self.__next[key] = SimpleTreeNode()

    def __contains__(self, item):
        return item in self.__next

    def __getitem__(self, key):
        return self.__next[key]

    def __setitem__(self, key, value):
        self.__next[key] = SimpleTreeNode()
        self.__next[key].setValue(value)

    def __str__(self, level=1):
        node_strings = "\n"
        tabs = "\t" * level
        for key, node in self.__next.items():
            node_strings += f"{tabs}{key}:{node.__str__(level + 1)}"
        # node_strings += '\n'
        return f"{self.__value} {node_strings}"
