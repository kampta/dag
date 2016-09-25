from copy import copy, deepcopy

class DAGValidationError(Exception):
    pass

class DAG(object):
    """ Directed acyclic graph implementation. """

    def __init__(self):
        """ Construct a new DAG with no nodes or edges. """
        self.graph = {}

    def add_node(self, node_name, graph=None):
        """ Add a node if it does not exist yet, or error out. """
        if not graph:
            graph = self.graph
        
        graph[node_name] = set()

    def add_node_if_not_exists(self, node_name, graph=None):
        try:
            self.add_node(node_name, graph=graph)
        except KeyError:
            pass

    def delete_node(self, node_name, graph=None):
        """ Deletes this node and all edges referencing it. """
        if not graph:
            graph = self.graph
        if node_name not in graph:
            raise KeyError('node %s does not exist' % node_name)
        graph.pop(node_name)

        for node, edges in graph.items():
            if node_name in edges:
                edges.remove(node_name)

    def delete_node_if_exists(self, node_name, graph=None):
        try:
            self.delete_node(node_name, graph=graph)
        except KeyError:
            pass

    def add_edge(self, ind_node, dep_node, graph=None):
        """ Add an edge (dependency) between the specified nodes. """
        if not graph:
            graph = self.graph
        if ind_node not in graph or dep_node not in graph:
            raise KeyError('one or more nodes do not exist in graph')
        test_graph = deepcopy(graph)
        test_graph[ind_node].add(dep_node)
        is_valid, message = self.validate(test_graph)
        if is_valid:
            graph[ind_node].add(dep_node)
        else:
            raise DAGValidationError()


    def delete_edge(self, ind_node, dep_node, graph=None):
        """ Delete an edge from the graph. """
        if not graph:
            graph = self.graph
        if dep_node not in graph.get(ind_node, []):
            raise KeyError('this edge does not exist in graph')
        graph[ind_node].remove(dep_node)


    def rename_edges(self, old_task_name, new_task_name, graph=None):
        """ Change references to a task in existing edges. """
        if not graph:
            graph = self.graph
        for node, edges in graph.items():

            if node == old_task_name:
                graph[new_task_name] = copy(edges)
                del graph[old_task_name]

            else:
                if old_task_name in edges:
                    edges.remove(old_task_name)
                    edges.add(new_task_name)


    def predecessors(self, node, graph=None):
        """ Returns a list of all predecessors of the given node """
        if graph is None:
            graph = self.graph
        return [key for key in graph if node in graph[key]]


    def downstream(self, node, graph=None):
        """ Returns a list of all nodes this node has edges towards. """
        if graph is None:
            graph = self.graph
        if node not in graph:
            raise KeyError('node %s is not in graph' % node)
        return list(graph[node])

    def all_downstreams(self, node, graph=None):
        """Returns a list of all nodes ultimately downstream
        of the given node in the dependency graph, in
        topological order."""
        if graph is None:
            graph = self.graph
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            downstreams = self.downstream(nodes[i], graph)
            for downstream_node in downstreams:
                if downstream_node not in nodes_seen:
                    nodes_seen.add(downstream_node)
                    nodes.append(downstream_node)
            i += 1
        return filter(lambda node: node in nodes_seen, self.topological_sort(graph=graph))

    def root(self, graph=None):
        """ Return a list of all leaves (nodes with no downstreams) """
        if graph is None:
            graph=self.graph
        return [key for key in graph if not graph[key]]

    def all_leaves(self, graph=None):
        """ Return a list of all leaves (nodes with no downstreams) """
        if graph is None:
            graph=self.graph
        return [key for key in graph if not graph[key]]

    def from_dict(self, graph_dict):
        """ Reset the graph and build it from the passed dictionary.
        The dictionary takes the form of {node_name: [directed edges]}
        """

        self.reset_graph()
        for new_node in graph_dict.keys():
            self.add_node(new_node)
            for node in graph_dict[new_node]:
                self.add_node(node)

        for ind_node, dep_nodes in graph_dict.items():
            if not isinstance(dep_nodes, list):
                raise TypeError('dict values must be lists')
            for dep_node in dep_nodes:
                self.add_edge(ind_node, dep_node)


    def reset_graph(self):
        """ Restore the graph to an empty state. """
        self.graph = {}


    def ind_nodes(self, graph=None):
        """ Returns a list of all nodes in the graph with no dependencies. """
        if graph is None:
            graph=self.graph
        all_nodes, dependent_nodes = set(graph.keys()), set()
        for downstream_nodes in graph.values():
            [dependent_nodes.add(node) for node in downstream_nodes]
        return list(all_nodes - dependent_nodes)


    def all_paths(self, start=None, graph=None):
        paths = []
        if graph is None:
            graph = self.graph

        if start is None:
            start_nodes = self.ind_nodes(graph)
        else:
            start_nodes = [start]

        all_leaves = self.all_leaves()

        for start in start_nodes:
            stack = [(start, [start])]
            while stack:
                (vertex, path) = stack.pop()
                for next in graph[vertex] - set(path):
                    if next in all_leaves:
                        paths.append(path + [next])
                    else:
                        stack.append((next, path + [next]))
        return paths

    def validate(self, graph=None):
        """ Returns (Boolean, message) of whether DAG is valid. """
        graph = graph if graph is not None else self.graph
        if len(self.ind_nodes(graph)) == 0:
            return (False, 'no independent nodes detected')
        try:
            self.topological_sort(graph)
        except ValueError:
            return (False, 'failed topological sort')
        return (True, 'valid')


    def _dependencies(self, target_node, graph):
        """ Returns a list of all nodes from incoming edges. """
        if graph is None:
            raise Exception("Graph given is None")
        result = set()
        for node, outgoing_nodes in graph.items():
            if target_node in outgoing_nodes:
                result.add(node)
        return list(result)


    def topological_sort(self, graph=None):
        """ Returns a topological ordering of the DAG.
        Raises an error if this is not possible (graph is not valid).
        """
        graph = deepcopy(graph if graph is not None else self.graph)
        l = []
        q = deepcopy(self.ind_nodes(graph))
        while len(q) != 0:
            n = q.pop(0)
            l.append(n)
            iter_nodes = deepcopy(graph[n])
            for m in iter_nodes:
                graph[n].remove(m)
                if len(self._dependencies(m, graph)) == 0:
                    q.append(m)

        if len(l) != len(graph.keys()):
            raise ValueError('graph is not acyclic')
        return l
