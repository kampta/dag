import json
import traceback
from copy import copy, deepcopy

class DAGValidationError(Exception):
    pass

class DAG(object):
    """ Directed acyclic graph implementation. """

    def __init__(self):
        """ Construct a new DAG with no nodes or edges. """
        self.graph = {}
        self.levels = {}
        self.max_level = -1

    def add_node(self, node_name):
        """ Add a node if it does not exist yet, or error out. """

        if node_name in self.graph:
            raise ValueError("node %s already exists" % node_name)

        self.graph[node_name] = set()

    def add_node_if_not_exists(self, node_name):
        try:
            self.add_node(node_name)
        except ValueError:
            pass

    def delete_node(self, node_name):
        """ Deletes this node and all edges referencing it. """

        if node_name not in graph:
            raise KeyError('node %s does not exist' % node_name)

        self.graph.pop(node_name)

        for node, edges in self.graph.items():
            if node_name in edges:
                edges.remove(node_name)

    def delete_node_if_exists(self, node_name):
        try:
            self.delete_node(node_name)
        except KeyError:
            pass

    def add_edge(self, ind_node, dep_node):
        """ Add an edge (dependency) between the specified nodes. """
        try:
            if ind_node not in self.graph or dep_node not in self.graph:
                raise KeyError('one or more nodes do not exist in graph')

            if dep_node in self.graph[ind_node]:
                return

            self.graph[ind_node].add(dep_node)
            is_valid, message = self.validate()

            if not is_valid:
                self.graph[ind_node].remove(dep_node)
                print(message)

        except DAGValidationError:
            self.graph[ind_node].remove(dep_node)
            print(traceback.print_exc())


    def delete_edge(self, ind_node, dep_node):
        """ Delete an edge from the graph. """

        if dep_node not in self.graph.get(ind_node, []):
            raise KeyError('this edge does not exist in graph')

        self.graph[ind_node].remove(dep_node)

    def predecessors(self, node):
        """ Returns a list of all predecessors of the given node """
        
        return [key for key in self.graph if node in self.graph[key]]

    def upstream(self, node):
        """ Returns path from root to node
        """
        if node not in self.graph:
            return []
            
        depth = self.depth(node)
        path = [node]
        while depth > -1:
            depth -= 1
            possible_parents = self.get_nodes_at_depth(depth)

            for possible_parent in possible_parents:
                if path[-1] in self.graph[possible_parent]:
                    path.append(possible_parent)
                    continue

        return path

    def downstream(self, node):
        """ Returns a list of all nodes this node has edges towards. """
        
        if node not in self.graph:
            raise KeyError('node %s is not in graph' % node)
        return list(self.graph[node])

    def all_downstreams(self, node):
        """Returns a list of all nodes ultimately downstream
        of the given node in the dependency graph, in
        topological order."""
        
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            downstreams = self.downstream(nodes[i])
            for downstream_node in downstreams:
                if downstream_node not in nodes_seen:
                    nodes_seen.add(downstream_node)
                    nodes.append(downstream_node)
            i += 1
        return filter(lambda node: node in nodes_seen, self.topological_sort())

    def root(self):
        """ Return a list of all leaves (nodes with no downstreams) """
        
        return [key for key in self.graph if not self.graph[key]]

    def all_leaves(self):
        """ Return a list of all leaves (nodes with no downstreams) """
        
        return [key for key in self.graph if not self.graph[key]]

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

        self.build_levels()

    def from_json(self, js, start=None):
        """ Reset the graph and build it from the passed json.
        The leaves of json are empty dict, for example
        {
            "A": {
                "A_1" : {},
                "A_2" : {
                    "A_2_1": {},
                    "A_2_2": {}
                }
            }
        }
        """

        self.reset_graph()

        if start is not None:
            start_nodes = [start]
        else:
            start_nodes = [key for key in js]

        for start_node in start_nodes:
            for parent, child in self.json2edges(start_node, js[start_node]):
                self.add_node_if_not_exists(parent)
                self.add_node_if_not_exists(child)
                self.add_edge(parent, child)

        self.build_levels()

    def json2edges(self, parent, children):
        iters = [(parent, children)]
        
        while iters:
            parent, children = iters.pop()
            for k, v in children.items():
                if isinstance(v, dict) and len(v)>0:
                    iters.append((k, v))
                yield (parent, k)

    def reset_graph(self):
        """ Restore the graph to an empty state. """
        self.graph = {}


    def ind_nodes(self):
        """ Returns a list of all nodes in the graph with no dependencies. """
        
        all_nodes, dependent_nodes = set(self.graph.keys()), set()
        for downstream_nodes in self.graph.values():
            [dependent_nodes.add(node) for node in downstream_nodes]
        return list(all_nodes - dependent_nodes)


    def all_paths(self, start=None):
        paths = []
        
        if start is None:
            start_nodes = self.ind_nodes()
        else:
            start_nodes = [start]

        all_leaves = self.all_leaves()

        for start in start_nodes:
            stack = [(start, [start])]
            while stack:
                (vertex, path) = stack.pop()
                for next in self.graph[vertex] - set(path):
                    if next in all_leaves:
                        paths.append(path + [next])
                    else:
                        stack.append((next, path + [next]))
        return paths

    def validate(self):
        """ Returns (Boolean, message) of whether DAG is valid. """

        if len(self.ind_nodes()) == 0:
            return (False, 'no independent nodes detected')
        try:
            self.topological_sort()

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


    def topological_sort(self):
        """ Returns a topological ordering of the DAG.
        Raises an error if this is not possible (graph is not valid).
        """
        
        graph = deepcopy(self.graph)

        l = []
        q = deepcopy(self.ind_nodes())
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

    def build_levels(self):
        """ Builds a dictionary with depth of each node
        """

        self.levels = {}
        batch = set(self.ind_nodes())
        if len(batch) == 0:
            raise ValueError("graph doesn't have a root")

        level = 0
        next_batch = set([])
        while batch:
            node = batch.pop()
            if node not in self.levels:
                self.levels[node] = level
                for child in self.graph[node]:
                    if child not in self.levels:
                        next_batch.add(child)

            if len(batch) == 0:
                batch.update(next_batch)
                next_batch = set([])
                level+=1

        self.max_level = max(self.levels.values())

    def get_nodes_at_depth(self, level):
        """ Returns depth of node
        """

        return [key for key, val in self.levels.items() if val==level]

    def depth(self, node):
        """ Returns depth of node
        """
        return self.levels[node]