package dag

import "sort"

// Node represents one executable step in a pipeline graph.
type Node struct {
	ID       string
	Name     string
	Type     string
	Config   []byte
	InDegree int
}

// Graph is a directed acyclic graph for pipeline execution planning.
type Graph struct {
	nodes       map[string]*Node
	dependsOn   map[string][]string
	dependents  map[string][]string
	insertOrder []string
}

func NewGraph() *Graph {
	return &Graph{
		nodes:      make(map[string]*Node),
		dependsOn:  make(map[string][]string),
		dependents: make(map[string][]string),
	}
}

func (g *Graph) AddNode(n *Node) {
	if _, exists := g.nodes[n.ID]; !exists {
		g.insertOrder = append(g.insertOrder, n.ID)
	}
	g.nodes[n.ID] = n
	if _, ok := g.dependsOn[n.ID]; !ok {
		g.dependsOn[n.ID] = []string{}
	}
	if _, ok := g.dependents[n.ID]; !ok {
		g.dependents[n.ID] = []string{}
	}
}

// AddDependency creates an edge dep -> nodeID, meaning nodeID depends on dep.
func (g *Graph) AddDependency(nodeID, dep string) {
	g.dependsOn[nodeID] = append(g.dependsOn[nodeID], dep)
	g.dependents[dep] = append(g.dependents[dep], nodeID)
	if node, ok := g.nodes[nodeID]; ok {
		node.InDegree++
	}
}

func (g *Graph) Node(id string) (*Node, bool) {
	n, ok := g.nodes[id]
	return n, ok
}

func (g *Graph) Nodes() []*Node {
	res := make([]*Node, 0, len(g.insertOrder))
	for _, id := range g.insertOrder {
		res = append(res, g.nodes[id])
	}
	return res
}

func (g *Graph) DependsOn(id string) []string {
	deps := append([]string(nil), g.dependsOn[id]...)
	sort.Strings(deps)
	return deps
}

func (g *Graph) Dependents(id string) []string {
	deps := append([]string(nil), g.dependents[id]...)
	sort.Strings(deps)
	return deps
}

// Roots returns node IDs that can execute first (no dependencies).
func (g *Graph) Roots() []string {
	roots := make([]string, 0)
	for _, id := range g.insertOrder {
		if g.nodes[id].InDegree == 0 {
			roots = append(roots, id)
		}
	}
	return roots
}

// TopologicalSort returns a valid execution order or false if a cycle exists.
func (g *Graph) TopologicalSort() ([]string, bool) {
	inDeg := make(map[string]int, len(g.nodes))
	for id, n := range g.nodes {
		inDeg[id] = n.InDegree
	}

	queue := make([]string, 0)
	for _, id := range g.insertOrder {
		if inDeg[id] == 0 {
			queue = append(queue, id)
		}
	}

	order := make([]string, 0, len(g.nodes))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		order = append(order, cur)

		for _, dep := range g.dependents[cur] {
			inDeg[dep]--
			if inDeg[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if len(order) != len(g.nodes) {
		return nil, false
	}
	return order, true
}
