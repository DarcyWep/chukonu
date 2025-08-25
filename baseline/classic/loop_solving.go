package classic

import "chukonu/concurrency_control/conflict/nezha"

type loopSolving struct {
	graph       *[][]int
	scces       []nezha.SCC // Strongly Connected Components
	deleteNodes []bool
	parent      []int

	// cache area
	visitNodes       []bool
	childLoopDegree  map[int][]int // the degree of loop in child
	fatherLoopDegree map[int][]int // the degree of loop in father
}

func newLoopSolving(graph *[][]int) *loopSolving {
	tarjan := nezha.NewTarjanSCC(graph)
	tarjan.SCC()
	return &loopSolving{
		graph:       graph,
		deleteNodes: make([]bool, len(*graph)),
		parent:      make([]int, 0),
		scces:       tarjan.GetSCCs(),

		visitNodes:       make([]bool, len(*graph)),
		childLoopDegree:  make(map[int][]int),
		fatherLoopDegree: make(map[int][]int),
	}
}

func (ls *loopSolving) removeLoops() {
	for _, scc := range ls.scces {
		//fmt.Println(scc.Vertices)
		if len(scc.Vertices) == 1 { // just one node in Strongly Connected Components, not loop
			continue
		}
		ls.removeLoop(scc.Vertices)
	}
}

// removeLoop is remove loop of one scc
func (ls *loopSolving) removeLoop(nodes []int) {
	for {
		ls.fatherLoopDegree = make(map[int][]int)
		ls.childLoopDegree = make(map[int][]int)
		ls.visitNodes = make([]bool, len(*ls.graph))
		for _, node := range nodes {
			if !ls.deleteNodes[node] && !ls.visitNodes[node] { // 没有被删除，且没有在当前轮次被访问
				ls.dfs(nodes, node)
			}
		}
		if len(ls.childLoopDegree) == 0 && len(ls.fatherLoopDegree) == 0 { // 已无成环的结点
			break
		}
		deleteNode := ls.maxLoopDegreeNode()
		ls.deleteNodes[deleteNode] = true
	}
}

func (ls *loopSolving) dfs(nodes []int, father int) {
	ls.parent = append(ls.parent, father) // push

	ls.visitNodes[father] = true
	for _, child := range (*ls.graph)[father] {
		// 孩子并不在该连通图中，直接跳过
		if !isExistIntInSlice(nodes, child) {
			continue
		}

		// 子结点已移除
		if ls.deleteNodes[child] {
			continue
		}

		// 子结点是父节点，需建立环度, child node in the parent, build loop degree
		if ls.isLoop(child) {
			ls.addLoopDegree(father, child)
		} else if !ls.visitNodes[child] { // 这个子结点不是父节点且未被访问过，继续dfs
			ls.dfs(nodes, child)
		}
	}

	ls.parent = ls.parent[:len(ls.parent)-1] // pop
}

func (ls *loopSolving) isLoop(child int) bool {
	return isExistIntInSlice(ls.parent, child)
}

// addLoopDegree the child is the father's grandfather, so this is a loop
// and the father is belonged childLoopDegree, the child is belonged fatherLoopDegree
func (ls *loopSolving) addLoopDegree(father, child int) {
	if _, ok := ls.childLoopDegree[father]; !ok {
		ls.childLoopDegree[father] = make([]int, 0)
	}
	if _, ok := ls.fatherLoopDegree[child]; !ok {
		ls.fatherLoopDegree[child] = make([]int, 0)
	}

	ls.childLoopDegree[father] = append(ls.childLoopDegree[father], child)  // 子结点到父节点到出环度
	ls.fatherLoopDegree[child] = append(ls.fatherLoopDegree[child], father) // 父节点入环度
}

func (ls *loopSolving) maxLoopDegreeNode() int {
	var (
		maxFatherLoopDegreeNode, maxFatherLoopDegree = -1, -1
		maxChildLoopDegreeNode, maxChildLoopDegree   = -1, -1
		doubleLoopDegreeNode                         = make(map[int]int) // 某一结点如果同时有入环度 和 出环度，则优先删除
		maxDoubleLoopDegreeNode, maxDoubleLoopDegree = -1, -1
	)
	for node, degrees := range ls.fatherLoopDegree {
		//fmt.Println(node, degrees) // 查看是否有重复结点
		if len(degrees) > maxFatherLoopDegree {
			maxFatherLoopDegree = len(degrees)
			maxFatherLoopDegreeNode = node
		}
		doubleLoopDegreeNode[node] = 1
	}
	//fmt.Println()
	for node, degrees := range ls.childLoopDegree {
		//fmt.Println(node, degrees) // 查看是否有重复结点
		if len(degrees) > maxChildLoopDegree {
			maxChildLoopDegree = len(degrees)
			maxChildLoopDegreeNode = node
		}

		if _, ok := doubleLoopDegreeNode[node]; ok { // 同时有入环度 和 出环度
			doubleLoopDegreeNode[node] = 2
		}
	}
	for node, isDouble := range doubleLoopDegreeNode {
		if isDouble == 1 {
			continue
		}
		// 同时有入环度 和 出环度
		degree := len(ls.childLoopDegree[node]) + len(ls.fatherLoopDegree)
		if degree > maxDoubleLoopDegree {
			maxDoubleLoopDegree = degree
			maxDoubleLoopDegreeNode = node
		} else if degree == maxDoubleLoopDegree && node > maxDoubleLoopDegreeNode { // 优先删除叶子结点
			maxDoubleLoopDegreeNode = node
		}
	}
	if maxDoubleLoopDegreeNode != -1 {
		return maxDoubleLoopDegreeNode
	}
	// 选择一个最大环度的结点移除
	if maxFatherLoopDegree > maxChildLoopDegree {
		return maxFatherLoopDegreeNode
	} else {
		return maxChildLoopDegreeNode
	}
}
