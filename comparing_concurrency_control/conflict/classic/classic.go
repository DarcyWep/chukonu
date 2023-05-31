package classic

import (
	"chukonu/comparing_concurrency_control/conflict/nezha"
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
)

func Classic(txs []*transaction.Transaction) []bool {
	cgtxs := make(classicGraphTxs, 0)
	for _, tx := range txs {
		cgtx := newClassicGraphTx(*tx.Hash, tx.ExecutionTime, tx.Index)
		cgtx.generateReadAndWrite(tx)
		cgtxs = append(cgtxs, cgtx)
	}

	graph := buildConflictGraph(cgtxs)

	//graph := [][]int{
	//	{1},
	//	{2, 4, 7},
	//	{3, 6},
	//	{2, 4, 5, 6},
	//	{5},
	//	{4},
	//	{0, 3},
	//	{0, 2},
	//}

	//var (
	//	deleteNodes = make([]bool, len(graph)) // 需要删除的结点
	//	visitNodes  = make([]bool, len(graph)) // 已经遍历了的结点
	//)
	//tarjan := nezha.NewTarjanSCC(&graph)
	//tarjan.SCC()
	//for _, scc := range tarjan.GetSCCs() {
	//	//fmt.Println(scc.Vertices)
	//	if len(scc.Vertices) == 1 {
	//		continue
	//	}
	//	deleteLoop(&graph, scc.Vertices, &deleteNodes, &visitNodes)
	//}
	//
	//tidyGraph(&graph, deleteNodes)
	//

	ls := newLoopSolving(&graph)
	ls.removeLoops()
	//TopologicalSort(graph, ls.deleteNodes)
	return ls.deleteNodes
}

func buildConflictGraph(cgtxs classicGraphTxs) [][]int {
	var gSlice = make([][]int, len(cgtxs))

	for i, cgtx := range cgtxs {

		for j := i + 1; j < len(cgtxs); j++ {
			for _, addr := range cgtxs[j].writeAddress {
				if isExistAddrInSlice(cgtx.readAddresses, addr) {
					// build r-w dependency
					if !isExistIntInSlice(gSlice[i], j) {
						gSlice[i] = append(gSlice[i], j)
					}
				} else if isExistAddrInSlice(cgtx.writeAddress, addr) {
					// build w-w dependency
					if !isExistIntInSlice(gSlice[i], j) {
						gSlice[i] = append(gSlice[i], j)
					}
				}
			}
		}

		for k := i - 1; k >= 0; k-- {
			for _, addr := range cgtxs[k].writeAddress {
				if isExistAddrInSlice(cgtx.writeAddress, addr) {
					// build r-w dependency
					if !isExistIntInSlice(gSlice[i], k) {
						gSlice[i] = append(gSlice[i], k)
					}
				}
			}
		}
	}

	return gSlice
}

func deleteLoop(graph *[][]int, nodes []int, deleteNodes, visitNodes *[]bool) {
	var parent = make([]int, 0) // 当前结点的所有父节点

	//fmt.Println(len(*graph))
	for _, node := range nodes {
		if !(*visitNodes)[node] {
			dfs(graph, nodes, node, visitNodes, deleteNodes, &parent)
		}
	}
}

func dfs(graph *[][]int, nodes []int, father int, visitNodes, deleteNodes *[]bool, parent *[]int) {
	*parent = append(*parent, father) // push

	(*visitNodes)[father] = true
	for i, child := range (*graph)[father] {
		// 孩子并不在该连通图中，直接跳过
		if !isExistIntInSlice(nodes, child) {
			continue
		}

		// 子结点已移除
		if child == -1 {
			continue
		}

		// 如果子结点已被删除，则继续下一个子结点
		if (*deleteNodes)[child] {
			(*visitNodes)[child] = true
			(*graph)[father][i] = -1
			continue
		}

		if isLoop(graph, parent, child) { // 如果子结点的子结点是父节点，那么这个子结点会导致环的出现，需要删除
			(*deleteNodes)[child] = true
			(*graph)[child] = make([]int, 0)
		} else { // 这个子结点不会成环，继续dfs
			dfs(graph, nodes, child, visitNodes, deleteNodes, parent)
		}
	}

	*parent = (*parent)[:len(*parent)-1] // pop
}

func isLoop(graph *[][]int, parent *[]int, child int) bool {
	for _, childChild := range (*graph)[child] {
		if isExistIntInSlice(*parent, childChild) { // 如果子结点的子结点是父节点，那么这个子结点会导致环的出现
			return true
		}
	}
	return false
}

// tidyGraph Remove delete node from the graph
func tidyGraph(graph *[][]int, deleteNodes []bool) {
	for i, children := range *graph {
		if len(children) == 0 {
			continue
		}
		newChildren := make([]int, 0)
		for _, child := range children {
			if child != -1 && !deleteNodes[child] { // 结点没有被删除
				newChildren = append(newChildren, child)
			}
		}
		(*graph)[i] = newChildren
	}

	//for _, children := range *graph {
	//	fmt.Println(children)
	//}
}

func TopologicalSort(graph [][]int, deleteNodes []bool) {
	var (
		inDegree        = make([]int, len(graph)) // 每个结点的入度
		topologicalSort = make([]int, 0)          // 入度为0的结点
		abortSum        = 0
	)

	for node, children := range graph {
		if deleteNodes[node] { // node has been deleted, continue
			inDegree[node] = -1
			abortSum += 1
			continue
		}
		for _, child := range children {
			if deleteNodes[child] { // node has been deleted, continue
				continue
			}
			inDegree[child] += 1 // 孩子的入度+1
		}
	}

	for {
		var isHaveTopoNode = false
		//fmt.Println(inDegree)
		//fmt.Println(topologicalSort)
		for node, degree := range inDegree {
			if degree != 0 { // 还不能排序
				continue
			}
			isHaveTopoNode = true
			topologicalSort = append(topologicalSort, node)
			inDegree[node] = -1

			for _, child := range graph[node] {
				if deleteNodes[child] { // node has been deleted, continue
					continue
				}
				inDegree[child] -= 1 // 孩子的入度+1
			}
		}
		if !isHaveTopoNode { // 已经没有入度为0的结点了
			break
		}
	}

	if len(graph)-abortSum == len(topologicalSort) {
		fmt.Println("完美的拓扑排序！")
	} else {
		fmt.Println("你的环好像没有完全去掉诶！")
	}
}

func isExistAddrInSlice(addresses []common.Address, address common.Address) bool {
	for _, addr := range addresses {
		if address == addr {
			return true
		}
	}
	return false
}

func isExistIntInSlice(numbers []int, number int) bool {
	for _, num := range numbers {
		if number == num {
			return true
		}
	}
	return false
}

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
