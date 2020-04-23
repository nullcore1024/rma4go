package main

import (
	"fmt"
	"github.com/nullcore1024/treeprint"
)

func main() {
	tree := treeprint.New()

	// create a new branch in the root
	one := tree.AddBranch("one")

	// add some nodes
	one.AddNode("subnode1").AddNode("subnode2")

	// create a new sub-branch
	one.AddBranch("two").
		AddNode("subnode1").AddNode("subnode2"). // add some nodes
		AddBranch("three").                      // add a new sub-branch
		AddNode("subnode1").AddNode("subnode2")  // add some nodes too

	// add one more node that should surround the inner branch
	one.AddNode("subnode3")

	m := one.GetMetaValue()
	fmt.Println(m)

	// add a new node to the root
	tree.AddNode("outernode")

	fmt.Println(tree.String())
}
