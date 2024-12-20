package storage

import (
	u "github.com/Ricky004/dungeonDB/internal/utils"
)

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(ptr uint64) BNode // dereference a pointer
	new func(BNode) uint64     // allocate a new page
	use func(uint64, BNode)    // reuse a page
}

// number of items in the list
func (fl *FreeList) Total() int {
	return 0
}

// get the nth pointer from the list
func (fl *FreeList) Get(topn int) uint64 {
	u.Assert(0 <= topn && topn < fl.Total())
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		u.Assert(next != 0)
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

func (fl *FreeList) Update(popn int, freed []uint64) {
	u.Assert(popn <= fl.Total())
	if popn == 0 && len(freed) == 0 {
		return // nothing to do
	}

	// prepare the new list
	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recycle the node itself
		if popn >= flnSize(node) {
			// phase 1
			// remove all the pointers from the node
			popn -= flnSize(node)
		} else {
			// phase 2
			// remove some pointers from the node
			remain := flnSize(node) - popn
			popn = 0
			// reuse pointers from the free list itself
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the 'freed' list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		// discard the node and move to the next one
		total -= flnSize(node)
		fl.head = flnNext(node)
	}
	u.Assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0)

	// phase 3: prepend new nodes
	flPush(fl, freed, reuse)
	// done
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))

}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	// code
}

func flnSize(node BNode) int {
	return 0
}

func flnNext(node BNode) uint64 {
	return 0
}

func flnPtr(node BNode, idx int) uint64 {
	return 0
}

func flnSetTotal(node BNode, total uint64) {
	// code
}
