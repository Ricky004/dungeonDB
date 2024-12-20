package storage

import (
	"encoding/binary"

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
	for len(freed) > 0 {
		new := BNode{make([]byte, BTREE_PAGE_SIZE)}

		// construct the new node
		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			// reuse a pointer from the free list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			// or append a page to house the new node
			fl.head = fl.new(new)
		}
	}
	u.Assert(len(reuse) == 0)
}

// Returns the size stored in the first 2 bytes of the node's Data
func flnSize(node BNode) int {
    u.Assert(len(node.Data) >= 2, "BNode data too small for flnSize")
    return int(binary.LittleEndian.Uint16(node.Data[:2]))
}

// Returns the next pointer stored in bytes 2 to 10 of the node's Data
func flnNext(node BNode) uint64 {
    u.Assert(len(node.Data) >= 10, "BNode data too small for flnNext")
    return binary.LittleEndian.Uint64(node.Data[2:10])
}

// Returns the pointer at index idx from the node's Data
func flnPtr(node BNode, idx int) uint64 {
    start := 10 + idx*8
    u.Assert(len(node.Data) >= start+8, "BNode data too small for flnPtr")
    return binary.LittleEndian.Uint64(node.Data[start : start+8])
}

// Sets the total number of free list items in bytes 10 to 18
func flnSetTotal(node BNode, total uint64) {
    u.Assert(len(node.Data) >= 18, "BNode data too small for flnSetTotal")
    binary.LittleEndian.PutUint64(node.Data[10:18], total)
}

// Sets the pointer at index idx in the node's Data
func flnSetPtr(node BNode, idx int, ptr uint64) {
    start := 10 + idx*8
    u.Assert(len(node.Data) >= start+8, "BNode data too small for flnSetPtr")
    binary.LittleEndian.PutUint64(node.Data[start:start+8], ptr)
}

// Sets the header with size and next pointer
func flnSetHeader(node BNode, size uint16, next uint64) {
    u.Assert(len(node.Data) >= 10, "BNode data too small for flnSetHeader")
    binary.LittleEndian.PutUint16(node.Data[:2], size)
    binary.LittleEndian.PutUint64(node.Data[2:10], next)
}

