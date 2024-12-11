package integration

import (
	"unsafe"

	s "github.com/Ricky004/dungeonDB/internal/storage"
	u "github.com/Ricky004/dungeonDB/internal/utils"
)

type C struct {
	tree  s.BTree
	ref   map[string]string
	pages map[uint64]s.BNode
}

func newC() *C {
	pages := map[uint64]s.BNode{}
	return &C{
		tree: s.BTree{
			Get: func(ptr uint64) s.BNode {
				node, ok := pages[ptr]
				u.Assert(ok)
				return node
			},
			New: func(node s.BNode) uint64 {
				u.Assert(node.Nbytes() <= s.BTREE_PAGE_SIZE)
				key := uint64(uintptr(unsafe.Pointer(&node.Data[0])))
				u.Assert(pages[key].Data == nil)
				pages[key] = node
				return key
			},
			Del: func(ptr uint64) {
				_, ok := pages[ptr]
				u.Assert(ok)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}
