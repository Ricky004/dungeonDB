package storage

import (
	"bytes"
	"encoding/binary"

	u "github.com/Ricky004/dungeonDB/internal/utils"
)

type BNode struct {
	Data []byte
}

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on disk pages
	Get func(uint64) BNode // dereference a pointer
	New func(BNode) uint64 // allocate a new page
	Del func(uint64)       // deallocate a page
}

const (
	HEADER             = 4    // header (4 byte) store metadata of nodes
	BTREE_PAGE_SIZE    = 4096 // 4kb
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	// 8 = space reserved for a pointer or page number
	// 2 = space for storing number of entries(key and values) in the node
	// 4 = space for additional metadata (e.g. flags and alignment)
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	u.Assert(node1max <= BTREE_PAGE_SIZE, "Node size exceeds pagesize")
}

//	helper functions
//
// header
func (node BNode) Btype() uint16 {
	return binary.LittleEndian.Uint16(node.Data)
}

func (node BNode) Nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.Data[2:4])
}

func (node BNode) SetHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.Data[0:2], btype)
	binary.LittleEndian.PutUint16(node.Data[2:4], nkeys)
}

// pointers
func (node BNode) GetPtr(idx uint16) uint64 {
	u.Assert(idx < node.Nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.Data[pos:])
}

func (node BNode) SetPtr(idx uint16, val uint64) {
	u.Assert(idx < node.Nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.Data[pos:], val)
}

// offset list
func OffsetPos(node BNode, idx uint16) uint16 {
	u.Assert(1 <= idx && idx <= node.Nkeys())
	return HEADER + 8*node.Nkeys() + 2*(idx-1)
}

func (node BNode) GetOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.Data[OffsetPos(node, idx):])
}

func (node BNode) SetOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.Data[OffsetPos(node, idx):], offset)
}

// key-values
func (node BNode) KvPos(idx uint16) uint16 {
	u.Assert(idx <= node.Nkeys())
	return HEADER + 8*node.Nkeys() + 2*node.Nkeys() + node.GetOffset(idx)
}

func (node BNode) GetKey(idx uint16) []byte {
	u.Assert(idx < node.Nkeys())
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos:])
	return node.Data[pos+4:][:klen]
}

func (node BNode) GetVal(idx uint16) []byte {
	u.Assert(idx < node.Nkeys())
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.Data[pos+2:])
	return node.Data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) Nbytes() uint16 {
	return node.KvPos(node.Nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: bisect
func NodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.Nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.GetKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node
func LeafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()+1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.Nkeys()-idx)

}

// upadete the leaf
func LeafUpadate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()+1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.Nkeys()-idx)

}

// copy multiple KVs into the position
func NodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	u.Assert(srcOld+n <= old.Nkeys())
	u.Assert(dstNew+n <= new.Nkeys())
	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.SetPtr(dstNew+i, old.GetPtr(srcOld+i))
	}

	// ofsets
	dstBegin := new.GetOffset(dstNew)
	srcBegin := old.GetOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.GetOffset(srcOld+i) - srcBegin
		new.SetOffset(dstNew+i, offset)
	}

	// KVs
	begin := old.KvPos(srcOld)
	end := old.KvPos(srcOld + n)
	copy(new.Data[new.KvPos(dstNew):], old.Data[begin:end])
}

// copy a KV into the position
func NodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.SetPtr(idx, ptr)
	// KVs
	pos := new.KvPos(idx)
	binary.LittleEndian.PutUint16(new.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.Data[pos+2:], uint16(len(val)))
	copy(new.Data[pos+4:], key)
	copy(new.Data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.SetOffset(idx+1, new.GetOffset(idx)+4+uint16((len(key)+len(val))))
}

// insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func TreeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{
		Data: make([]byte, 2*BTREE_PAGE_SIZE),
	}

	// where to insert the key?
	idx := NodeLookupLE(node, key)
	// act depending on the node type
	switch node.Btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.GetKey(idx)) {
			// found the key, upadate it.
			LeafUpadate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			LeafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		NodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

func NodeInsert(
	tree *BTree, new BNode,
	node BNode, idx uint16,
	key []byte, val []byte,
) {
	// get and deallocate the kid node
	kptr := node.GetPtr(idx)
	knode := tree.Get(kptr)
	tree.Del(kptr)
	// recursive insertion to the kid node
	knode = TreeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := NodeSplit3(knode)
	// update the kid links
	NodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
func NodeSplit2(left BNode, right BNode, old BNode) {
	// Calculate the midpoint of the old node's data
	mid := len(old.Data) / 2

	// Copy the left half of the old node's data into the left node
	copy(left.Data, old.Data[:mid])

	// Copy the right half of the old node's data into the right node
	copy(right.Data, old.Data[mid:])

	// Trim excess data in both left and right nodes
	left.Data = left.Data[:mid]
	right.Data = right.Data[:len(old.Data)-mid]

	u.Assert(len(left.Data) <= len(left.Data))
}

// split a node if it's too big. the result are 1-3 nodes.
func NodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.Nbytes() <= BTREE_PAGE_SIZE {
		old.Data = old.Data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	NodeSplit2(left, right, old)
	if left.Btype() <= BTREE_PAGE_SIZE {
		left.Data = left.Data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	NodeSplit2(leftleft, middle, left)
	u.Assert(leftleft.Nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

// replace a link with multiple links
func NodeReplaceKidN(
	tree *BTree, new BNode, old BNode,
	idx uint16, kids ...BNode,
) {
	inc := uint16(len(kids))
	new.SetHeader(BNODE_NODE, old.Nkeys()+inc-1)
	NodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		NodeAppendKV(new, idx+uint16(i), tree.New(node), node.GetKey(0), nil)
	}
	NodeAppendRange(new, old, idx+inc, idx+1, old.Nkeys()-(idx+1))
}

// remove a key from a leaf node
func LeafDelete(new BNode, old BNode, idx uint16) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()-1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendRange(new, old, idx, idx+1, old.Nkeys()-(idx+1))
}

// delete a key from the tree
func TreeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key?
	idx := NodeLookupLE(node, key)
	// act depending on the node type
	switch node.Btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.GetKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		new := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		LeafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return NodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}

// part of the treeDelete() function
func NodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.GetPtr(idx)
	updated := TreeDelete(tree, tree.Get(kptr), key)
	if len(updated.Data) == 0 {
		return BNode{} // not found
	}
	tree.Del(kptr)

	new := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
	// check for merging
	mergeDir, sibling := ShouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		NodeMerge(merged, sibling, updated)
		tree.Del(node.GetPtr(idx - 1))
		NodeReplace2Kid(new, node, idx-1, tree.New(merged), merged.GetKey(0))
	case mergeDir > 0: // right
		merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		NodeMerge(merged, updated, sibling)
		tree.Del(node.GetPtr(idx + 1))
		NodeReplace2Kid(new, node, idx, tree.New(merged), merged.GetKey(0))
	case mergeDir == 0:
		u.Assert(updated.Nkeys() > 0)
		NodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// should the updated kid be merged with a sibling?
func ShouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.Nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.Get(node.GetPtr(idx - 1))
		merged := sibling.Nbytes() + updated.Nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.Nkeys() {
		sibling := tree.Get(node.GetPtr(idx + 1))
		merged := sibling.Nbytes() + updated.Nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

// merge 2 nodes into 1
func NodeMerge(new BNode, left BNode, right BNode) {
	new.SetHeader(left.Btype(), left.Nkeys()+right.Nkeys())
	NodeAppendRange(new, left, 0, 0, left.Nkeys())
	NodeAppendRange(new, right, left.Nkeys(), 0, right.Nkeys())
}

// The NodeReplace2Kid function replaces two children of a node with a single child during a merge operation.
func NodeReplace2Kid(new BNode, parentNode BNode, idx uint16, val uint64, key []byte) {
	// Create a temporary child node for the merged key and pointer
	tempChild := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
	tempChild.SetHeader(BNODE_LEAF, 1) // Assuming it's a single key node

	// Directly set the key and pointer in the tempChild node
	copy(tempChild.Data[:len(key)], key) // Copy the key to the start of the data
	tempChild.SetPtr(0, val)             // Set the pointer for the child node

	// Update the tempChild's metadata if necessary (e.g., byte size, nkeys)
	// Ensure any header fields are consistent with your implementation.

	// Use nodeReplaceKidN to replace the two child nodes with the new merged child
	NodeReplaceKidN(nil, new, parentNode, idx, tempChild)
}

// root node
func (tree *BTree) Delete(key []byte) bool {
	u.Assert(len(key) != 0)
	u.Assert(len(key) <= BTREE_MAX_KEY_SIZE)
	if tree.root == 0 {
		return false
	}
	updated := TreeDelete(tree, tree.Get(tree.root), key)
	if len(updated.Data) == 0 {
		return false // not found
	}
	tree.Del(tree.root)
	if updated.Btype() == BNODE_NODE && updated.Nkeys() == 1 {
		// remove a level
		tree.root = updated.GetPtr(0)
	} else {
		tree.root = tree.New(updated)
	}
	return true
}

// the final interface for insertion
func (tree *BTree) Insert(key []byte, val []byte) {
	u.Assert(len(key) != 0)
	u.Assert(len(key) <= BTREE_MAX_KEY_SIZE)
	u.Assert(len(val) <= BTREE_MAX_VAL_SIZE)
	if tree.root == 0 {
		// create the first node
		root := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		root.SetHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		NodeAppendKV(root, 0, 0, nil, nil)
		NodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.New(root)
		return
	}
	node := tree.Get(tree.root)
	tree.Del(tree.root)
	node = TreeInsert(tree, node, key, val)
	nsplit, splitted := NodeSplit3(node)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		root.SetHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.New(knode), knode.GetKey(0)
			NodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.New(root)
	} else {
		tree.root = tree.New(splitted[0])
	}
}


