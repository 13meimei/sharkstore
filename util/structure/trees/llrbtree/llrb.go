// Implements Left-Leaning Red Black trees.
// Structure is not thread safe.
package llrbtree

import (
	"util/compare"
)

type (
	LLRBMode byte

	color bool //represents the color of a Node
)

const (
	LLRB234 = LLRBMode(0)
	LLRB23  = LLRBMode(1)

	// Red as false give us the defined behaviour that new nodes are red. Although this
	// is incorrect for the root node, that is resolved on the first insertion.
	red   color = false
	black color = true
)

func (c color) String() string {
	if c {
		return "Black"
	}
	return "Red"
}

// A Node represents a node in the LLRB tree.
type node struct {
	mode        LLRBMode
	color       color
	key         compare.Comparable
	value       interface{}
	left, right *node
}

// A Tree manages the root node of an LLRB tree. Public methods are exposed through this type.
type Tree struct {
	mode  LLRBMode
	root  *node // Root node of the tree.
	count int   // Number of elements stored.
}

func New(mode LLRBMode) *Tree {
	return &Tree{mode: mode}
}

func (t *Tree) Len() int {
	return t.count
}

func (t *Tree) Empty() bool {
	return t.count == 0
}

func (t *Tree) Clear() {
	t.root = nil
	t.count = 0
}

// Get returns the first match of q in the Tree. If insertion without
// replacement is used, this is probably not what you want.
func (t *Tree) Get(key compare.Comparable) (compare.Comparable, interface{}) {
	if t.root == nil {
		return nil, nil
	}
	n := t.root.search(key)
	if n == nil {
		return nil, nil
	}
	return n.key, n.value
}

func (n *node) search(key compare.Comparable) *node {
	for n != nil {
		switch c := key.Compare(n.key); {
		case c == 0:
			return n
		case c < 0:
			n = n.left
		default:
			n = n.right
		}
	}
	return n
}

func (t *Tree) Insert(key compare.Comparable, value interface{}) {
	var d int
	t.root, d = t.root.insert(key, value, t.mode)
	t.count += d
	t.root.color = black
}

func (n *node) insert(key compare.Comparable, value interface{}, mode LLRBMode) (root *node, d int) {
	if n == nil {
		return &node{
			mode:  mode,
			key:   key,
			value: value,
		}, 1
	} else if n.key == nil {
		n.key, n.value, n.mode = key, value, mode
		return n, 1
	}
	if mode == LLRB234 {
		if n.left.getColor() == red && n.right.getColor() == red {
			n.flipColors()
		}
	}

	switch c := key.Compare(n.key); {
	case c == 0:
		n.key, n.value, n.mode = key, value, mode
	case c < 0:
		n.left, d = n.left.insert(key, value, mode)
	default:
		n.right, d = n.right.insert(key, value, mode)
	}

	if n.right.getColor() == red && n.left.getColor() == black {
		n = n.rotateLeft()
	}
	if n.left.getColor() == red && n.left.left.getColor() == red {
		n = n.rotateRight()
	}
	if mode == LLRB23 {
		if n.left.getColor() == red && n.right.getColor() == red {
			n.flipColors()
		}
	}

	root = n
	return
}

func (t *Tree) DeleteMin() {
	if t.root == nil {
		return
	}
	var d int
	t.root, d = t.root.deleteMin()
	t.count += d
	if t.root == nil {
		return
	}
	t.root.color = black
}

func (n *node) deleteMin() (root *node, d int) {
	if n.left == nil {
		return nil, -1
	}
	if n.left.getColor() == black && n.left.left.getColor() == black {
		n = n.moveRedLeft()
	}
	n.left, d = n.left.deleteMin()
	root = n.fixUp()
	return
}

func (t *Tree) DeleteMax() {
	if t.root == nil {
		return
	}
	var d int
	t.root, d = t.root.deleteMax()
	t.count += d
	if t.root == nil {
		return
	}
	t.root.color = black
}

func (n *node) deleteMax() (root *node, d int) {
	if n.left != nil && n.left.getColor() == red {
		n = n.rotateRight()
	}
	if n.right == nil {
		return nil, -1
	}
	if n.right.getColor() == black && n.right.left.getColor() == black {
		n = n.moveRedRight()
	}
	n.right, d = n.right.deleteMax()
	root = n.fixUp()
	return
}

func (t *Tree) Delete(key compare.Comparable) {
	if t.root == nil {
		return
	}
	var d int
	t.root, d = t.root.delete(key)
	t.count += d
	if t.root == nil {
		return
	}
	t.root.color = black
}

func (n *node) delete(key compare.Comparable) (root *node, d int) {
	if key.Compare(n.key) < 0 {
		if n.left != nil {
			if n.left.getColor() == black && n.left.left.getColor() == black {
				n = n.moveRedLeft()
			}
			n.left, d = n.left.delete(key)
		}
	} else {
		if n.left.getColor() == red {
			n = n.rotateRight()
		}
		if n.right == nil && key.Compare(n.key) == 0 {
			return nil, -1
		}
		if n.right != nil {
			if n.right.getColor() == black && n.right.left.getColor() == black {
				n = n.moveRedRight()
			}
			if key.Compare(n.key) == 0 {
				min := n.right.min()
				n.key, n.value = min.key, min.value
				n.right, d = n.right.deleteMin()
			} else {
				n.right, d = n.right.delete(key)
			}
		}
	}

	root = n.fixUp()
	return
}

func (t *Tree) Min() (compare.Comparable, interface{}) {
	if t.root == nil {
		return nil, nil
	}
	min := t.root.min()
	if min == nil {
		return nil, nil
	}
	return min.key, min.value
}

func (n *node) min() *node {
	for ; n.left != nil; n = n.left {
	}
	return n
}

func (t *Tree) Max() (compare.Comparable, interface{}) {
	if t.root == nil {
		return nil, nil
	}
	max := t.root.max()
	if max == nil {
		return nil, nil
	}
	return max.key, max.value
}

func (n *node) max() *node {
	for ; n.right != nil; n = n.right {
	}
	return n
}

// Floor returns the greatest value equal to or less than the query key according to key.Compare().
func (t *Tree) Floor(key compare.Comparable) (compare.Comparable, interface{}) {
	if t.root == nil {
		return nil, nil
	}
	n := t.root.floor(key)
	if n == nil {
		return nil, nil
	}
	return n.key, n.value
}

func (n *node) floor(key compare.Comparable) *node {
	if n == nil {
		return nil
	}
	switch c := key.Compare(n.key); {
	case c == 0:
		return n
	case c < 0:
		return n.left.floor(key)
	default:
		if r := n.right.floor(key); r != nil {
			return r
		}
	}
	return n
}

// Ceil returns the smallest value equal to or greater than the query q according to q.Compare().
func (t *Tree) Ceil(key compare.Comparable) (compare.Comparable, interface{}) {
	if t.root == nil {
		return nil, nil
	}
	n := t.root.ceil(key)
	if n == nil {
		return nil, nil
	}
	return n.key, n.value
}

func (n *node) ceil(key compare.Comparable) *node {
	if n == nil {
		return nil
	}
	switch c := key.Compare(n.key); {
	case c == 0:
		return n
	case c > 0:
		return n.right.ceil(key)
	default:
		if l := n.left.ceil(key); l != nil {
			return l
		}
	}
	return n
}

func (t *Tree) Keys() []compare.Comparable {
	keys := make([]compare.Comparable, 0, t.count)
	t.Do(func(key compare.Comparable, value interface{}) bool {
		keys = append(keys, key)
		return false
	})
	return keys
}

func (t *Tree) Values() []interface{} {
	values := make([]interface{}, 0, t.count)
	t.Do(func(key compare.Comparable, value interface{}) bool {
		values = append(values, value)
		return false
	})
	return values
}

// An Operation is a function that operates on a Comparable. If done is returned true, the
// Operation is indicating that no further work needs to be done and so the Do function should
// traverse no further.
type Operation func(key compare.Comparable, value interface{}) (done bool)

// Do performs fn on all values stored in the tree. A boolean is returned indicating whether the
// Do traversal was interrupted by an Operation returning true. If fn alters stored values' sort
// relationships, future tree operation behaviors are undefined.
func (t *Tree) Do(fn Operation) bool {
	if t.root == nil {
		return false
	}
	return t.root.do(fn)
}

func (n *node) do(fn Operation) (done bool) {
	if n.left != nil {
		done = n.left.do(fn)
		if done {
			return
		}
	}
	done = fn(n.key, n.value)
	if done {
		return
	}
	if n.right != nil {
		done = n.right.do(fn)
	}
	return
}

// DoReverse performs fn on all values stored in the tree, but in reverse of sort order. A boolean
// is returned indicating whether the Do traversal was interrupted by an Operation returning true.
// If fn alters stored values' sort relationships, future tree operation behaviors are undefined.
func (t *Tree) DoReverse(fn Operation) bool {
	if t.root == nil {
		return false
	}
	return t.root.doReverse(fn)
}

func (n *node) doReverse(fn Operation) (done bool) {
	if n.right != nil {
		done = n.right.doReverse(fn)
		if done {
			return
		}
	}
	done = fn(n.key, n.value)
	if done {
		return
	}
	if n.left != nil {
		done = n.left.doReverse(fn)
	}
	return
}

// DoRange performs fn on all values stored in the tree over the interval [from, to) from left
// to right. If to is less than from DoRange will panic. A boolean is returned indicating whether
// the Do traversal was interrupted by an Operation returning true. If fn alters stored values'
// sort relationships future tree operation behaviors are undefined.
func (t *Tree) DoRange(fn Operation, from, to compare.Comparable) bool {
	if t.root == nil {
		return false
	}
	if from.Compare(to) > 0 {
		panic("llrb: inverted range")
	}
	return t.root.doRange(fn, from, to)
}

func (n *node) doRange(fn Operation, lo, hi compare.Comparable) (done bool) {
	lc, hc := lo.Compare(n.key), hi.Compare(n.key)
	if lc <= 0 && n.left != nil {
		done = n.left.doRange(fn, lo, hi)
		if done {
			return
		}
	}
	if lc <= 0 && hc > 0 {
		done = fn(n.key, n.value)
		if done {
			return
		}
	}
	if hc > 0 && n.right != nil {
		done = n.right.doRange(fn, lo, hi)
	}
	return
}

// DoRangeReverse performs fn on all values stored in the tree over the interval (to, from] from
// right to left. If from is less than to DoRange will panic. A boolean is returned indicating
// whether the Do traversal was interrupted by an Operation returning true. If fn alters stored
// values' sort relationships future tree operation behaviors are undefined.
func (t *Tree) DoRangeReverse(fn Operation, from, to compare.Comparable) bool {
	if t.root == nil {
		return false
	}
	if from.Compare(to) < 0 {
		panic("llrb: inverted range")
	}
	return t.root.doRangeReverse(fn, from, to)
}

func (n *node) doRangeReverse(fn Operation, hi, lo compare.Comparable) (done bool) {
	lc, hc := lo.Compare(n.key), hi.Compare(n.key)
	if hc > 0 && n.right != nil {
		done = n.right.doRangeReverse(fn, hi, lo)
		if done {
			return
		}
	}
	if lc <= 0 && hc > 0 {
		done = fn(n.key, n.value)
		if done {
			return
		}
	}
	if lc <= 0 && n.left != nil {
		done = n.left.doRangeReverse(fn, hi, lo)
	}
	return
}

// DoMatch performs fn on all values stored in the tree that match q according to Compare, with
// q.Compare() used to guide tree traversal, so DoMatching() will out perform Do() with a called
// conditional function if the condition is based on sort order, but can not be reliably used if
// the condition is independent of sort order. A boolean is returned indicating whether the Do
// traversal was interrupted by an Operation returning true. If fn alters stored values' sort
// relationships, future tree operation behaviors are undefined.
func (t *Tree) DoMatching(fn Operation, q compare.Comparable) bool {
	if t.root == nil {
		return false
	}
	return t.root.doMatch(fn, q)
}

func (n *node) doMatch(fn Operation, q compare.Comparable) (done bool) {
	c := q.Compare(n.key)
	if c <= 0 && n.left != nil {
		done = n.left.doMatch(fn, q)
		if done {
			return
		}
	}
	if c == 0 {
		done = fn(n.key, n.value)
		if done {
			return
		}
	}
	if c >= 0 && n.right != nil {
		done = n.right.doMatch(fn, q)
	}
	return
}

func (n *node) getColor() color {
	if n == nil {
		return black
	}
	return n.color
}

// (a,c)b -rotL-> ((a,)b,)c
func (n *node) rotateLeft() (root *node) {
	root = n.right
	n.right = root.left
	root.left = n
	root.color = n.color
	n.color = red
	return
}

// (a,c)b -rotR-> (,(,c)b)a
func (n *node) rotateRight() (root *node) {
	root = n.left
	n.left = root.right
	root.right = n
	root.color = n.color
	n.color = red
	return
}

// (aR,cR)bB -flipC-> (aB,cB)bR | (aB,cB)bR -flipC-> (aR,cR)bB
func (n *node) flipColors() {
	n.color = !n.color
	n.left.color = !n.left.color
	n.right.color = !n.right.color
}

// fixUp ensures that black link balance is correct, that red nodes lean left,
// and that 4 nodes are split in the case of BU23 and properly balanced in TD234.
func (n *node) fixUp() *node {
	if n.right.getColor() == red {
		if n.mode == LLRB234 && n.right.left.getColor() == red {
			n.right = n.right.rotateRight()
		}
		n = n.rotateLeft()
	}
	if n.left.getColor() == red && n.left.left.getColor() == red {
		n = n.rotateRight()
	}
	if n.mode == LLRB23 && n.left.getColor() == red && n.right.getColor() == red {
		n.flipColors()
	}
	return n
}

func (n *node) moveRedLeft() *node {
	n.flipColors()
	if n.right.left.getColor() == red {
		n.right = n.right.rotateRight()
		n = n.rotateLeft()
		n.flipColors()
		if n.mode == LLRB234 && n.right.right.getColor() == red {
			n.right = n.right.rotateLeft()
		}
	}
	return n
}

func (n *node) moveRedRight() *node {
	n.flipColors()
	if n.left.left.getColor() == red {
		n = n.rotateRight()
		n.flipColors()
	}
	return n
}
