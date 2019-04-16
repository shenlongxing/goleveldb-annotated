// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type tSet struct {
	level int
	table *tFile
}

type version struct {
	s *session

	levels []tFiles // 各个level的sst文件

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed. These fields
	// are initialized by computeCompaction()
	cLevel int     // 下一次compaction的level
	cScore float64 // 各个level最大的cscore值

	cSeek unsafe.Pointer // TODO

	closing  bool
	ref      int
	released bool
}

func newVersion(s *session) *version {
	return &version{s: s}
}

func (v *version) incref() {
	if v.released {
		panic("already released")
	}

	v.ref++
	if v.ref == 1 {
		// Incr file ref.
		for _, tt := range v.levels {
			for _, t := range tt {
				v.s.addFileRef(t.fd, 1)
			}
		}
	}
}

func (v *version) releaseNB() {
	v.ref--
	if v.ref > 0 {
		return
	} else if v.ref < 0 {
		panic("negative version ref")
	}

	for _, tt := range v.levels {
		for _, t := range tt {
			if v.s.addFileRef(t.fd, -1) == 0 {
				v.s.tops.remove(t)
			}
		}
	}

	v.released = true
}

func (v *version) release() {
	v.s.vmu.Lock()
	v.releaseNB()
	v.s.vmu.Unlock()
}

// 从sst文件中查找key，顺序是level从小到大
// 使用的是闭包，第一个匿名函数返回的是单个文件的查找结果
func (v *version) walkOverlapping(aux tFiles, ikey internalKey, f func(level int, t *tFile) bool, lf func(level int) bool) {
	ukey := ikey.ukey()

	// Aux level.
	if aux != nil {
		for _, t := range aux {
			if t.overlaps(v.s.icmp, ukey, ukey) {
				if !f(-1, t) {
					return
				}
			}
		}

		if lf != nil && !lf(-1) {
			return
		}
	}

	// Walk tables level-by-level.
	// level从小到大
	for level, tables := range v.levels {
		if len(tables) == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other. Find all files that
			// overlap ukey.
			/*
			 * level 0存在overlap，所以需要遍历全部文件
			 * 不管当前文件是否找到，都会继续后面的查找，一直到查完全部文件为止
			 */
			for _, t := range tables {
				// 判断ukey是否在sst文件的[imin，imax]范围内
				if t.overlaps(v.s.icmp, ukey, ukey) {
					// 文件查找中，找到/没找到返回的都是true，只有异常才返回false
					// 也就是说一旦某个文件查找异常，则退出
					if !f(level, t) {
						return
					}
				}
			}
		} else {
			// 对于level>0的情况，key只可能存在一个文件中，因此这里的查找逻辑与level 0也不同。
			// 只需要查找到范围包含key的文件即可。例如各文件的key范围如下：
			// [0,200],[201,400],[401,600],[601,800],[801,+∞]   待查找的ukey为500
			// 此时，imax大于ukey的有3个，但是只需要找到“最小”的即可，因为ukey只可能在这个文件中
			// searchMax实现的就是查找这个“最小”文件的功能
			if i := tables.searchMax(v.s.icmp, ikey); i < len(tables) {
				t := tables[i]
				// 检查ukey是不是大于文件的min key，是的话表示ukey在[imin,imax]范围内
				if v.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					// 注意：f()闭包中，对于level<=0和level>0的返回值规则是不同的
					// 对于level<=0，只有查找异常才会返回false，也就是只有查找异常才会退出
					// 但是level>0的场景，找到了返回false，也就是找到就直接返回
					if !f(level, t) {
						return
					}
				}
			}
		}

		// lf这个闭包的作用是判断level 0是否已经找到了，找到则直接返回
		// 因此看起来放这里是不合适的，直接放level == 0的条件下即可
		// TODO
		if lf != nil && !lf(level) {
			return
		}
	}
}

func (v *version) get(aux tFiles, ikey internalKey, ro *opt.ReadOptions, noValue bool) (value []byte, tcomp bool, err error) {
	if v.closing {
		return nil, false, ErrClosed
	}

	ukey := ikey.ukey()

	var (
		tset  *tSet // tset包含level和tFile
		tseek bool

		// Level-0.
		// 记录level中各个文件的查找情况，确保找到最新的kv
		zfound bool // 表示在level 0查找到了
		zseq   uint64
		zkt    keyType
		zval   []byte
	)

	err = ErrNotFound

	// Since entries never hop across level, finding key/value
	// in smaller level make later levels irrelevant.
	// walkOverlapping是查询全部level，其中后两个参数都是匿名函数构成的闭包
	// 第一个参数是实现在单个文件中查询，并将结果记录到fikey, fval中
	// 第二个匿名函数的作用是：

	// 注意：f()闭包中，对于level<=0和level>0的返回值规则是不同的
	// 对于level<=0，只有查找异常才会返回false，也就是只有查找异常才会退出
	// 但是level>0的场景，找到了返回false，也就是找到就直接返回
	v.walkOverlapping(aux, ikey, func(level int, t *tFile) bool {
		// TODO
		if level >= 0 && !tseek {
			if tset == nil {
				tset = &tSet{level, t} // 第一次进入到闭包时，设置tset
			} else {
				tseek = true
			}
		}

		var (
			fikey, fval []byte
			ferr        error
		)
		// noValue表示不用返回value，类似于exists
		// find/fineKey类似于skiplist的findGE，找到>=ikey的key
		// tops: tOps, table operations
		if noValue {
			fikey, ferr = v.s.tops.findKey(t, ikey, ro)
		} else {
			fikey, fval, ferr = v.s.tops.find(t, ikey, ro)
		}

		switch ferr {
		case nil:
		case ErrNotFound:
			return true
		default:
			err = ferr
			return false
		}

		// f开头的key，seq这类变量，表示从file中获取的
		if fukey, fseq, fkt, fkerr := parseInternalKey(fikey); fkerr == nil {
			// fukey == ukey，表示找到指定key
			if v.s.icmp.uCompare(ukey, fukey) == 0 {
				// Level <= 0 may overlaps each-other.
				if level <= 0 {
					// 如果是level 0，由于level 0的文件中key可能有重叠
					// 找到seq最大的，表示是最新的有效数据
					if fseq >= zseq {
						zfound = true
						zseq = fseq
						zkt = fkt
						zval = fval
					}
				} else {
					switch fkt {
					case keyTypeVal:
						value = fval
						err = nil
					case keyTypeDel:
					default:
						panic("leveldb: invalid internalKey type")
					}
					return false
				}
			}
		} else {
			err = fkerr
			return false
		}

		return true
	}, func(level int) bool { // TODO，这个level的参数似乎没用到
		if zfound {
			switch zkt {
			case keyTypeVal:
				value = zval
				err = nil
			case keyTypeDel:
			default:
				panic("leveldb: invalid internalKey type")
			}
			return false
		}

		return true
	})

	// consumeSeek就是把seekLeft递减
	if tseek && tset.table.consumeSeek() <= 0 {
		// tset中记录的是待执行compaction的文件信息：level+文件number
		// 并将是否需要执行compaction的结果给tcomp，并返回
		tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
	}

	return
}

func (v *version) sampleSeek(ikey internalKey) (tcomp bool) {
	var tset *tSet

	v.walkOverlapping(nil, ikey, func(level int, t *tFile) bool {
		if tset == nil {
			tset = &tSet{level, t}
			return true
		}
		if tset.table.consumeSeek() <= 0 {
			tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
		}
		return false
	}, nil)

	return
}

// 对于sst文件，生成两类的iterator
// 1. level 0文件由于key有重叠，所以每个文件新建一个table iterator
// 2. level N(N>0)，每个level生成一个indexed的iterator
func (v *version) getIterators(slice *util.Range, ro *opt.ReadOptions) (its []iterator.Iterator) {
	strict := opt.GetStrict(v.s.o.Options, ro, opt.StrictReader)
	for level, tables := range v.levels {
		if level == 0 {
			// Merge all level zero files together since they may overlap.
			// level 0的每个文件生成一个tops(table)类型的iterator
			for _, t := range tables {
				its = append(its, v.s.tops.newIterator(t, slice, ro))
			}
		} else if len(tables) != 0 {
			// newIndexIterator生成一个index迭代器，再基于这个index生成一个indexed iterator
			its = append(its, iterator.NewIndexedIterator(tables.newIndexIterator(v.s.tops, v.s.icmp, slice, ro), strict))
		}
	}
	return
}

func (v *version) newStaging() *versionStaging {
	return &versionStaging{base: v}
}

// Spawn a new version based on this version.
func (v *version) spawn(r *sessionRecord) *version {
	staging := v.newStaging()
	staging.commit(r)
	return staging.finish()
}

func (v *version) fillRecord(r *sessionRecord) {
	for level, tables := range v.levels {
		for _, t := range tables {
			r.addTableFile(level, t)
		}
	}
}

func (v *version) tLen(level int) int {
	if level < len(v.levels) {
		return len(v.levels[level])
	}
	return 0
}

func (v *version) offsetOf(ikey internalKey) (n int64, err error) {
	for level, tables := range v.levels {
		for _, t := range tables {
			if v.s.icmp.Compare(t.imax, ikey) <= 0 {
				// Entire file is before "ikey", so just add the file size
				n += t.size
			} else if v.s.icmp.Compare(t.imin, ikey) > 0 {
				// Entire file is after "ikey", so ignore
				if level > 0 {
					// Files other than level 0 are sorted by meta->min, so
					// no further files in this level will contain data for
					// "ikey".
					break
				}
			} else {
				// "ikey" falls in the range for this table. Add the
				// approximate offset of "ikey" within the table.
				if m, err := v.s.tops.offsetOf(t, ikey); err == nil {
					n += m
				} else {
					return 0, err
				}
			}
		}
	}

	return
}

func (v *version) pickMemdbLevel(umin, umax []byte, maxLevel int) (level int) {
	if maxLevel > 0 {
		if len(v.levels) == 0 {
			return maxLevel
		}
		/* 新生成的sst文件与当前level 0的文件是否有overlap */
		if !v.levels[0].overlaps(v.s.icmp, umin, umax, true) {
			var overlaps tFiles
			for ; level < maxLevel; level++ {
				if pLevel := level + 1; pLevel >= len(v.levels) {
					return maxLevel
				} else if v.levels[pLevel].overlaps(v.s.icmp, umin, umax, false) {
					/* overlap则直接返回 */
					break
				}
				if gpLevel := level + 2; gpLevel < len(v.levels) {
					overlaps = v.levels[gpLevel].getOverlaps(overlaps, v.s.icmp, umin, umax, false)
					if overlaps.size() > int64(v.s.o.GetCompactionGPOverlaps(level)) {
						break
					}
				}
			}
		}
	}
	return
}

func (v *version) computeCompaction() {
	// Precomputed best level for next compaction
	bestLevel := int(-1)
	bestScore := float64(-1)

	statFiles := make([]int, len(v.levels))
	statSizes := make([]string, len(v.levels))
	statScore := make([]string, len(v.levels))
	statTotSize := int64(0)

	for level, tables := range v.levels {
		var score float64
		size := tables.size()
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compaction.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(tables)) / float64(v.s.o.GetCompactionL0Trigger())
		} else {
			score = float64(size) / float64(v.s.o.GetCompactionTotalSize(level))
		}

		if score > bestScore {
			bestLevel = level
			bestScore = score
		}

		statFiles[level] = len(tables)
		statSizes[level] = shortenb(int(size))
		statScore[level] = fmt.Sprintf("%.2f", score)
		statTotSize += size
	}

	v.cLevel = bestLevel
	v.cScore = bestScore

	v.s.logf("version@stat F·%v S·%s%v Sc·%v", statFiles, shortenb(int(statTotSize)), statSizes, statScore)
}

// 当compaction score大于1，需要compaction
func (v *version) needCompaction() bool {
	return v.cScore >= 1 || atomic.LoadPointer(&v.cSeek) != nil
}

type tablesScratch struct {
	added   map[int64]atRecord
	deleted map[int64]struct{}
}

type versionStaging struct {
	base   *version
	levels []tablesScratch
}

func (p *versionStaging) getScratch(level int) *tablesScratch {
	if level >= len(p.levels) {
		newLevels := make([]tablesScratch, level+1)
		copy(newLevels, p.levels)
		p.levels = newLevels
	}
	return &(p.levels[level])
}

func (p *versionStaging) commit(r *sessionRecord) {
	// Deleted tables.
	for _, r := range r.deletedTables {
		scratch := p.getScratch(r.level)
		if r.level < len(p.base.levels) && len(p.base.levels[r.level]) > 0 {
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			scratch.deleted[r.num] = struct{}{}
		}
		if scratch.added != nil {
			delete(scratch.added, r.num)
		}
	}

	// New tables.
	for _, r := range r.addedTables {
		scratch := p.getScratch(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}
	}
}

func (p *versionStaging) finish() *version {
	// Build new version.
	nv := newVersion(p.base.s)
	numLevel := len(p.levels)
	if len(p.base.levels) > numLevel {
		numLevel = len(p.base.levels)
	}
	nv.levels = make([]tFiles, numLevel)
	for level := 0; level < numLevel; level++ {
		var baseTabels tFiles
		if level < len(p.base.levels) {
			baseTabels = p.base.levels[level]
		}

		if level < len(p.levels) {
			scratch := p.levels[level]

			var nt tFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(tFiles, 0, n)
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				nt = append(nt, t)
			}

			// New tables.
			for _, r := range scratch.added {
				nt = append(nt, tableFileFromRecord(r))
			}

			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.levels[level] = nt
			}
		} else {
			nv.levels[level] = baseTabels
		}
	}

	// Trim levels.
	n := len(nv.levels)
	for ; n > 0 && nv.levels[n-1] == nil; n-- {
	}
	nv.levels = nv.levels[:n]

	// Compute compaction score for new version.
	// 计算cScore/clevel
	nv.computeCompaction()

	return nv
}

type versionReleaser struct {
	v    *version
	once bool
}

func (vr *versionReleaser) Release() {
	v := vr.v
	v.s.vmu.Lock()
	if !vr.once {
		v.releaseNB()
		vr.once = true
	}
	v.s.vmu.Unlock()
}
