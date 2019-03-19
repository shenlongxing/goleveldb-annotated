// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

/*
 * WAL文件结构如下：
          0x00       0x03     0x05   0x06
           ---------------------------------------------
         /| checksum | Length | type |      data        |  chunk1
        / |---------------------------------------------|
    block | checksum | Length | type |      data        |  chunk2
        \ |---------------------------------------------|
         \| checksum | Length | type |      data        |  chunk3
           ---------------------------------------------
	1. 文件按block进行划分，每个block大小为32KB
	2. 每个block包含若干个完整的chunk
	3. 每个chunk包含了一个7字节大小的header，前4字节是该chunk的checksum，
	   紧接的2字节是该chunk数据的长度，以及最后一个字节是该chunk的类型
	4. chunk共有四种类型：full、first、middle、last，表示这个chunk是完整还是部分记录
	5. 一条日志记录包含一个或多个chunk，chunk的type表示记录包含在一个还是多个chunk

	其中，chunk的data部分的结构如下：
	 -------------------------------------------
	| seq num | item num | batch | ... |  batch |
	 -------------------------------------------
	 seq num:  根据batch写的规则可知，一次merge写入的数据的seq是相同的
	 item num：本条log日志包含的写操作记录数(put/delete)
     batch：   编码后的batch内容
*/
func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal.Next()
	if err != nil {
		return err
	}
	// header就是chunk的前两个部分：seq num + item num
	// 先写header，然后写batch data
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}

	// TODO,基于writer的
	if err := db.journal.Flush(); err != nil {
		return err
	}
	if sync {
		return db.journalWriter.Sync()
	}
	return nil
}

// 进行minor compaction
func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	err = db.compTriggerWait(db.mcompCmdC)
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	mem, err = db.newMem(n)
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	// 上面创建了新的memtable，原memtable rotate成了immutable table
	// 再来一次minor compaction
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		db.compTrigger(db.mcompCmdC)
	}
	return
}

/* 这个函数主要干如下几件事：
 * 1. 判断L0的sst文件数，如果大于8个，则sleep 1ms，若大于12个，则阻塞写
 * 2. 判断当前memtable的剩余空间与待写入的数据量的大小关系
*     如果剩余 > 待写入的数据量：直接返回剩余的内存大小
      如果剩余 < 待写入的数据量: 触发一次minor compaction，申请新的memtable

*/
func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	// slowdown和pause trigger是触发level0 compaction的阈值，分别是8/12
	// 意义是如果L0的sst文件数超过8个，sleep 1ms，超过12个则禁止写，确保compaction成功
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger() // 8
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()       // 12
	// 匿名函数，等待compaction
	// memtable有足够内存，且L0的文件数低于8个时返回false，否则返回true
	flush := func() (retry bool) {
		// 获取memtable
		mdb = db.getEffectiveMem()
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry {
				mdb.decref()
				mdb = nil
			}
		}()
		// 通过session获取level 0的sst文件数量
		tLen := db.s.tLen(0)
		// 当前kvData这个slice剩余空间，即cap - len
		mdbFree = mdb.Free()
		switch {
		// L0文件数超过8个，则sleep 1ms，设置delay标识
		case tLen >= slowdownTrigger && !delayed:
			delayed = true
			time.Sleep(time.Millisecond)
		// memtable的可用内存大于待写入内存，不会触发compaction，直接返回
		case mdbFree >= n:
			return false
		// 暂停数据写入
		case tLen >= pauseTrigger:
			delayed = true
			// L0文件数超12个，则阻塞写
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			// tcompCmdC是用来trigger major compaction
			// 触发compaction(major)
			err = db.compTriggerWait(db.tcompCmdC)
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default:
			// 走到这个分支，表示memtable中内存不够写入batch
			// 且L0的sst文件数未到阈值
			// Allow memdb to grow if it has no entry.
			// Len表示kv对的个数
			if mdb.Len() == 0 {
				mdbFree = n
			} else {
				mdb.decref()
				// 触发minor compaction
				mdb, err = db.rotateMem(n, false)
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	// 循环执行匿名函数flush，一直等到memtable的free mem够装下batch的数据
	// flush函数返回false时退出
	for flush() {
	}
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		// 更新统计信息
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		// 写失败
		db.writeAckC <- err
	}
	if overflow {
		// Pass lock to the next write (that failed to merge).
		// overflow了，说明merge失败。释放writeMergedC锁，重新执行merge操作
		db.writeMergedC <- false
	} else {
		// Release lock.
		// 释放写锁，重新执行写操作
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	// 判断当年memtable剩余内存是否够写入kv，如果不够则触发compaction
	// 这里两种compaction都有可能发生：
	// 如果L0的文件数超过阈值(8/12)，则会sleep/阻塞写操作，这时触发major compaction
	// 如果L0文件数不超，但是memtable剩余内存不足，则触发一次minor compaction
	// 如果memtable剩余内存够，则直接返回剩余内存
	mdb, mdbFree, err := db.flush(batch.internalLen) // internalLen = keyLen + valueLen + 8
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch} // 使用batch初始化batches数组
	)

	if merge {
		// Merge limit.
		// 计算一次merge的总的大小的阈值，TODO：internalLen > 1M的场景呢？
		var mergeLimit int
		if batch.internalLen > 128<<10 {
			// 如果当前batch的大小超过128k，则batch的merge阈值设置为1M
			mergeLimit = (1 << 20) - batch.internalLen
		} else {
			// 如果当前batch的大小小于128k，则batch的merge阈值设置为128k
			mergeLimit = 128 << 10
		}
		// 计算memtable的剩余空间大小
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}

	merge:
		// 判断是否有需要merge的数据，即判断writeMergeC中是否有数据。
		// 因为有default分支，当没有可以merge的数据时，退出merge

		// 真正的merge操作，当merge的大小超过上面计算的mergeLimit，
		// 或者没有待merge的数据，则退出merge操作
		for mergeLimit > 0 {
			select {
			// 到这一步，表示有可以merge的数据
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil {
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						overflow = true
						break merge
					}
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else {
					// Merge put.
					// 如果writeMergeC中是raw kv数据，则先将kv塞到batch中，再执行append
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync
				merged++                // merge的batch的总数量
				db.writeMergedC <- true // 写writeMergedC，让阻塞等待的待merge的操作返回

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any.
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	// 由于leveldb中的修改/删除都不是in-place的，而是追加一条新的记录
	// 因此使用递增的seq表示数据的时效性
	seq := db.seq + 1

	// 先写wal日志
	// Write journal.
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}

	// Put batches.
	// 将batch写入到memtable
	for _, batch := range batches {
		if err := batch.putMem(seq, mdb.DB); err != nil {
			panic(err)
		}
		// 更新当前的seq
		// 注意：从这里可以看出，同一个batch的kv的seqid相同
		seq += uint64(batch.Len())
	}

	// Incr seq number.
	// TODO，为毛这里的addSeq不和上面的seq合并，当batches有一个写失败时，会不会导致seq不一致
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.
	if batch.internalLen >= mdbFree {
		// rotate是将memtable转成immutable table
		db.rotateMem(0, false)
	}

	// 释放各种锁
	db.unlockWrite(overflow, merged, nil)
	return nil
}

// Write apply the given batch to the DB. The batch records will be applied
// sequentially. Write might be used concurrently, when used concurrently and
// batch is small enough, write will try to merge the batches. Set NoWriteMerge
// option to true to disable write merge.
//
// It is safe to modify the contents of the arguments after Write returns but
// not before. Write will not modify content of the batch.
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}

	// If the batch size is larger than write buffer, it may justified to write
	// using transaction instead. Using transaction the batch will be written
	// into tables directly, skipping the journaling.
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	// 注意：这里的select没有default分支，即阻塞等待
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				// 阻塞等待merge的结果，如果merge成功则退出，否则继续获取写锁
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync)
}

func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {
		return err
	}

	// merge表示将多条记录进行合并，使用batch提交
	// 使用merge和非merge的区别在于：
	// 非merge的场景下，相当于完全的串行化，因此只需要一个writeLockC的锁
	// merge的场景下，由于要将多个写请求进行合并，因此有writeMergeC/writeMergedC/writeLockC/writeAckC四种锁
	// 这四种锁的使用，后面会详细说明
	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	// sync是写磁盘文件时(wal log)，是否每次都执行sync
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		// openDB时进行的初始化：writeMergeC:  make(chan writeMerge)
		// 可以看出writeMergeC是非缓冲的channel，发送会被阻塞，等待接收完成才返回
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			// 阻塞等待writeMergedC锁
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
			// openDB时初始化：writeLockC:   make(chan struct{}, 1)
			// 可见writeLockC是带缓冲的，长度为1
			// 由于是带缓冲区的，所以写完立马返回。也就是获取了writeLockC的锁
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		// 取得writeLockC的lock，可以正常执行append操作，否则阻塞
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	// 从batch pool中取一个batch
	batch := db.batchPool.Get().(*Batch)
	// 初始化
	batch.Reset()
	// 将kt,k,v拼接好，塞到batch中，并补齐index信息
	batch.appendRec(kt, key, value)
	// 这个是重点了，TODO
	return db.writeLocked(batch, batch, merge, sync)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
// leveldb对外提供的接口
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	// 与delete相比，差别在于参数keyTypeVal/keyTypeDel
	return db.putRec(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}

// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
