/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A memstore-local allocation buffer.
 * <p>
 * The MemStoreLAB is basically a bump-the-pointer allocator that allocates big (2MB) chunks from
 * and then doles it out to threads that request slices into the array. These chunks can get pooled
 * as well. See {@link ChunkCreator}.
 * <p>
 *   基于指针碰撞的分配器，每次分配2M的chunks，这些chunk可以池化
 * The purpose of this is to combat heap fragmentation in the regionserver. By ensuring that all
 * Cells in a given memstore refer only to large chunks of contiguous memory, we ensure that large
 * blocks get freed up when the memstore is flushed.
 * <p>
 * 主要为了减少 heap内存碎片化的问题。通过把memstore 的所有cell都放到这些在可复用的内存里的chunk，
 * 可以确保memstore flush时，这些block会被释放
 *
 * Without the MSLAB, the byte array allocated during insertion end up interleaved throughout the
 * heap, and the old generation gets progressively more fragmented until a stop-the-world compacting
 * collection occurs.
 * 没有MSLA，新分配的字节数组 会交错在堆中，并且老年代的碎片化现象越来越严重，直到需要stw（GC）
 * <p>
 * This manages the large sized chunks. When Cells are to be added to Memstore, MemStoreLAB's
 * {@link #copyCellInto(Cell)} gets called. This allocates enough size in the chunk to hold this
 * cell's data and copies into this area and then recreate a Cell over this copied data.
 * <p>
 * @see ChunkCreator
 */
@InterfaceAudience.Private
public interface MemStoreLAB {

  String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  boolean USEMSLAB_DEFAULT = true;
  String MSLAB_CLASS_NAME = "hbase.regionserver.mslab.class";

  String CHUNK_SIZE_KEY = "hbase.hregion.memstore.mslab.chunksize";
  int CHUNK_SIZE_DEFAULT = 2048 * 1024;
  String INDEX_CHUNK_SIZE_PERCENTAGE_KEY = "hbase.hregion.memstore.mslab.indexchunksize.percent";
  float INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT = 0.1f;
  String MAX_ALLOC_KEY = "hbase.hregion.memstore.mslab.max.allocation";
  int MAX_ALLOC_DEFAULT = 256 * 1024; // allocs bigger than this don't go through
                                      // allocator

  // MSLAB pool related configs
  String CHUNK_POOL_MAXSIZE_KEY = "hbase.hregion.memstore.chunkpool.maxsize";
  String CHUNK_POOL_INITIALSIZE_KEY = "hbase.hregion.memstore.chunkpool.initialsize";
  float POOL_MAX_SIZE_DEFAULT = 1.0f;
  float POOL_INITIAL_SIZE_DEFAULT = 0.0f;

  /**
   * Allocates slice in this LAB and copy the passed Cell into this area. Returns new Cell instance
   * over the copied the data. When this MemStoreLAB can not copy this Cell, it returns null.
   */
  Cell copyCellInto(Cell cell);

  /**
   * Allocates slice in this LAB and copy the passed Cell into this area. Returns new Cell instance
   * over the copied the data. When this MemStoreLAB can not copy this Cell, it returns null. Since
   * the process of flattening to CellChunkMap assumes all cells are allocated on MSLAB, and since
   * copyCellInto does not copy big cells (for whom size > maxAlloc) into MSLAB, this method is
   * called while the process of flattening to CellChunkMap is running, for forcing the allocation
   * of big cells on this MSLAB.
   */
  Cell forceCopyOfBigCellInto(Cell cell);

  /**
   * Close instance since it won't be used any more, try to put the chunks back to pool
   */
  void close();

  /**
   * Called when opening a scanner on the data of this MemStoreLAB
   */
  void incScannerCount();

  /**
   * Called when closing a scanner on the data of this MemStoreLAB
   */
  void decScannerCount();

  /*
   * Returning a new pool chunk, without replacing current chunk, meaning MSLABImpl does not make
   * the returned chunk as CurChunk. The space on this chunk will be allocated externally. The
   * interface is only for external callers.
   */
  Chunk getNewExternalChunk(ChunkCreator.ChunkType chunkType);

  /*
   * Returning a new chunk, without replacing current chunk, meaning MSLABImpl does not make the
   * returned chunk as CurChunk. The space on this chunk will be allocated externally. The interface
   * is only for external callers.
   */
  Chunk getNewExternalChunk(int size);

  static MemStoreLAB newInstance(Configuration conf) {
    MemStoreLAB memStoreLAB = null;
    if (isEnabled(conf)) {
      String className = conf.get(MSLAB_CLASS_NAME, MemStoreLABImpl.class.getName());
      memStoreLAB = ReflectionUtils.instantiateWithCustomCtor(className,
        new Class[] { Configuration.class }, new Object[] { conf });
    }
    return memStoreLAB;
  }

  static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(USEMSLAB_KEY, USEMSLAB_DEFAULT);
  }

  boolean isOnHeap();

  boolean isOffHeap();
}
