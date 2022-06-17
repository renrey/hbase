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

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallback;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * RegionScannerImpl is used to combine scanners from multiple Stores (aka column families).
 */
@InterfaceAudience.Private
class RegionScannerImpl implements RegionScanner, Shipper, RpcCallback {

  private static final Logger LOG = LoggerFactory.getLogger(RegionScannerImpl.class);

  // Package local for testability
  KeyValueHeap storeHeap = null;

  /**
   * Heap of key-values that are not essential for the provided filters and are thus read on demand,
   * if on-demand column family loading is enabled.
   */
  KeyValueHeap joinedHeap = null;

  /**
   * If the joined heap data gathering is interrupted due to scan limits, this will contain the row
   * for which we are populating the values.
   */
  protected Cell joinedContinuationRow = null;
  private boolean filterClosed = false;

  protected final byte[] stopRow;
  protected final boolean includeStopRow;
  protected final HRegion region;
  protected final CellComparator comparator;

  private final ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;

  private final long readPt;
  private final long maxResultSize;
  private final ScannerContext defaultScannerContext;
  private final FilterWrapper filter;
  private final String operationId;

  private RegionServerServices rsServices;

  @Override
  public RegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  private static boolean hasNonce(HRegion region, long nonce) {
    RegionServerServices rsServices = region.getRegionServerServices();
    return nonce != HConstants.NO_NONCE && rsServices != null
      && rsServices.getNonceManager() != null;
  }

  RegionScannerImpl(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region,
    long nonceGroup, long nonce) throws IOException {
    this.region = region;
    this.maxResultSize = scan.getMaxResultSize();
    if (scan.hasFilter()) {
      this.filter = new FilterWrapper(scan.getFilter());
    } else {
      this.filter = null;
    }
    // meta的走meta，不是就cell
    this.comparator = region.getCellComparator();
    /**
     * By default, calls to next/nextRaw must enforce the batch limit. Thus, construct a default
     * scanner context that can be used to enforce the batch limit in the event that a
     * ScannerContext is not specified during an invocation of next/nextRaw
     */
    // scanner 上下文，初始设置一批条数
    defaultScannerContext = ScannerContext.newBuilder().setBatchLimit(scan.getBatch()).build();
    this.stopRow = scan.getStopRow();
    this.includeStopRow = scan.includeStopRow();
    this.operationId = scan.getId();

    // synchronize on scannerReadPoints so that nobody calculates
    // getSmallestReadPoint, before scannerReadPoints is updated.
    /**
     * mvcc 相关
     */
    IsolationLevel isolationLevel = scan.getIsolationLevel();
    long mvccReadPoint = PackagePrivateFieldAccessor.getMvccReadPoint(scan);
    this.scannerReadPoints = region.scannerReadPoints;
    this.rsServices = region.getRegionServerServices();
    // 上锁，证明基于同一个region 生成scanner的时候，是串行的
    synchronized (scannerReadPoints) {
      // scan已经有point了
      if (mvccReadPoint > 0) {
        this.readPt = mvccReadPoint;
      } else if (hasNonce(region, nonce)) {
        this.readPt = rsServices.getNonceManager().getMvccFromOperationContext(nonceGroup, nonce);
      } else {
        // get 的时候，没有nonce
        // 如果读提交，使用mvcc当前的readPoint
        // 读未提交, MAX等于不使用
        this.readPt = region.getReadPoint(isolationLevel);
      }
      // 保存当前scanner 与readPoint到 当前Region的map中
      scannerReadPoints.put(this, this.readPt);
    }
    /**
     * 初始化下级scanner(StoreScanner), 每个family1个scanner
     */
    initializeScanners(scan, additionalScanners);
  }

  private void initializeScanners(Scan scan, List<KeyValueScanner> additionalScanners)
    throws IOException {
    /**
     * 这里的scanner数 就是 条件需要获取的family数量--- 每个目标family1个scanner
     */
    // Here we separate all scanners into two lists - scanner that provide data required
    // by the filter to operate (scanners list) and all others (joinedScanners list).
    List<KeyValueScanner> scanners = new ArrayList<>(scan.getFamilyMap().size());
    List<KeyValueScanner> joinedScanners = new ArrayList<>(scan.getFamilyMap().size());
    // Store all already instantiated scanners for exception handling
    List<KeyValueScanner> instantiatedScanners = new ArrayList<>();
    // handle additionalScanners
    if (additionalScanners != null && !additionalScanners.isEmpty()) {
      scanners.addAll(additionalScanners);
      instantiatedScanners.addAll(additionalScanners);
    }

    try {
      // 遍历当前的所有family
      for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
        // 1。 get当前family的store
        HStore store = region.getStore(entry.getKey());
        // 创建StoreScanner（第2级，new），StoreScanner
        KeyValueScanner scanner = store.getScanner(scan, entry.getValue(), this.readPt);
        instantiatedScanners.add(scanner);
        if (
          this.filter == null || !scan.doLoadColumnFamiliesOnDemand()
            || this.filter.isFamilyEssential(entry.getKey())
        ) {
          scanners.add(scanner);
        } else {
          joinedScanners.add(scanner);
        }
      }
      /**
       *  初始化StoreHeap--- 通过上面生成的scanner
       *  这个对象是获取数据的入口！！！
       */
      initializeKVHeap(scanners, joinedScanners, region);
    } catch (Throwable t) {
      throw handleException(instantiatedScanners, t);
    }
  }

  protected void initializeKVHeap(List<KeyValueScanner> scanners,
    List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
    this.storeHeap = new KeyValueHeap(scanners, comparator);
    if (!joinedScanners.isEmpty()) {
      this.joinedHeap = new KeyValueHeap(joinedScanners, comparator);
    }
  }

  private IOException handleException(List<KeyValueScanner> instantiatedScanners, Throwable t) {
    // remove scaner read point before throw the exception
    scannerReadPoints.remove(this);
    if (storeHeap != null) {
      storeHeap.close();
      storeHeap = null;
      if (joinedHeap != null) {
        joinedHeap.close();
        joinedHeap = null;
      }
    } else {
      // close all already instantiated scanners before throwing the exception
      for (KeyValueScanner scanner : instantiatedScanners) {
        scanner.close();
      }
    }
    return t instanceof IOException ? (IOException) t : new IOException(t);
  }

  @Override
  public long getMaxResultSize() {
    return maxResultSize;
  }

  @Override
  public long getMvccReadPoint() {
    return this.readPt;
  }

  @Override
  public int getBatch() {
    return this.defaultScannerContext.getBatchLimit();
  }

  @Override
  public String getOperationId() {
    return operationId;
  }

  /**
   * Reset both the filter and the old filter.
   * @throws IOException in case a filter raises an I/O exception.
   */
  protected final void resetFilters() throws IOException {
    if (filter != null) {
      filter.reset();
    }
  }

  @Override
  public boolean next(List<Cell> outResults) throws IOException {
    // apply the batching limit by default
    // 尽量获取一行的数据（返回条件的），但里面的cell是同一行的
    return next(outResults, defaultScannerContext);
  }

  @Override
  public synchronized boolean next(List<Cell> outResults, ScannerContext scannerContext)
    throws IOException {
    if (this.filterClosed) {
      throw new UnknownScannerException("Scanner was closed (timed out?) "
        + "after we renewed it. Could be caused by a very slow scanner "
        + "or a lengthy garbage collection");
    }
    region.startRegionOperation(Operation.SCAN);
    try {
      // 尽量获取一行的数据（符合条件的），但里面的cell是同一行的
      return nextRaw(outResults, scannerContext);
    } finally {
      region.closeRegionOperation(Operation.SCAN);
    }
  }

  @Override
  public boolean nextRaw(List<Cell> outResults) throws IOException {
    // Use the RegionScanner's context by default
    return nextRaw(outResults, defaultScannerContext);
  }

  @Override
  public boolean nextRaw(List<Cell> outResults, ScannerContext scannerContext) throws IOException {
    if (storeHeap == null) {
      // scanner is closed
      throw new UnknownScannerException("Scanner was closed");
    }
    boolean moreValues = false;
    // 第1次肯定没有（传入空result）
    if (outResults.isEmpty()) {
      // Usually outResults is empty. This is true when next is called
      // to handle scan or get operation.
      // 尽量获取一行row的数据（不一定一行完整的，且是符合查询条件）
      moreValues = nextInternal(outResults, scannerContext);
    } else {
      List<Cell> tmpList = new ArrayList<>();
      moreValues = nextInternal(tmpList, scannerContext);
      outResults.addAll(tmpList);
    }

    region.addReadRequestsCount(1);
    if (region.getMetrics() != null) {
      region.getMetrics().updateReadRequestCount();
    }

    // If the size limit was reached it means a partial Result is being returned. Returning a
    // partial Result means that we should not reset the filters; filters should only be reset in
    // between rows
    if (!scannerContext.mayHaveMoreCellsInRow()) {
      resetFilters();
    }

    if (isFilterDoneInternal()) {
      moreValues = false;
    }
    // 代表查询，还能继续有数据返回
    return moreValues;
  }

  /**
   * @return true if more cells exist after this batch, false if scanner is done
   */
  private boolean populateFromJoinedHeap(List<Cell> results, ScannerContext scannerContext)
    throws IOException {
    assert joinedContinuationRow != null;
    boolean moreValues =
      populateResult(results, this.joinedHeap, scannerContext, joinedContinuationRow);

    if (!scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
      // We are done with this row, reset the continuation.
      joinedContinuationRow = null;
    }
    // As the data is obtained from two independent heaps, we need to
    // ensure that result list is sorted, because Result relies on that.
    results.sort(comparator);
    return moreValues;
  }

  /**
   * Fetches records with currentRow into results list, until next row, batchLimit (if not -1) is
   * reached, or remainingResultSize (if not -1) is reaced
   * @param heap KeyValueHeap to fetch data from.It must be positioned on correct row before call.
   * @return state of last call to {@link KeyValueHeap#next()}
   */
  private boolean populateResult(List<Cell> results, KeyValueHeap heap,
    ScannerContext scannerContext, Cell currentRowCell) throws IOException {
    Cell nextKv;
    boolean moreCellsInRow = false;
    boolean tmpKeepProgress = scannerContext.getKeepProgress();
    // Scanning between column families and thus the scope is between cells
    LimitScope limitScope = LimitScope.BETWEEN_CELLS;
    /**
     * 这里循环是只要当前cell所属的row还有其他cell，就会循环获取，直到没有其他cell或者达到条件上限
     */
    do {
      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation.
      region.checkInterrupt();

      // We want to maintain any progress that is made towards the limits while scanning across
      // different column families. To do this, we toggle the keep progress flag on during calls
      // to the StoreScanner to ensure that any progress made thus far is not wiped away.
      scannerContext.setKeepProgress(true);
      // 当前scanner切到下个cell
      // 并把current写入result
      heap.next(results, scannerContext);
      scannerContext.setKeepProgress(tmpKeepProgress);

      nextKv = heap.peek();
      /**
       * 判断下一个跟当前拿到是否同一行
       */
      moreCellsInRow = moreCellsInRow(nextKv, currentRowCell);
      if (!moreCellsInRow) {
        incrementCountOfRowsScannedMetric(scannerContext);
      }
      if (moreCellsInRow && scannerContext.checkBatchLimit(limitScope)) {
        return scannerContext.setScannerState(NextState.BATCH_LIMIT_REACHED).hasMoreValues();
      } else if (scannerContext.checkSizeLimit(limitScope)) {
        ScannerContext.NextState state =
          moreCellsInRow ? NextState.SIZE_LIMIT_REACHED_MID_ROW : NextState.SIZE_LIMIT_REACHED;
        return scannerContext.setScannerState(state).hasMoreValues();
      } else if (scannerContext.checkTimeLimit(limitScope)) {
        ScannerContext.NextState state =
          moreCellsInRow ? NextState.TIME_LIMIT_REACHED_MID_ROW : NextState.TIME_LIMIT_REACHED;
        return scannerContext.setScannerState(state).hasMoreValues();
      }
    } while (moreCellsInRow);
    return nextKv != null;
  }

  /**
   * Based on the nextKv in the heap, and the current row, decide whether or not there are more
   * cells to be read in the heap. If the row of the nextKv in the heap matches the current row then
   * there are more cells to be read in the row.
   * @return true When there are more cells in the row to be read
   */
  private boolean moreCellsInRow(final Cell nextKv, Cell currentRowCell) {
    return nextKv != null && CellUtil.matchingRows(nextKv, currentRowCell);
  }

  /**
   * @return True if a filter rules the scanner is over, done.
   */
  @Override
  public synchronized boolean isFilterDone() throws IOException {
    return isFilterDoneInternal();
  }

  private boolean isFilterDoneInternal() throws IOException {
    return this.filter != null && this.filter.filterAllRemaining();
  }

  private void checkClientDisconnect(Optional<RpcCall> rpcCall) throws CallerDisconnectedException {
    if (rpcCall.isPresent()) {
      // If a user specifies a too-restrictive or too-slow scanner, the
      // client might time out and disconnect while the server side
      // is still processing the request. We should abort aggressively
      // in that case.
      long afterTime = rpcCall.get().disconnectSince();
      if (afterTime >= 0) {
        throw new CallerDisconnectedException(
          "Aborting on region " + getRegionInfo().getRegionNameAsString() + ", call " + this
            + " after " + afterTime + " ms, since " + "caller disconnected");
      }
    }
  }

  private void resetProgress(ScannerContext scannerContext, int initialBatchProgress,
    long initialSizeProgress, long initialHeapSizeProgress) {
    // Starting to scan a new row. Reset the scanner progress according to whether or not
    // progress should be kept.
    if (scannerContext.getKeepProgress()) {
      // Progress should be kept. Reset to initial values seen at start of method invocation.
      scannerContext.setProgress(initialBatchProgress, initialSizeProgress,
        initialHeapSizeProgress);
    } else {
      scannerContext.clearProgress();
    }
  }

  // 尽量获取一行的cell数据，并且尽量是有数据返回的
  private boolean nextInternal(List<Cell> results, ScannerContext scannerContext)
    throws IOException {
    Preconditions.checkArgument(results.isEmpty(), "First parameter should be an empty list");
    Preconditions.checkArgument(scannerContext != null, "Scanner context cannot be null");
    Optional<RpcCall> rpcCall = RpcServer.getCurrentCall();

    // Save the initial progress from the Scanner context in these local variables. The progress
    // may need to be reset a few times if rows are being filtered out so we save the initial
    // progress.
    int initialBatchProgress = scannerContext.getBatchProgress();
    long initialSizeProgress = scannerContext.getDataSizeProgress();
    long initialHeapSizeProgress = scannerContext.getHeapSizeProgress();

    // Used to check time limit
    LimitScope limitScope = LimitScope.BETWEEN_CELLS;

    // The loop here is used only when at some point during the next we determine
    // that due to effects of filters or otherwise, we have an empty row in the result.
    // Then we loop and try again. Otherwise, we must get out on the first iteration via return,
    // "true" if there's more data to read, "false" if there isn't (storeHeap is at a stop row,
    // and joinedHeap has no more data to read for the last row (if set, joinedContinuationRow).
    // 这里循环的是，如果本次获取拿到row是没有数据（可能由于filter、查询条件），会进入下次循环继续获取
    // 还有数据未读完返回true，没有可以读（到了stop、没有更多数据）返回false
    while (true) {
      resetProgress(scannerContext, initialBatchProgress, initialSizeProgress,
        initialHeapSizeProgress);
      checkClientDisconnect(rpcCall);

      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation.
      region.checkInterrupt();

      // Let's see what we have in the storeHeap.
      /**
       * 从storeHeap拿当前最先的cell
       * ---其实就是最先的scanner里面拿
       */
      Cell current = this.storeHeap.peek();

      // 判断是否需要停止scan
      boolean shouldStop = shouldStop(current);
      // When has filter row is true it means that the all the cells for a particular row must be
      // read before a filtering decision can be made. This means that filters where hasFilterRow
      // run the risk of enLongAddering out of memory errors in the case that they are applied to a
      // table that has very large rows.
      boolean hasFilterRow = this.filter != null && this.filter.hasFilterRow();

      // If filter#hasFilterRow is true, partial results are not allowed since allowing them
      // would prevent the filters from being evaluated. Thus, if it is true, change the
      // scope of any limits that could potentially create partial results to
      // LimitScope.BETWEEN_ROWS so that those limits are not reached mid-row
      if (hasFilterRow) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("filter#hasFilterRow is true which prevents partial results from being "
            + " formed. Changing scope of limits that may create partials");
        }
        scannerContext.setSizeLimitScope(LimitScope.BETWEEN_ROWS);
        scannerContext.setTimeLimitScope(LimitScope.BETWEEN_ROWS);
        limitScope = LimitScope.BETWEEN_ROWS;
      }

      if (scannerContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)) {
        if (hasFilterRow) {
          throw new IncompatibleFilterException(
            "Filter whose hasFilterRow() returns true is incompatible with scans that must "
              + " stop mid-row because of a limit. ScannerContext:" + scannerContext);
        }
        return true;
      }

      // Check if we were getting data from the joinedHeap and hit the limit.
      // If not, then it's main path - getting results from storeHeap.
      if (joinedContinuationRow == null) {
        // First, check if we are at a stop row. If so, there are no more results.
        if (shouldStop) {
          if (hasFilterRow) {
            filter.filterRowCells(results);
          }
          // 到达stop row，返回false停止
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
        }

        // Check if rowkey filter wants to exclude this row. If so, loop to next.
        // Technically, if we hit limits before on this row, we don't need this call.
        /**
         * 当前rowkey被filter排除, 代表当前row是被排除的
         */
        if (filterRowKey(current)) {
          incrementCountOfRowsFilteredMetric(scannerContext);
          // early check, see HBASE-16296
          if (isFilterDoneInternal()) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }
          // Typically the count of rows scanned is incremented inside #populateResult. However,
          // here we are filtering a row based purely on its row key, preventing us from calling
          // #populateResult. Thus, perform the necessary increment here to rows scanned metric
          incrementCountOfRowsScannedMetric(scannerContext);
          /**
           * 切换下个row（遍历当前row的cell）
           */
          boolean moreRows = nextRow(scannerContext, current);
          if (!moreRows) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }
          results.clear();

          // Read nothing as the rowkey was filtered, but still need to check time limit
          if (scannerContext.checkTimeLimit(limitScope)) {
            return true;
          }
          // 由于filter过滤当前row被过滤，进行下一次获取
          continue;
        }

        // Ok, we are good, let's try to get some results from the main heap.
        /**
         * 填充当前row的cell！！！
         * 循环获取当前cell对应row的其他cell（尽量拿到完整一行数据），直到当前row没有其他cell或者到达限制条件
         * 并写入result中
         */
        populateResult(results, this.storeHeap, scannerContext, current);
        // 达到限制
        if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
          if (hasFilterRow) {
            throw new IncompatibleFilterException(
              "Filter whose hasFilterRow() returns true is incompatible with scans that must "
                + " stop mid-row because of a limit. ScannerContext:" + scannerContext);
          }
          return true;
        }

        // Check for thread interrupt status in case we have been signaled from
        // #interruptRegionOperation.
        region.checkInterrupt();

        // 获取下一个cell
        Cell nextKv = this.storeHeap.peek();
        shouldStop = shouldStop(nextKv);
        // save that the row was empty before filters applied to it.
        final boolean isEmptyRow = results.isEmpty(); // 上面的填充cell，没有数据(例如当前row没有指定的cell)

        // We have the part of the row necessary for filtering (all of it, usually).
        // First filter with the filterRow(List).
        /**
         * 对当前row进行filter过滤
         */
        FilterWrapper.FilterRowRetCode ret = FilterWrapper.FilterRowRetCode.NOT_CALLED;
        if (hasFilterRow) {
          // 对当前row已拿到的cell进行filter过滤
          ret = filter.filterRowCellsWithRet(results);

          // We don't know how the results have changed after being filtered. Must set progress
          // according to contents of results now.
          if (scannerContext.getKeepProgress()) {
            scannerContext.setProgress(initialBatchProgress, initialSizeProgress,
              initialHeapSizeProgress);
          } else {
            scannerContext.clearProgress();
          }
          scannerContext.incrementBatchProgress(results.size());
          for (Cell cell : results) {
            scannerContext.incrementSizeProgress(PrivateCellUtil.estimatedSerializedSizeOf(cell),
              cell.heapSize());
          }
        }

        /**
         * 当前根据查询条件，不会有结果的情况处理
         * 1。当前row 本次查询不会返回数据（例如没有需要cell）
         * 2。当前row符合filter过滤
         */
        if (isEmptyRow || ret == FilterWrapper.FilterRowRetCode.EXCLUDE || filterRow()) {
          incrementCountOfRowsFilteredMetric(scannerContext);
          results.clear();// 清理（被filter过滤的情况）
          // 切到下一行
          boolean moreRows = nextRow(scannerContext, current);
          if (!moreRows) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }

          // This row was totally filtered out, if this is NOT the last row,
          // we should continue on. Otherwise, nothing else to do.
          if (!shouldStop) {
            // Read nothing as the cells was filtered, but still need to check time limit
            if (scannerContext.checkTimeLimit(limitScope)) {
              return true;
            }
            // 什么限制都没达到下，进行下一行的获取
            continue;
          }
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
        }

        // Ok, we are done with storeHeap for this row.
        // Now we may need to fetch additional, non-essential data into row.
        // These values are not needed for filter to work, so we postpone their
        // fetch to (possibly) reduce amount of data loads from disk.
        if (this.joinedHeap != null) {
          boolean mayHaveData = joinedHeapMayHaveData(current);
          if (mayHaveData) {
            joinedContinuationRow = current;
            populateFromJoinedHeap(results, scannerContext);

            if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
              return true;
            }
          }
        }
      } else {
        // Populating from the joined heap was stopped by limits, populate some more.
        populateFromJoinedHeap(results, scannerContext);
        if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
          return true;
        }
      }
      // We may have just called populateFromJoinedMap and hit the limits. If that is
      // the case, we need to call it again on the next next() invocation.
      if (joinedContinuationRow != null) {
        return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
      }

      // Finally, we are done with both joinedHeap and storeHeap.
      // Double check to prevent empty rows from appearing in result. It could be
      // the case when SingleColumnValueExcludeFilter is used.
      /**
       * 万一当前的row还是没数据返回，切下一行继续获取
       */
      if (results.isEmpty()) {
        incrementCountOfRowsFilteredMetric(scannerContext);
        boolean moreRows = nextRow(scannerContext, current);
        if (!moreRows) {
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
        }
        if (!shouldStop) {
          continue;
        }
      }

      // 最终返回
      if (shouldStop) {
        return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
      } else {
        return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
      }
    }
  }

  private void incrementCountOfRowsFilteredMetric(ScannerContext scannerContext) {
    region.filteredReadRequestsCount.increment();
    if (region.getMetrics() != null) {
      region.getMetrics().updateFilteredRecords();
    }

    if (scannerContext == null || !scannerContext.isTrackingMetrics()) {
      return;
    }

    scannerContext.getMetrics().countOfRowsFiltered.incrementAndGet();
  }

  private void incrementCountOfRowsScannedMetric(ScannerContext scannerContext) {
    if (scannerContext == null || !scannerContext.isTrackingMetrics()) {
      return;
    }

    scannerContext.getMetrics().countOfRowsScanned.incrementAndGet();
  }

  /**
   * @return true when the joined heap may have data for the current row
   */
  private boolean joinedHeapMayHaveData(Cell currentRowCell) throws IOException {
    Cell nextJoinedKv = joinedHeap.peek();
    boolean matchCurrentRow =
      nextJoinedKv != null && CellUtil.matchingRows(nextJoinedKv, currentRowCell);
    boolean matchAfterSeek = false;

    // If the next value in the joined heap does not match the current row, try to seek to the
    // correct row
    if (!matchCurrentRow) {
      Cell firstOnCurrentRow = PrivateCellUtil.createFirstOnRow(currentRowCell);
      boolean seekSuccessful = this.joinedHeap.requestSeek(firstOnCurrentRow, true, true);
      matchAfterSeek = seekSuccessful && joinedHeap.peek() != null
        && CellUtil.matchingRows(joinedHeap.peek(), currentRowCell);
    }

    return matchCurrentRow || matchAfterSeek;
  }

  /**
   * This function is to maintain backward compatibility for 0.94 filters. HBASE-6429 combines both
   * filterRow & filterRow({@code List<KeyValue> kvs}) functions. While 0.94 code or older, it may
   * not implement hasFilterRow as HBase-6429 expects because 0.94 hasFilterRow() only returns true
   * when filterRow({@code List<KeyValue> kvs}) is overridden not the filterRow(). Therefore, the
   * filterRow() will be skipped.
   */
  private boolean filterRow() throws IOException {
    // when hasFilterRow returns true, filter.filterRow() will be called automatically inside
    // filterRowCells(List<Cell> kvs) so we skip that scenario here.
    return filter != null && (!filter.hasFilterRow()) && filter.filterRow();
  }

  private boolean filterRowKey(Cell current) throws IOException {
    return filter != null && filter.filterRowKey(current);
  }

  /**
   * A mocked list implementation - discards all updates.
   */
  private static final List<Cell> MOCKED_LIST = new AbstractList<Cell>() {

    @Override
    public void add(int index, Cell element) {
      // do nothing
    }

    @Override
    public boolean addAll(int index, Collection<? extends Cell> c) {
      return false; // this list is never changed as a result of an update
    }

    @Override
    public KeyValue get(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return 0;
    }
  };

  protected boolean nextRow(ScannerContext scannerContext, Cell curRowCell) throws IOException {
    assert this.joinedContinuationRow == null : "Trying to go to next row during joinedHeap read.";
    Cell next;
    /**
     * scanner 把当前row的cell遍历完
     */
    while ((next = this.storeHeap.peek()) != null && CellUtil.matchingRows(next, curRowCell)) {
      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation.
      region.checkInterrupt();
      this.storeHeap.next(MOCKED_LIST);
    }
    resetFilters();

    // Calling the hook in CP which allows it to do a fast forward
    return this.region.getCoprocessorHost() == null
      || this.region.getCoprocessorHost().postScannerFilterRow(this, curRowCell);
  }

  protected boolean shouldStop(Cell currentRowCell) {
    if (currentRowCell == null) {
      return true;
    }
    if (stopRow == null || Bytes.equals(stopRow, HConstants.EMPTY_END_ROW)) {
      return false;
    }
    int c = comparator.compareRows(currentRowCell, stopRow, 0, stopRow.length);
    return c > 0 || (c == 0 && !includeStopRow);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "this method is only called inside close which is synchronized")
  private void closeInternal() {
    if (storeHeap != null) {
      storeHeap.close();
      storeHeap = null;
    }
    if (joinedHeap != null) {
      joinedHeap.close();
      joinedHeap = null;
    }
    // no need to synchronize here.
    scannerReadPoints.remove(this);
    this.filterClosed = true;
  }

  @Override
  public synchronized void close() {
    TraceUtil.trace(this::closeInternal, () -> region.createRegionSpan("RegionScanner.close"));
  }

  @Override
  public synchronized boolean reseek(byte[] row) throws IOException {
    return TraceUtil.trace(() -> {
      if (row == null) {
        throw new IllegalArgumentException("Row cannot be null.");
      }
      boolean result = false;
      region.startRegionOperation();
      Cell kv = PrivateCellUtil.createFirstOnRow(row, 0, (short) row.length);
      try {
        // use request seek to make use of the lazy seek option. See HBASE-5520
        result = this.storeHeap.requestSeek(kv, true, true);
        if (this.joinedHeap != null) {
          result = this.joinedHeap.requestSeek(kv, true, true) || result;
        }
      } finally {
        region.closeRegionOperation();
      }
      return result;
    }, () -> region.createRegionSpan("RegionScanner.reseek"));
  }

  @Override
  public void shipped() throws IOException {
    if (storeHeap != null) {
      storeHeap.shipped();
    }
    if (joinedHeap != null) {
      joinedHeap.shipped();
    }
  }

  @Override
  public void run() throws IOException {
    // This is the RPC callback method executed. We do the close in of the scanner in this
    // callback
    this.close();
  }
}
