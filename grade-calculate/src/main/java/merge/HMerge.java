//package merge;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HConstants;
//import org.apache.hadoop.hbase.HRegionInfo;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.RemoteExceptionHandler;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HConnection;
//import org.apache.hadoop.hbase.client.HConnectionManager;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.regionserver.HRegion;
//import org.apache.hadoop.hbase.regionserver.InternalScanner;
//import org.apache.hadoop.hbase.regionserver.wal.HLog;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.Writables;
//
///**
//* A non-instantiable class that has a static method capable of compacting
//* a table by merging adjacent regions.
//*/
//public class HMerge {
//    static final Log    LOG  = LogFactory.getLog(HMerge.class);
//    static final Random rand = new Random();
//
//    /*
//     * Not instantiable
//     */
//    public HMerge() {
//        super();
//    }
//
//    /**
//     * Scans the table and merges two adjacent regions if they are small. This
//     * only happens when a lot of rows are deleted.
//     *
//     * When merging the META region, the HBase instance must be offline.
//     * When merging a normal table, the HBase instance must be online, but the
//     * table must be disabled.
//     *
//     * @param conf        - configuration object for HBase
//     * @param fs          - FileSystem where regions reside
//     * @param tableName   - Table to be compacted
//     * @throws IOException
//     */
//    public static void merge(Configuration conf, FileSystem fs, final byte[] tableName)
//                                                                                       throws IOException {
//        merge(conf, fs, tableName, true);
//    }
//
//    /**
//     * Scans the table and merges two adjacent regions if they are small. This
//     * only happens when a lot of rows are deleted.
//     *
//     * When merging the META region, the HBase instance must be offline.
//     * When merging a normal table, the HBase instance must be online, but the
//     * table must be disabled.
//     *
//     * @param conf        - configuration object for HBase
//     * @param fs          - FileSystem where regions reside
//     * @param tableName   - Table to be compacted
//     * @param testMasterRunning True if we are to verify master is down before
//     * running merge
//     * @throws IOException
//     */
//    public static void merge(Configuration conf, FileSystem fs, final byte[] tableName,
//                             final boolean testMasterRunning) throws IOException {
//        boolean masterIsRunning = false;
//        if (testMasterRunning) {
//            HConnection connection = HConnectionManager.getConnection(conf);
//            masterIsRunning = connection.isMasterRunning();
//        }
//        HConnectionManager.deleteConnection(conf, true);
//        if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
//            if (masterIsRunning) {
//                throw new IllegalStateException("Can not compact META table if instance is on-line");
//            }
//            new OfflineMerger(conf, fs).process();
//        } else {
//            if (!masterIsRunning) {
//                throw new IllegalStateException(
//                    "HBase instance must be running to merge a normal table");
//            }
//            //      HBaseAdmin admin = new HBaseAdmin(conf);
//            //      if (!admin.isTableDisabled(tableName)) {
//            //        throw new TableNotDisabledException(tableName);
//            //      }
//            new OnlineMerger(conf, fs, tableName).process();
//        }
//    }
//
//    private static abstract class Merger {
//        protected final Configuration conf;
//        protected final FileSystem    fs;
//        protected final Path          tabledir;
//        protected final HLog          hlog;
//        private final long            maxFilesize;
//
//        protected Merger(Configuration conf, FileSystem fs, final byte[] tableName)
//                                                                                   throws IOException {
//            this.conf = conf;
//            this.fs = fs;
//            this.maxFilesize = conf.getLong("hbase.hregion.max.filesize",
//                HConstants.DEFAULT_MAX_FILE_SIZE);
//
//            this.tabledir = new Path(fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR))),
//                Bytes.toString(tableName));
//            Path logdir = new Path(tabledir, "merge_" + System.currentTimeMillis()
//                                             + HConstants.HREGION_LOGDIR_NAME);
//            Path oldLogDir = new Path(tabledir, HConstants.HREGION_OLDLOGDIR_NAME);
//            this.hlog = new HLog(fs, logdir, oldLogDir, conf);
//        }
//
//        void process() throws IOException {
//            try {
//                for (HRegionInfo[] regionsToMerge = next(); regionsToMerge != null; regionsToMerge = next()) {
//                    if (!merge(regionsToMerge)) {
//                        return;
//                    }
//                }
//            } finally {
//                try {
//                    hlog.closeAndDelete();
//
//                } catch (IOException e) {
//                    LOG.error(e);
//                }
//            }
//        }
//
//        protected boolean merge(final HRegionInfo[] info) throws IOException {
//            if (info.length < 2) {
//                LOG.info("only one region - nothing to merge");
//                return false;
//            }
//
//            HRegion currentRegion = null;
//            long currentSize = 0;
//            HRegion nextRegion = null;
//            long nextSize = 0;
//            for (int i = 0; i < info.length - 1; i++) {
//                if (currentRegion == null) {
//                    currentRegion = HRegion.newHRegion(tabledir, hlog, fs, conf, info[i], null);
//                    currentRegion.initialize();
//                    currentSize = currentRegion.getLargestHStoreSize();
//                }
//                nextRegion = HRegion.newHRegion(tabledir, hlog, fs, conf, info[i + 1], null);
//                nextRegion.initialize();
//                nextSize = nextRegion.getLargestHStoreSize();
//
//                if ((currentSize + nextSize) <= (maxFilesize / 2)) {
//                    // We merge two adjacent regions if their total size is less than
//                    // one half of the desired maximum size
//                    LOG.info("Merging regions " + currentRegion.getRegionNameAsString() + " and "
//                             + nextRegion.getRegionNameAsString());
//                    HRegion mergedRegion = HRegion.mergeAdjacent(currentRegion, nextRegion);
//                    updateMeta(currentRegion.getRegionName(), nextRegion.getRegionName(),
//                        mergedRegion);
//                    break;
//                }
//                LOG.info("not merging regions " + Bytes.toString(currentRegion.getRegionName())
//                         + " and " + Bytes.toString(nextRegion.getRegionName()));
//                currentRegion.close();
//                currentRegion = nextRegion;
//                currentSize = nextSize;
//            }
//            if (currentRegion != null) {
//                currentRegion.close();
//            }
//            return true;
//        }
//
//        protected abstract HRegionInfo[] next() throws IOException;
//
//        protected abstract void updateMeta(final byte[] oldRegion1, final byte[] oldRegion2,
//                                           HRegion newRegion) throws IOException;
//
//    }
//
//    /** Instantiated to compact a normal user table */
//    private static class OnlineMerger extends Merger {
//        private final byte[]        tableName;
//        private final HTable        table;
//        private final ResultScanner metaScanner;
//        private HRegionInfo         latestRegion;
//
//        OnlineMerger(Configuration conf, FileSystem fs, final byte[] tableName) throws IOException {
//            super(conf, fs, tableName);
//            this.tableName = tableName;
//            this.table = new HTable(conf, HConstants.META_TABLE_NAME);
//            this.metaScanner = table.getScanner(HConstants.CATALOG_FAMILY,
//                HConstants.REGIONINFO_QUALIFIER);
//            this.latestRegion = null;
//        }
//
//        private HRegionInfo nextRegion() throws IOException {
//            try {
//                HRegionInfo results = getMetaRow();
//
//                return results;
//            } catch (IOException e) {
//                e = RemoteExceptionHandler.checkIOException(e);
//                LOG.error("meta scanner error", e);
//                metaScanner.close();
//                throw e;
//            }
//        }
//
//        /*
//         * Check current row has a HRegionInfo.  Skip to next row if HRI is empty.
//         * @return A Map of the row content else null if we are off the end.
//         * @throws IOException
//         */
//        private HRegionInfo getMetaRow() throws IOException {
//
//            Result currentRow = metaScanner.next();
//            boolean found = false;
//            HRegionInfo region = null;
//            while (currentRow != null) {
//                LOG.info("Row: <" + Bytes.toString(currentRow.getRow()) + ">");
//                byte[] regionInfoValue = currentRow.getValue(HConstants.CATALOG_FAMILY,
//                    HConstants.REGIONINFO_QUALIFIER);
//
//                if (regionInfoValue == null || regionInfoValue.length == 0) {
//
//                    currentRow = metaScanner.next();
//                    continue;
//                } else {
//                    region = Writables.getHRegionInfo(regionInfoValue);
//                    if (!Bytes.equals(region.getTableDesc().getName(), this.tableName)) {
//                        currentRow = metaScanner.next();
//                        continue;
//                    }
//                }
//
//                found = true;
//                break;
//            }
//            return found ? region : null;
//        }
//
//        @Override
//        protected HRegionInfo[] next() throws IOException {
//            List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
//            if (latestRegion == null) {
//                latestRegion = nextRegion();
//            }
//            if (latestRegion != null) {
//                regions.add(latestRegion);
//            }
//            latestRegion = nextRegion();
//            if (latestRegion != null) {
//                regions.add(latestRegion);
//            }
//            return regions.toArray(new HRegionInfo[regions.size()]);
//        }
//
//        @Override
//        protected void updateMeta(final byte[] oldRegion1, final byte[] oldRegion2,
//                                  HRegion newRegion) throws IOException {
//            byte[][] regionsToDelete = { oldRegion1, oldRegion2 };
//            for (int r = 0; r < regionsToDelete.length; r++) {
//                if (Bytes.equals(regionsToDelete[r], latestRegion.getRegionName())) {
//                    latestRegion = null;
//                }
//                Delete delete = new Delete(regionsToDelete[r]);
//                table.delete(delete);
//
//                LOG.info("updated columns in row: " + Bytes.toString(regionsToDelete[r]));
//
//            }
//
//            Put put = new Put(newRegion.getRegionName());
//            put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables
//                .getBytes(newRegion.getRegionInfo()));
//            table.put(put);
//
//            HBaseAdmin admin = new HBaseAdmin(conf);
//            admin.getMaster().assign(newRegion.getRegionName(), true);
//
//            LOG.info("updated columns in row: " + Bytes.toString(newRegion.getRegionName()));
//
//        }
//    }
//
//    /** Instantiated to compact the meta region */
//    private static class OfflineMerger extends Merger {
//        private final List<HRegionInfo> metaRegions = new ArrayList<HRegionInfo>();
//        private final HRegion           root;
//
//        OfflineMerger(Configuration conf, FileSystem fs) throws IOException {
//            super(conf, fs, HConstants.META_TABLE_NAME);
//
//            Path rootTableDir = HTableDescriptor.getTableDir(fs.makeQualified(new Path(conf
//                .get(HConstants.HBASE_DIR))), HConstants.ROOT_TABLE_NAME);
//
//            // Scan root region to find all the meta regions
//
//            root = HRegion.newHRegion(rootTableDir, hlog, fs, conf, HRegionInfo.ROOT_REGIONINFO,
//                null);
//            root.initialize();
//
//            Scan scan = new Scan();
//            scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
//            InternalScanner rootScanner = root.getScanner(scan);
//
//            try {
//                List<KeyValue> results = new ArrayList<KeyValue>();
//                while (rootScanner.next(results)) {
//                    for (KeyValue kv : results) {
//                        HRegionInfo info = Writables.getHRegionInfoOrNull(kv.getValue());
//                        if (info != null) {
//                            metaRegions.add(info);
//                        }
//                    }
//                }
//            } finally {
//                rootScanner.close();
//                try {
//                    root.close();
//
//                } catch (IOException e) {
//                    LOG.error(e);
//                }
//            }
//        }
//
//        @Override
//        protected HRegionInfo[] next() {
//            HRegionInfo[] results = null;
//            if (metaRegions.size() > 0) {
//                results = metaRegions.toArray(new HRegionInfo[metaRegions.size()]);
//                metaRegions.clear();
//            }
//            return results;
//        }
//
//        @Override
//        protected void updateMeta(final byte[] oldRegion1, final byte[] oldRegion2,
//                                  HRegion newRegion) throws IOException {
//            byte[][] regionsToDelete = { oldRegion1, oldRegion2 };
//            for (int r = 0; r < regionsToDelete.length; r++) {
//                Delete delete = new Delete(regionsToDelete[r]);
//                delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
//                delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
//                delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
//                delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER);
//                delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER);
//                root.delete(delete, null, true);
//
//                LOG.info("updated columns in row: " + Bytes.toString(regionsToDelete[r]));
//
//            }
//            HRegionInfo newInfo = newRegion.getRegionInfo();
//            newInfo.setOffline(true);
//            Put put = new Put(newRegion.getRegionName());
//            put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables
//                .getBytes(newInfo));
//            root.put(put);
//
//            LOG.info("updated columns in row: " + Bytes.toString(newRegion.getRegionName()));
//
//        }
//    }
//}