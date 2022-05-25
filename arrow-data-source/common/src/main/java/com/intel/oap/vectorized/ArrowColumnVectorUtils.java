package com.intel.oap.vectorized;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Utilities to help manipulate data associate with ColumnVectors. These should be used mostly
 * for debugging or other non-performance critical paths.
 * These utilities are mostly used to convert ColumnVectors into other formats.
 */
public class ArrowColumnVectorUtils {
    /**
     * Populates the entire `col` with `row[fieldIdx]`
     * This is copied from {@link org.apache.spark.sql.execution.vectorized.ColumnVectorUtils#populate}.
     * We changed the way to putByteArrays.
     */
    public static void populate(WritableColumnVector col, InternalRow row, int fieldIdx) {
        ArrowWritableColumnVector arrowCol = (ArrowWritableColumnVector) col;
        int capacity = arrowCol.getCapacity();

        if (row.isNullAt(fieldIdx)) {
            arrowCol.putNulls(0, capacity);
        } else {
            if (arrowCol.dataType() == DataTypes.StringType) {
                UTF8String v = row.getUTF8String(fieldIdx);
                byte[] bytes = v.getBytes();
                arrowCol.putByteArrays(0, capacity, bytes, 0, bytes.length);
            } else {
                ColumnVectorUtils.populate(col, row, fieldIdx);
            }
        }
    }
}
