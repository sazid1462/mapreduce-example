package com.sazid.utils;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.WritableComparable;

public class CompanyInfoWritable extends SortedMapWritable implements WritableComparable<CompanyInfoWritable> {

    @Override
    public int compareTo(@NotNull CompanyInfoWritable o) {
        // Implement your compare logic
        return 0;
    }
}