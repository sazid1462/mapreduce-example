package com.sazid.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class CompanyInfoArrayWritable extends ArrayWritable {
    public CompanyInfoArrayWritable() {
        super(CompanyInfoWritable.class);
    }
    public CompanyInfoArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public CompanyInfoArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    public CompanyInfoArrayWritable(String[] strings) {
        super(strings);
    }
}
