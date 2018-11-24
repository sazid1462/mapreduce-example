package com.sazid.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

public class CompanyInfoArrayWritable extends ArrayWritable {
    // This default constructor is necessary to be defined. Or there will be errors.
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

    public JSONArray toJsonArray() throws JSONException {
        // Convert to JSON and then write to a String - ensures JSON read-in compatibility
        JSONArray jsonArr = new JSONArray();
        for (Writable obj : get())
        {
            jsonArr.put(((CompanyInfoWritable)obj).toJsonObject());
        }

        return jsonArr;
    }

    /**
     * toString method is override to produce JSON string
     * @return JSON string representation of the object
     */
    @Override
    public String toString() {
        try {
            JSONArray jsonArr = toJsonArray();
            return jsonArr.toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return "";
    }
}
