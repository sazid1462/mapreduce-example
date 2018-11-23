package com.sazid.utils;

import com.sun.istack.NotNull;;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.StringTokenizer;

import static com.sazid.mapreduce.Main.accountFieldKey;
import static com.sazid.mapreduce.Main.typeFieldKey;

public class CompanyInfoWritable extends SortedMapWritable implements WritableComparable<CompanyInfoWritable> {

    @Override
    public int compareTo(@NotNull CompanyInfoWritable o) {
        // Implement your compare logic
        return 0;
    }

    public JSONObject toJsonObject() throws JSONException {
        JSONObject jsonObj = new JSONObject();
        Config config = Config.getConfig();
        String type = get(typeFieldKey).toString();
        String[] headers;
        if (type.equals("accounts")) {
            headers = config.get("accounts.column.headers").toString().split(",");
        } else {
            headers = config.get("company.column.headers").toString().split(",");
        }
        for (String header : headers) {
            Text key = new Text(header.trim().toLowerCase());
            var keyStr = key.toString();
            keyStr = keyStr.equals("org_number") ? "orgno" : keyStr;
            switch (keyStr) {
                case "type":
                    break;
                default:
                    if (!containsKey(key)) break;
                    var val = get(key).toString();
                    if (val.equals("")) continue;
                    jsonObj.put(keyStr, val);
                    break;
            }
        }
        if (containsKey(accountFieldKey)) {
            CompanyInfoArrayWritable arr = (CompanyInfoArrayWritable) get(accountFieldKey);
            var jsonArr = arr.toJsonArray();
            if (jsonArr.length()!=0) {
                jsonObj.put(accountFieldKey.toString(), jsonArr);
            }
        }

        return jsonObj;
    }

    @Override
    public String toString() {
        // Convert to JSON and then write to a String - ensures JSON read-in compatibility
        JSONObject jsonObj = null;
        try {
            jsonObj = toJsonObject();
            return jsonObj.toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }
}