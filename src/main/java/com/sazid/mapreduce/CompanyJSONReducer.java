package com.sazid.mapreduce;

import com.google.gson.Gson;
import com.sazid.utils.CompanyInfoWritable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CompanyJSONReducer extends Reducer<Text, CompanyInfoWritable, Text, Text> {
    public void reduce(Text key, Iterable<CompanyInfoWritable> values, Context context) throws IOException, InterruptedException {
        Gson gson = new Gson();
        Text typeField = new Text("type");
        Text accountsField = new Text("accounts");
        CompanyInfoWritable company = new CompanyInfoWritable();
        ArrayList<CompanyInfoWritable> accounts = new ArrayList<>();
        CompanyInfoWritable[] accountsArr = new CompanyInfoWritable[accounts.size()];
        accountsArr = accounts.toArray(accountsArr);
        for (CompanyInfoWritable val : values) {
            if (val.containsKey(typeField) && val.get(typeField).toString().equals("accounts")) {
                accounts.add(val);
            } else {
                company = val;
            }
        }
        CompanyJSONCombiner.addAccountsInfoToCompanyInfo(accountsField, company, accountsArr);
        context.write(key, new Text(gson.toJson(company)));
    }
}
