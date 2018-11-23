package com.sazid.mapreduce;

import com.google.gson.Gson;
import com.sazid.utils.CompanyInfoArrayWritable;
import com.sazid.utils.CompanyInfoWritable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompanyJSONCombiner extends Reducer<Text, CompanyInfoWritable, Text, CompanyInfoWritable> {
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
        addAccountsInfoToCompanyInfo(accountsField, company, accountsArr);
        context.write(key, company);
    }

    static void addAccountsInfoToCompanyInfo(Text accountsField, CompanyInfoWritable company, CompanyInfoWritable[] accountsArr) {
        if (company.containsKey(accountsField)) {
            CompanyInfoArrayWritable tmp = (CompanyInfoArrayWritable) company.get(accountsField);
            Writable[] oldAccountsArr = tmp.get();
            tmp.set(ArrayUtils.addAll(oldAccountsArr, accountsArr));
            company.put(accountsField, tmp);
        } else {
            company.put(accountsField, new CompanyInfoArrayWritable(CompanyInfoWritable.class, accountsArr));
        }
    }
}
