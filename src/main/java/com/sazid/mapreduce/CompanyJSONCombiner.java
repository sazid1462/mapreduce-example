package com.sazid.mapreduce;

import com.sazid.utils.CompanyInfoArrayWritable;
import com.sazid.utils.CompanyInfoWritable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import static com.sazid.mapreduce.Main.accountFieldKey;
import static com.sazid.mapreduce.Main.orgNoFieldKey;
import static com.sazid.mapreduce.Main.typeFieldKey;

public class CompanyJSONCombiner extends Reducer<Text, CompanyInfoWritable, Text, CompanyInfoWritable> {
    public void reduce(Text key, Iterable<CompanyInfoWritable> values, Context context) throws IOException, InterruptedException {
        CompanyInfoWritable company = null;
        ArrayList<CompanyInfoWritable> accounts = new ArrayList<>();
        for (CompanyInfoWritable val : values) {
            if (val.containsKey(typeFieldKey) && val.get(typeFieldKey).toString().equals("accounts")) {
                val.remove(orgNoFieldKey);
                accounts.add(val);
            } else {
                if (company == null) {
                    company = val;
                } else {
                    CompanyJSONCombiner.addAccountsInfoToCompanyInfo(company, (CompanyInfoWritable[]) ((CompanyInfoArrayWritable)val.get(accountFieldKey)).get());
                }
            }
        }
        if (company == null) company = new CompanyInfoWritable();
        company.put(typeFieldKey, new Text("company"));
        if (!accounts.isEmpty()) {
            CompanyInfoWritable[] accountsArr = new CompanyInfoWritable[accounts.size()];
            accountsArr = accounts.toArray(accountsArr);
            addAccountsInfoToCompanyInfo(company, accountsArr);
        }
        context.write(key, company);
    }

    static void addAccountsInfoToCompanyInfo(CompanyInfoWritable company, Writable[] accountsArr) {
        if (company.containsKey(accountFieldKey)) {
            CompanyInfoArrayWritable tmp = (CompanyInfoArrayWritable) company.get(accountFieldKey);
            Writable[] oldAccountsArr = tmp.get();
            tmp.set(ArrayUtils.addAll(oldAccountsArr, accountsArr));
            company.put(accountFieldKey, tmp);
        } else {
            company.put(accountFieldKey, new CompanyInfoArrayWritable(CompanyInfoWritable.class, accountsArr));
        }
    }
}
