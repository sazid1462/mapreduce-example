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
import static com.sazid.mapreduce.Main.typeFieldKey;

/**
 * Combiner class for company and accounts data. As accounts and company information of are mapped under same org_number as key,
 * this combiner will create a combined CompanyInfoWritable object with additional 'accounts' field.
 */
public class CompanyJSONCombiner extends Reducer<Text, CompanyInfoWritable, Text, CompanyInfoWritable> {
    public void reduce(Text key, Iterable<CompanyInfoWritable> values, Context context) throws IOException, InterruptedException {
        CompanyInfoWritable company = null;
        ArrayList<CompanyInfoWritable> accounts = new ArrayList<>();
        for (CompanyInfoWritable val : values) {
            // Handle 'accounts' and 'company' values differently
            if (val.containsKey(typeFieldKey) && val.get(typeFieldKey).toString().equals("accounts")) {
                // remove the org_number/orgno field from the accounts objects as they will be reside within the company object as an array.
//                val.remove(orgNoFieldKey);
                // add the accounts info in the array
                accounts.add(new CompanyInfoWritable(val));
            } else {
                if (company == null) {
                    // If no company info is found already consider the first one as the final combined object
                    company = new CompanyInfoWritable(val);
                } else {
                    // If there are multiple such objects then just concatenate the accounts array together
                    CompanyJSONCombiner.addAccountsInfoToCompanyInfo(company, ((CompanyInfoArrayWritable)val.get(accountFieldKey)).get());
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
            company.put(accountFieldKey, new CompanyInfoArrayWritable(CompanyInfoWritable.class,
                    ArrayUtils.addAll(oldAccountsArr, accountsArr)));
        } else {
            company.put(accountFieldKey, new CompanyInfoArrayWritable(CompanyInfoWritable.class, accountsArr));
        }
    }
}
