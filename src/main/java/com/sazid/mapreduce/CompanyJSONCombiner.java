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

/**
 * Combiner class for company and accounts data. As accounts and company information of are mapped under same org_number as key,
 * this combiner will create a combined CompanyInfoWritable object with additional 'accounts' field.
 */
public class CompanyJSONCombiner extends Reducer<Text, CompanyInfoWritable, Text, CompanyInfoWritable> {
    /**
     * @param key is the org_number
     * @param values are the company and accounts info
     * @param context reducer context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<CompanyInfoWritable> values, Context context) throws IOException, InterruptedException {
        CompanyInfoWritable company = null;
        ArrayList<CompanyInfoWritable> accounts = new ArrayList<>();
        company = CompanyJSONReducer.combineValuesToObject(values, null, accounts);
        if (company == null) company = new CompanyInfoWritable();
        company.put(typeFieldKey, new Text("company"));
        // If there are accounts info for the company add them to companyInfo
        if (!accounts.isEmpty()) {
            CompanyInfoWritable[] accountsArr = new CompanyInfoWritable[accounts.size()];
            accountsArr = accounts.toArray(accountsArr);
            addAccountsInfoToCompanyInfo(company, accountsArr);
        }
        context.write(key, company);
    }

    static void addAccountsInfoToCompanyInfo(CompanyInfoWritable company, Writable[] accountsArr) {
        // Add accounts info to the company object under a new property 'accounts'. If there is already exists the accounts info then merge the new value with older.
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
