package com.sazid.mapreduce;

import com.sazid.utils.CompanyInfoArrayWritable;
import com.sazid.utils.CompanyInfoWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;

import static com.sazid.mapreduce.Main.accountFieldKey;
import static com.sazid.mapreduce.Main.orgNoFieldKey;
import static com.sazid.mapreduce.Main.typeFieldKey;

/**
 * Reducer class to write combined JSON from accounts and company CSV files
 */
public class CompanyJSONReducer extends Reducer<Text, CompanyInfoWritable, NullWritable, Text> {
    private MultipleOutputs multipleOutputs;

    /**
     * Setup the multi file output with given context
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        multipleOutputs = new MultipleOutputs(context);

    }

    /**
     * reduce the mapped info to JSON
     * @param key is the org_number
     * @param values are the company and accounts info
     * @param context reducer context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<CompanyInfoWritable> values, Context context) throws IOException, InterruptedException {
        CompanyInfoWritable company = null;
        ArrayList<CompanyInfoWritable> accounts = new ArrayList<>();
        company = combineValuesToObject(values, null, accounts);
        if (company == null) company = new CompanyInfoWritable();
        company.put(typeFieldKey, new Text("company"));
        // If there are accounts info for the company add them to companyInfo
        if (!accounts.isEmpty()) {
            CompanyInfoWritable[] accountsArr = new CompanyInfoWritable[accounts.size()];
            accountsArr = accounts.toArray(accountsArr);
            CompanyJSONCombiner.addAccountsInfoToCompanyInfo(company, accountsArr);
        }
        // Write the JSON output of each company to separate directory with <org_number>__dir as the name. This directory will hold the part files.
        multipleOutputs.write(NullWritable.get(), new Text(company.toString()), key.toString()+"__dir/"+key.toString());
    }

    static CompanyInfoWritable combineValuesToObject(Iterable<CompanyInfoWritable> values, CompanyInfoWritable company, ArrayList<CompanyInfoWritable> accounts) {
        for (CompanyInfoWritable val : values) {
            // Handle 'accounts' and 'company' values differently
            if (val.containsKey(typeFieldKey) && val.get(typeFieldKey).toString().equals("accounts")) {
                // remove the org_number/orgno field from the accounts objects as they will be reside within the company object as an array.
                val.remove(orgNoFieldKey);
                // add the accounts info in the array. Constructing a new object is compulsory. This will make a copy of the object.
                accounts.add(new CompanyInfoWritable(val));
            } else {
                if (company == null) {
                    company = new CompanyInfoWritable(val);
                } else {

                    CompanyJSONCombiner.addAccountsInfoToCompanyInfo(company, ((CompanyInfoArrayWritable)val.get(accountFieldKey)).get());
                }
            }
        }
        return company;
    }

    /**
     * Close the multi file output in cleanup phase
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        multipleOutputs.close();
    }
}
