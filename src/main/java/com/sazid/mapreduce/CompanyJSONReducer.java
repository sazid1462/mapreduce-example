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
import static com.sazid.mapreduce.Main.typeFieldKey;

public class CompanyJSONReducer extends Reducer<Text, CompanyInfoWritable, NullWritable, Text> {
    private MultipleOutputs multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        multipleOutputs = new MultipleOutputs(context);

    }

    public void reduce(Text key, Iterable<CompanyInfoWritable> values, Context context) throws IOException, InterruptedException {
        CompanyInfoWritable company = null;
        ArrayList<CompanyInfoWritable> accounts = new ArrayList<>();
        for (CompanyInfoWritable val : values) {
            if (val.containsKey(typeFieldKey) && val.get(typeFieldKey).toString().equals("accounts")) {
                accounts.add(new CompanyInfoWritable(val));
            } else {
                if (company == null) {
                    company = new CompanyInfoWritable(val);
                } else {

                    CompanyJSONCombiner.addAccountsInfoToCompanyInfo(company, ((CompanyInfoArrayWritable)val.get(accountFieldKey)).get());
                }
            }
        }
        if (company == null) company = new CompanyInfoWritable();
        company.put(typeFieldKey, new Text("company"));
        if (!accounts.isEmpty()) {
            CompanyInfoWritable[] accountsArr = new CompanyInfoWritable[accounts.size()];
            accountsArr = accounts.toArray(accountsArr);
            CompanyJSONCombiner.addAccountsInfoToCompanyInfo(company, accountsArr);
        }
        multipleOutputs.write(NullWritable.get(), new Text(company.toString()), key.toString()+"__dir/"+key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        multipleOutputs.close();
    }
}
