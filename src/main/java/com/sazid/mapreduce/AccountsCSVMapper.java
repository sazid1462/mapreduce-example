package com.sazid.mapreduce;

import com.sazid.utils.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import static com.sazid.mapreduce.Main.typeFieldKey;

/**
 * Mapper class for accounts CSV files. It will map all the accounts info of the same company under same key.
 */
public class AccountsCSVMapper extends Mapper<LongWritable, Text, Text, CompanyInfoWritable> {
    /**
     * @param key is the position in the input file
     * @param value is the line of text from the input file at the key position
     * @param context mapper context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // handle header row and normal row differently
        if (key.get() > 0) {
            // get the global configuration
            Config config = Config.getConfig();
            // wait until the headers row is available in config
            while (config.get("accounts.column.headers") == null) wait(10);
            String[] headers = config.get("accounts.column.headers").toString().split(",");

            Reader in = new StringReader(value.toString());
            CompanyInfoWritable accounts = new CompanyInfoWritable();
            // put a type flag to be able to distinguish 'company' and 'accounts' data in reducer
            accounts.put(typeFieldKey, new Text("accounts"));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                // map the company info from the row of csv file
                CompanyCSVMapper.prepareCompanyInfo(accounts, record, headers);
                context.write(new Text(record.get(1)), accounts);
            }
            in.close();
        } else {
            System.out.println("Collecting header row with value: " + value.toString());
            Config config = Config.getConfig();
            config.put("accounts.column.headers", value.toString());
        }
    }

}
