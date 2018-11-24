package com.sazid.mapreduce;

import com.sazid.utils.CompanyInfoArrayWritable;
import com.sazid.utils.CompanyInfoWritable;
import com.sazid.utils.Config;
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
 * Mapper class for company CSV files. It will map all the companies under org_number as key.
 */
public class CompanyCSVMapper extends Mapper<LongWritable, Text, Text, CompanyInfoWritable> {
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
            while (config.get("company.column.headers") == null) wait(10);
            String[] headers = config.get("company.column.headers").toString().split(",");

            Reader in = new StringReader(value.toString());
            CompanyInfoWritable company = new CompanyInfoWritable();
            // put a type flag to be able to distinguish 'company' and 'accounts' data in reducer
            company.put(typeFieldKey, new Text("company"));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                // map the company info from the row of csv file
                prepareCompanyInfo(company, record, headers);
                context.write(new Text(record.get(0)), company);
            }
            in.close();
        } else {
            System.out.println("Collecting header row with value: " + value.toString());
            Config config = Config.getConfig();
            config.put("company.column.headers", value.toString());
        }
    }

    /**
     * This method set the values of corresponding headers for the given record/row
     * @param company
     * @param record
     * @param headers
     */
    static void prepareCompanyInfo(CompanyInfoWritable company, CSVRecord record, String[] headers) {
        int colID = 0;
        for (String header : headers) {
            header = header.trim().toLowerCase();
            if (header.equals("")) {
                colID++;
                continue;
            }
            if (header.equals("org_number")) header = "orgno";
            String val = record.get(colID++);
//            if (val==null || val.equals("")) continue;
            company.put(new Text(header), new Text(val));
        }
        company.put(new Text("accounts"), new CompanyInfoArrayWritable(CompanyInfoWritable.class, new CompanyInfoWritable[0]));
    }
}
