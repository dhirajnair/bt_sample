import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class BTSample {
    public static void main(String[] args) {
        System.out.println("ProjectId:"+args[0]);
        System.out.println("InstanceId:"+args[1]);
        System.out.println("TableId:"+args[2]);
        System.out.println("Columns:"+args[3]);
        System.out.println("Output File Path:"+args[4]);
        String projectId = args[0];
        String instanceId = args[1];
        String tableId = args[2];
        writeCSVData(args[4],filterlimitCells(projectId,instanceId,tableId,args[3]));
    }

    public static List<String[]> filterlimitCells(String projectId, String instanceId, String tableId,String cols) {

        // A filter that matches cells and limits the number of cell values returned
        List columns = Arrays.asList(cols.split(","));
        Filters.Filter ifilter = FILTERS.interleave();
        int skipKey=0;
        for (Object col: columns){
            if(skipKey==0){
                skipKey++;
                continue;
            }
            ((Filters.InterleaveFilter)ifilter).filter(FILTERS.qualifier().exactMatch((String) col));
        }
        Filters.Filter filter = FILTERS
                .chain()
                .filter(ifilter)
                .filter(FILTERS.limit().cellsPerColumn(1));

        return readFilter(projectId, instanceId, tableId, filter,cols);
    }

    public static void writeCSVData(String filePath,List<String[]> data)
    {

        // first create file object for file placed at location
        // specified by filepath
        File file = new File(filePath);
        FileWriter outputfile = null;
        CSVWriter writer = null;
        try {
            // create FileWriter object with file as parameter
           outputfile = new FileWriter(file);

            // create CSVWriter with ',' as separator
           writer = new CSVWriter(outputfile, ',',
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
            writer.writeAll(data);
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if(writer!=null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<String[]> readFilter(
            String projectId, String instanceId, String tableId, Filters.Filter filter,String cols) {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests. After completing all of your requests, call
        // the "close" method on the client to safely clean up any remaining background resources.
        BigtableDataClient dataClient=null;
        System.out.printf("Reading data...");
        List<String[]> d= new ArrayList<>();
        d.add(cols.split(","));
        try {
            dataClient = BigtableDataClient.create(projectId, instanceId);
            Query query = Query.create(tableId).filter(filter);
            ServerStream<Row> rows = dataClient.readRows(query);
            int i=0;
            for (Row row : rows) {
                //printRow(row);
                d.add(writeRow(row));
                i++;
            }
            System.out.println("Rowcount:"+i);
        } catch (IOException e) {
            System.out.println(
                    "Unable to initialize service client, as a network error occurred: \n" + e.toString());
        }finally {
            if(dataClient!=null)dataClient.close();
        }
        return d;
    }

    private static String [] writeRow(Row row) {
        String[] data = new String[row.getCells().size()+1];
        String key = row.getKey().toStringUtf8();
        data[0]=key;
        int i=1;
        for (RowCell cell : row.getCells()) {
           data[i++]=cell.getValue().toStringUtf8();
        }
        return data;
    }

    private static void printRow(Row row) {
        System.out.printf("Reading data for %s%n", row.getKey().toStringUtf8());
        String colFamily = "";
        for (RowCell cell : row.getCells()) {
            if (!cell.getFamily().equals(colFamily)) {
                colFamily = cell.getFamily();
                System.out.printf("Column Family %s%n", colFamily);
            }
            System.out.printf(
                    "\t%s: %s @%s%n",
                    cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8(), cell.getTimestamp());
        }
        System.out.println();
    }
}
