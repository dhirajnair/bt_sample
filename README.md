# bt_sample
Sample code to fetch data from GCP Bigtable. 

Below program arguments needed
System.out.println("ProjectId:"+args[0]);
System.out.println("InstanceId:"+args[1]);
System.out.println("TableId:"+args[2]);
System.out.println("Columns:"+args[3]);// comma separated columsn names. First value is "key", which is bigtable row key
System.out.println("Output File Path:"+args[4]);// path to csv file that will have the data

Below env var needed
GOOGLE_APPLICATION_CREDENTIALS=
