# bt_sample
<h1>Sample code to fetch data from GCP Bigtable.</h1>

<h2>Below program arguments needed</h2>
<ol>
<li>"ProjectId:"+args[0]</li>
<li>"InstanceId:"+args[1]</li>
<li>"TableId:"+args[2]</li>
<li>"Columns:"+args[3]: Comma separated columns names. First value is "key", which is bigtable row key</li>
<li>"Output File Path:"+args[4]:  path to csv file that will have the data</li>
</ol>


<h2>Below env var needed<h/2>
  
GOOGLE_APPLICATION_CREDENTIALS=
