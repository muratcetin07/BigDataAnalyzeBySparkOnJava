package callCenterAnalysis;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlCallCenterDataAnalysis {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("ChicagoCallCenterDataAnalysis")
                .getOrCreate();

        //analyze chicago 311 call center service data on spark

        Dataset<Row> rowData = sparkSession.read().option("header", "true")
                .csv("C:\\Users\\Murat Çetin\\Downloads\\311chicago.csv");

        Dataset<Row> selectedData = rowData.select(new Column("Creation_Date"), new Column("Status"),
                new Column("Completion_Date"), new Column("Service_Request_Number"),
                new Column("Type_of_Service_Request"), new Column("Street_Address"));

        Dataset<Row> callerNumberGroupData = selectedData.groupBy(new Column("Service_Request_Number")).count();

        Dataset<Row> filteredData = callerNumberGroupData.filter(new Column("count").equalTo("5"));

        Dataset<Row>  specificNumber = selectedData.filter(new Column("Service_Request_Number").equalTo("14-01783521"));

        specificNumber.show();

        filteredData.show();

        callerNumberGroupData.show();

    }
}
