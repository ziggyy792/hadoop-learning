package homework;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class Test_5 {
	public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    //建立连接
    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
 
     public static void countRows (String tableName) throws IOException
     {
         init();
         Table table = connection.getTable(TableName.valueOf(tableName));
         Scan scan = new Scan();
         ResultScanner scanner =table.getScanner(scan);
         int num = 0;
         for(Result result = scanner.next();result!=null;result=scanner.next())
         {
             num++;
         }
         System.out.println("行数："+num);
         scanner.close();
         close();
     }
    
    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        Test_5 test_5=new Test_5();
        Scanner scan = new Scanner(System.in);
        System.out.println("请输入要统计行数的表名");
        String tableName=scan.nextLine();
        test_5.countRows(tableName);
    }

}
