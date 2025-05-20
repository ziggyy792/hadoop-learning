package homework;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test_4 {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    // 建立连接
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭连接
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 清空指定的表的所有记录数据
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void clearRows(String tableName) throws IOException {
        init();
        TableName tablename = TableName.valueOf(tableName);

        // 读取表的描述信息
        HTableDescriptor tDescriptor = admin.getTableDescriptor(tablename);

        // 删除表
        admin.disableTable(tablename);
        admin.deleteTable(tablename);

        // 重新建表
        admin.createTable(tDescriptor);
        close();
    }

    /**
     * 根据表名查找表信息
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void getData(String tableName) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            showCell(result);
        }
        close();
    }

    /**
     * 格式化输出
     *
     * @param result 结果
     */
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName(行键):" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp(时间戳):" + cell.getTimestamp() + " ");
            System.out.println("column Family（列簇）:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("column Name（列名）:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:（值）" + new String(CellUtil.cloneValue(cell)) + " ");
            System.out.println();
        }
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Test_4 test_4 = new Test_4();

        Scanner scan = new Scanner(System.in);
        System.out.println("请输入要清空的表名");
        String tableName = scan.nextLine();

        try {
            System.out.println("表原来的信息：");
            test_4.getData(tableName);
            test_4.clearRows(tableName);
            System.out.println("表已清空：");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}