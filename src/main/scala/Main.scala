import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager}
import java.io.File
import java.io.FileNotFoundException
import java.util.{InputMismatchException, Scanner}
import java.sql.{Connection, DriverManager, SQLException}
import scala.io.StdIn._
import scala.sys.exit

object MainPage {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder()
      .appName("Branch Out")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session");

    spark.sparkContext.setLogLevel("WARN")
    //spark.sql("Drop table Bev_BranchA")
    spark.sql("CREATE TABLE IF NOT EXISTS Bev_BranchA(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Bev_BranchA")
    spark.sql("CREATE TABLE IF NOT EXISTS Bev_BranchB(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' OVERWRITE INTO TABLE Bev_BranchB")
    spark.sql("CREATE TABLE IF NOT EXISTS Bev_BranchC(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' OVERWRITE INTO TABLE Bev_BranchC")
    spark.sql("CREATE TABLE IF NOT EXISTS Bev_ConscountA(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' OVERWRITE INTO TABLE Bev_ConscountA")
    spark.sql("CREATE TABLE IF NOT EXISTS Bev_ConscountB(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' OVERWRITE INTO TABLE Bev_ConscountB")
    spark.sql("CREATE TABLE IF NOT EXISTS Bev_ConscountC(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' OVERWRITE INTO TABLE Bev_ConscountC")
    spark.sql(sqlText = "SELECT * FROM Bev_BranchA").show()
    spark.sql(sqlText = "SELECT * FROM Bev_BranchB").show()
    spark.sql(sqlText = "SELECT * FROM Bev_BranchC").show()
    spark.sql(sqlText = "SELECT * FROM Bev_ConscountA").show()
    spark.sql(sqlText = "SELECT * FROM Bev_ConscountB").show()
    spark.sql(sqlText = "SELECT * FROM Bev_ConscountC").show()


    spark.sql("CREATE TABLE IF NOT EXISTS Bev_BranchABC(Beverage STRING, Branch STRING) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Bev_BranchABC")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Bev_BranchABC")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Bev_BranchABC")

    spark.sql("CREATE TABLE IF NOT EXISTS Bev_ConscountABC(Beverage STRING, Count INT) row format delimited fields terminated by ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Bev_BranchA")
    println("--");

    val box = 1
    while (box == 1) {

      println("Welcome, please select a Scenario to view.")
      println("1. Scenario 1")
      println("2. Scenario 2")
      println("3. Scenario 3")
      println("4. Scenario 4")
      println("5. Scenario 5")
      println("6. Scenario 6")
      println("7. Exit")

      val x = scala.io.StdIn.readLine()
      val myBoolean = true;

      if (x.equals("1")) {
        println("Scenario 1");

        spark.sql("SELECT COUNT(Branch) FROM Bev_BranchA WHERE Branch = 'Branch1'").show();
        spark.sql("SELECT COUNT(Branch) FROM Bev_BranchB WHERE Branch = 'Branch1'").show();
        spark.sql("SELECT COUNT(Branch) FROM Bev_BranchC WHERE Branch = 'Branch1'").show();
        println("Result: 20 consumers of Branch1.")

        spark.sql("SELECT COUNT(Branch) FROM Bev_BranchA WHERE Branch = 'Branch2'").show();
        spark.sql("SELECT COUNT(Branch) FROM Bev_BranchB WHERE Branch = 'Branch2'").show();
        spark.sql("SELECT COUNT(Branch) FROM Bev_BranchC WHERE Branch = 'Branch2'").show();
        println("Result: 80 consumers of Branch2.")
      }

      else if (x.equals("2")) {
        println("Scenario 2");
        spark.sql("SELECT MAX(Beverage) FROM Bev_BranchA WHERE Branch = 'Branch1'").show();
        spark.sql("SELECT MAX(Beverage) FROM Bev_BranchB WHERE Branch = 'Branch1'").show();
        spark.sql("SELECT MAX(Beverage) FROM Bev_BranchC WHERE Branch = 'Branch1'").show();
        println("Result: Triple Mocha is the most consumed beverage for Branch1.");

        spark.sql("SELECT MIN(Beverage) FROM Bev_BranchA WHERE Branch = 'Branch2'").show();
        spark.sql("SELECT MIN(Beverage) FROM Bev_BranchB WHERE Branch = 'Branch2'").show();
        spark.sql("SELECT MIN(Beverage) FROM Bev_BranchC WHERE Branch = 'Branch2'").show();
        println("Result: Cold Lite and Cold Coffee are tied for the two least consumed beverages for Branch2");

        spark.sql("SELECT COUNT(Beverage) FROM Bev_BranchA WHERE Branch = 'Branch2'").show();
        spark.sql("SELECT COUNT(Beverage) FROM Bev_BranchB WHERE Branch = 'Branch2'").show();
        spark.sql("SELECT COUNT(Beverage) FROM Bev_BranchC WHERE Branch = 'Branch2'").show();
        println("The average number of beverages consumed by Branch2 is roughly 27 ((20 + 60)/3).");
      }

      else if (x.equals("3")) {
        println("Scenario 3");

        println("Branch8 offers the following beverages:")
        spark.sql("SELECT Beverage FROM Bev_BranchABC WHERE Branch = 'Branch8'").show()
        //spark.sql("SELECT Beverage FROM Bev_BranchB WHERE Branch = 'Branch8'").show()
        //spark.sql("SELECT Beverage FROM Bev_BranchC WHERE Branch = 'Branch8'").show()

        println("Branch1 offers the following beverages:")
        spark.sql("SELECT Beverage FROM Bev_BranchABC WHERE Branch = 'Branch1'").show()
        //spark.sql("SELECT Beverage FROM Bev_BranchB WHERE Branch = 'Branch1'").show()
        //spark.sql("SELECT Beverage FROM Bev_BranchC WHERE Branch = 'Branch1'").show()

        println("Branches 4 and 7 share the following beverages:")
        spark.sql("SELECT DISTINCT Bev_BranchABC.Beverage FROM Bev_BranchABC " +
          "WHERE Bev_BranchABC.Branch = 'Branch4' OR Bev_BranchABC.Branch = 'Branch7'").show()

        //code after ON keyword needs to be an equivalent, ON is where you tell the join how to join
        //joins only work for two tables
        //inner join only takes one column that intersects

      }
      else if (x.equals("4")) {
        println("Scenario 4");
        spark.sql("DROP TABLE IF EXISTS Partly_of_Four");

        spark.sql("CREATE TABLE IF NOT EXISTS Attempt_100(Beverage String) PARTITIONED BY (Branch String)" +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").show()
        spark.sql("SELECT * FROM Attempt_100").show()
        spark.sql("show partitions Attempt_100").show()

      }
      else if (x.equals("5")) {
        println("Scenario 5");

        spark.sql("ALTER TABLE Bev_BranchABC SET TBLPROPERTIES('Note to Self:'='Tommie4Life!')").show()
        spark.sql("SHOW TBLPROPERTIES Bev_BranchABC").show()

        spark.sql("SELECT * FROM Bev_BranchABC WHERE NOT Branch = 'Branch1' AND NOT Branch = 'Branch5'").show()

      }
      else if (x.equals("6")) {
        println("What does this mean going forward?");

        spark.sql("CREATE TABLE IF NOT EXISTS Branch_A_Outlook(Beverage String, Branch_No String, Sales Int, Year Int)" +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','").show()
        spark.sql("LOAD DATA LOCAL INPATH 'input/Future_Query.txt' OVERWRITE INTO TABLE Branch_A_Outlook").show()
        spark.sql("SELECT * FROM Branch_A_Outlook ORDER BY Year ASC, Beverage, Sales, Branch_No").show(50)
      }


      else {
        //val myBoolean = false;
        println("Thank you, come again!")

      }
    }
  }
}
