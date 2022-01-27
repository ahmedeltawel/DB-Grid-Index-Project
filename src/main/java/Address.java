
import java.io.Serializable;

/**
 * Bucket Entry Class <value, PageName, RowIndex> = 400, Employee5.ser, 10
 * 
 */
public class Address implements Serializable {
   public String tableName;
   public Object pkValue;
   public int pageNumber;

   // public static Vector<Page> pagesBuffer = new Vector<Page>();
   // public static Vector<Table> tablesBuffer = new Vector<Table>();

   public Address(String TableName, Object pkValue, int pageNumber) {
      this.tableName = TableName;
      this.pkValue = pkValue;
      this.pageNumber = pageNumber;
   }

   public Row grapRow(Table table) {

      Page page = null;
      // Table table = null;
      // for (Table t : tablesBuffer) {
      // if (tableName.equals(t.TableName)) {
      // table = t;
      // break;
      // }
      // }
      // for (Page p : pagesBuffer) {
      // if (this.pageNumber == p.number) {
      // page = p;
      // break;
      // }
      // }
      // if(table == null){
      // table = Table.loadTable(tableName);
      // tablesBuffer.add(table);
      // }
      // if (page == null) {
      try {
         page = table.loadPage(pageNumber);
         // pagesBuffer.add(page);
      } catch (Exception e) {
         System.out.println(
               String.format("Error while trying to grap row from page %s of table %s", pageNumber, tableName));
         e.printStackTrace();
      }
      // }
      // String pkColumnName = table.getPrimaryColumn().name;

      int rowNumber = table.BinarySearchForRow(page, pkValue);
      return page.Rows.get(rowNumber);

      // return null/;
   }

}
