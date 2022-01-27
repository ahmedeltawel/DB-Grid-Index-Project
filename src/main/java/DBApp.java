import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import net.sf.jsqlparser.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.SelectUtils;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.table.Index.ColumnParams;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.DateValue;

import org.apache.commons.lang3.StringUtils;

public class DBApp implements DBAppInterface {
   static int entered = 0;

   @Override
   public void init() {
      helper.createFolder("src/main/resources/data/");
      helper.createFile("src/main/resources/metadata.csv");
   }

   @Override
   public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
         Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
      if (Table.loadTable(tableName) == null) {
         Table t = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
         t.saveTable();
      } else {
         System.out.println(String.format("Can't Create Table %s already exists", tableName));
      }

   }

   @Override
   public void createIndex(String tableName, String[] columnNames) throws DBAppException {
      Table table = Table.loadTable(tableName);
      Vector<Column> indexingColumns = new Vector<Column>();

      for (Column col : table.Columns)
         for (String inputColName : columnNames)
            if (col.name.equals(inputColName)) {
               indexingColumns.add(col);
               col.indexed = true;
            }

      if (indexingColumns.isEmpty())
         throw new DBAppException("Invalid column Names while creating the index!");
         
      table.indices.add(new GridIndex(indexingColumns, table));

      int firstPageNumber = table.PagesPath.keys().asIterator().next();
      Page p = null;
      try {
         p = table.loadPage(firstPageNumber);
      } catch (Exception e1) {
         e1.printStackTrace();
      }

      // Fill the index if table isn't Empty
      if (p.Rows.size() > 0) {

         for (int pageNumber : table.PagesPath.keySet()) {
            Page page = null;
            try {
               page = table.loadPage(pageNumber);
            } catch (Exception e) {
               e.printStackTrace();
            }
            for (int i = 0; i < page.Rows.size(); i++) {
               Row row = page.Rows.get(i);
               table.indiciesAddRow(row,
                     new Address(table.TableName, row.get(table.getPrimaryColumn().name), pageNumber));
            }
            try {
               table.unloadPage(page);
            } catch (Exception e) {
               e.printStackTrace();
            }
         }

      }
      table.saveTable();
      updateMetaData(table);
   }

   @Override
   public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
      Table t = Table.loadTable(tableName);
      updateConstraintsUsingMetadata(t);
      String PrimColName = t.getPrimaryColumn().name;
      Integer pageNumberIndex = null;
      GridIndex searchIndex = t.searchingIndex(colNameValue);
      Query indexdColumns = new Query();
      for (String colName : colNameValue.keySet()) {
         if (isIndexedColumnName(searchIndex, colName)) {
            if (indexdColumns.size() != 0)
               indexdColumns.operators.add("AND");
            SQLTerm term = new SQLTerm();
            term._strTableName = tableName;
            term._strColumnName = colName;
            term._strOperator = "=";
            term._objValue = colNameValue.get(colName);
            indexdColumns.terms.add(term);
         }
      }
      if (indexdColumns.size() != 0) {
         Vector<GridCell> matchingGridCells = searchIndex.evaluateIndexQuery(indexdColumns);
         loop: for (GridCell gridCell : matchingGridCells) {
            for (String bucketPath : gridCell.bucketList) {
               Bucket bucket = gridCell.loadBucket(bucketPath);
               nextRow: for (Address address : bucket.addresses) {
                  Comparable pkValue = (Comparable) colNameValue.get(PrimColName);

                  Page page = null;
                  try {
                     page = t.loadPage(address.pageNumber);
                     if (helper.compareObjects(pkValue, page.minPkeyValue) >= 0
                           && (helper.compareObjects(pkValue, page.maxPkeyValue) <= 0)) {
                        pageNumberIndex = page.number;
                        break loop;
                     } else {
                        Page nextPage = null;
                        try {
                           nextPage = t.loadPage(address.pageNumber + 1);
                        } catch (Exception e) {
                           pageNumberIndex = page.number;
                           break loop;
                        }
                        if (helper.compareObjects(pkValue, page.maxPkeyValue) <= 0
                              && (helper.compareObjects(pkValue, nextPage.minPkeyValue) >= 0)) {
                           pageNumberIndex = page.number;
                           break loop;
                        } else {
                           Page prevPage = null;
                           try {
                              prevPage = t.loadPage(address.pageNumber - 1);
                           } catch (Exception e) {
                              pageNumberIndex = 0;
                              break loop;
                           }
                           if (helper.compareObjects(pkValue, page.minPkeyValue) <= 0
                                 && (helper.compareObjects(pkValue, prevPage.maxPkeyValue) >= 0)) {
                              pageNumberIndex = prevPage.number;
                              break loop;
                           }
                        }
                     }

                  } catch (Exception e) {
                     e.printStackTrace();
                  }
                  break nextRow;
               }

            }
         }

      }

      Page page = null;
      Row r = new Row();
      r.putAll(colNameValue);

      Object value = r.get(PrimColName);

      if (value == null)
         throw new DBAppException(String.format("Primary key can't be null"));

      try {
         if (t.PagesPath.size() == 1 && t.loadPage(0).Rows.size() == 0) {
            try {
               page = t.loadPage(0);
            } catch (Exception e4) {
               throw new DBAppException(String.format("Primary key can't be null"));
            }
            try {
               page.verifyAddRow(r);
               page.Rows.add(0, r);
               page.updatePageMinMaxAddRow(r);
            } catch (Exception e3) {
               throw new DBAppException(e3.getMessage());
            }
            try {
               t.unloadPage(0);
            } catch (Exception e2) {
               throw new DBAppException(e2.getMessage());
            }

            return;
         }
      } catch (Exception e5) {
         throw new DBAppException(e5.getMessage());
      }

      if (t.PagesPath.size() == 0) {
         t.addPageLast();
         try {
            page = t.loadPage(0);
         } catch (Exception e4) {
            // TODO Auto-generated catch block
            throw new DBAppException(e4.getMessage());
         }
         try {
            page.verifyAddRow(r);
            page.Rows.add(0, r);
            page.updatePageMinMaxAddRow(r);
         } catch (Exception e3) {
            throw new DBAppException(e3.getMessage());
         }
         try {
            t.unloadPage(0);
         } catch (Exception e2) {
            // TODO Auto-generated catch block
            throw new DBAppException(e2.getMessage());
         }

      } else {
         if (pageNumberIndex != null)
            try {
               page = t.loadPage(pageNumberIndex);
            } catch (Exception e) {
               e.printStackTrace();
            }
         else {
            page = t.BinarySearchForPageInsert(value);
            Integer n = null;
            if (page != null) {
               n = t.BinarySearchForRow(page, value);
               if (n != null)
                  throw new DBAppException("Primary key must be unique");
            }
         }
         if (!page.isFull()) {
            int com = helper.compareObjects(page.maxPkeyValue, value);
            if (com < 0) {
               try {
                  page.verifyAddRow(r);
                  page.Rows.add(page.Rows.size(), r);
                  page.updatePageMinMaxAddRow(r);
               } catch (Exception e3) {
                  throw new DBAppException(e3.getMessage());
               }
               try {
                  t.unloadPage(page);
               } catch (Exception e2) {
                  // TODO Auto-generated catch block
                  throw new DBAppException(e2.getMessage());
               }
            } else {
               int rowNumber = t.BinarySearchForRowInsert(page, value);
               try {
                  page.verifyAddRow(r);
                  page.Rows.add(rowNumber, r);
                  page.updatePageMinMaxAddRow(r);
               } catch (Exception e3) {
                  throw new DBAppException(e3.getMessage());
               }
               try {
                  t.unloadPage(page.number);
               } catch (Exception e2) {
                  // TODO Auto-generated catch block
                  throw new DBAppException(e2.getMessage());
               }
            }

         } else {
            Row move = null;
            int com = helper.compareObjects(page.maxPkeyValue, value);

            // Insert in page 1 and get record to be moved to page2
            if (com < 0) {
               move = r;
            } else {
               int rowNumber = t.BinarySearchForRowInsert(page, value);
               move = page.Rows.remove(page.Rows.size() - 1);
               page.updatePageMinMaxRemoveRow(move);
               try {
                  page.verifyAddRow(r);
                  page.Rows.add(rowNumber, r);
                  t.indiciesAddRow(r, new Address(t.TableName, r.get(t.getPrimaryColumn().name), page.number));
                  page.updatePageMinMaxAddRow(r);
               } catch (Exception e3) {
                  throw new DBAppException(e3.getMessage());
               }
               try {
                  t.unloadPage(page.number);
               } catch (Exception e2) {
                  // TODO Auto-generated catch block
                  throw new DBAppException(e2.getMessage());
               }
            }

            // Insert in Page 2
            if (t.PagesPath.get(page.number + 1) != null) {
               Page p2 = null;
               try {
                  p2 = t.loadPage(page.number + 1);
               } catch (Exception e1) {
                  // TODO Auto-generated catch block
                  throw new DBAppException(e1.getMessage());
               }
               // Insert if page 2 is not full
               if (!p2.isFull()) {
                  p2.Rows.add(0, move);
                  t.indiciesUpdateRow(move, move,
                        new Address(t.TableName, move.get(t.getPrimaryColumn().name), p2.number));
                  p2.updatePageMinMaxAddRow(move);
                  try {
                     t.unloadPage(p2.number);
                  } catch (Exception e2) {
                     // TODO Auto-generated catch block
                     throw new DBAppException(e2.getMessage());
                  }
               } else {
                  // overflow
                  page = t.addoverflow(page.number + 1);
                  page.Rows.add(0, move);
                  t.indiciesUpdateRow(move, move,
                        new Address(t.TableName, move.get(t.getPrimaryColumn().name), p2.number));

                  page.updatePageMinMaxAddRow(move);

                  try {
                     t.unloadPage(page.number);
                  } catch (Exception e2) {
                     throw new DBAppException(e2.getMessage());
                  }

               }

            }

            // Create new page to move the record`
            else {
               page = t.addPageLast();
               page.Rows.add(0, move);
               t.indiciesUpdateRow(move, move,
                     new Address(t.TableName, move.get(t.getPrimaryColumn().name), page.number));
               page.updatePageMinMaxAddRow(move);

               try {
                  t.unloadPage(page.number);
               } catch (Exception e2) {
                  // TODO Auto-generated catch block
                  throw new DBAppException(e2.getMessage());
               }

            }

         }

      }
      t.saveTable();

   }

   @Override
   public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
         throws DBAppException {
      if (StringUtils.isEmpty(clusteringKeyValue))
         throw new DBAppException("clusteringKeyValue Must not be empty!");

      Table table = Table.loadTable(tableName);
      updateConstraintsUsingMetadata(table);

      GridIndex searchIndex = table.searchingIndex(columnNameValue);

      Query indexdColumns = new Query();
      for (String colName : columnNameValue.keySet()) {
         if (isIndexedColumnName(searchIndex, colName)) {
            if (indexdColumns.size() != 0)
               indexdColumns.operators.add("AND");
            SQLTerm term = new SQLTerm();
            term._strTableName = tableName;
            term._strColumnName = colName;
            term._strOperator = "=";
            term._objValue = columnNameValue.get(colName);
            indexdColumns.terms.add(term);
         }
      }
      Row updateRow = null;
      if (indexdColumns.size() != 0) {
         Vector<GridCell> matchingGridCells = searchIndex.evaluateIndexQuery(indexdColumns);
         loop: for (GridCell gridCell : matchingGridCells) {
            for (String bucketPath : gridCell.bucketList) {
               Bucket bucket = gridCell.loadBucket(bucketPath);

               nextRow: for (Address address : bucket.addresses) {
                  Row row = address.grapRow(table);
                  for (String colName : columnNameValue.keySet()) {
                     Comparable value = (Comparable) columnNameValue.get(colName);
                     if (value.compareTo(row.get(colName)) != 0)
                        continue nextRow;
                     updateRow = row;
                     break loop;
                  }
               }
            }
         }
      }

      String pkColName = table.getPrimaryColumn().name;

      Page page = null;
      int idx = -1;
      Row row = null;

      if (updateRow != null)
         row = updateRow;
      else {
         Object searchValue = helper.parseString(clusteringKeyValue);
         try {
            page = table.BinarySearchForPage(searchValue);
            idx = table.BinarySearchForRow(page, searchValue);
            // Searching for Row will throw Null pointer Excetion
         } catch (Exception e) {
            // System.out.println("Search Value not Found, update rejected");
            throw new DBAppException("WARN: Search Value not Found, update rejected");
         }
         row = page.Rows.get(idx);
      }

      // Verify updated values do not change PK value
      Object pkValue = row.get(pkColName);
      // String updatedPKvalue = String.valueOf( columnNameValue.get(pkCol) );
      Object updatedPKvalue = columnNameValue.get(pkColName);
      if (updatedPKvalue != null)
         if (!pkValue.equals(updatedPKvalue))
            throw new DBAppException("Primary Key value Cannot be updated, Row update rejected!");

      Row r = new Row();
      Enumeration<String> e = columnNameValue.keys();
      while (e.hasMoreElements()) {
         String key = e.nextElement();
         r.put(key, columnNameValue.get(key));
      }

      try {
         page.verifyValidRow(r);
      } catch (Exception e1) {
         throw new DBAppException(e1.getMessage());
      }

      Row oldRow = row;
      row.replaceAll((key, oldValue) -> (columnNameValue.get(key) != null) ? columnNameValue.get(key) : oldValue);
      try {
         page.verifyValidRow(row);
      } catch (Exception e1) {
         row.clear();
         row.putAll(oldRow);
         throw new DBAppException(e1.getMessage());
      }

      table.indiciesUpdateRow(oldRow, row,
            new Address(table.TableName, row.get(table.getPrimaryColumn().name), page.number));

      try {
         table.unloadPage(page);
      } catch (Exception e2) {
         throw new DBAppException(e2.getMessage());
      }

      table.saveTable();

   }

   @Override
   public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
      Table table = Table.loadTable(tableName);
      updateConstraintsUsingMetadata(table);
      GridIndex searchIndex = table.searchingIndex(columnNameValue);

      Query indexdColumns = new Query();
      for (String colName : columnNameValue.keySet()) {
         if (isIndexedColumnName(searchIndex, colName)) {
            if (indexdColumns.size() != 0)
               indexdColumns.operators.add("AND");
            SQLTerm term = new SQLTerm();
            term._strTableName = tableName;
            term._strColumnName = colName;
            term._strOperator = "=";
            term._objValue = columnNameValue.get(colName);
            indexdColumns.terms.add(term);
         }
      }
      if (indexdColumns.size() != 0) {
         Vector<GridCell> matchingGridCells = searchIndex.evaluateIndexQuery(indexdColumns);
         for (GridCell gridCell : matchingGridCells) {
            for (String bucketPath : gridCell.bucketList) {
               Bucket bucket = gridCell.loadBucket(bucketPath);
               Vector<Address> toBeDeletedAddresses = new Vector<Address>();
               nextRow: for (Address address : bucket.addresses) {
                  Row row = address.grapRow(table);
                  System.out.println(row);
                  for (String colName : columnNameValue.keySet()) {
                     Comparable value = (Comparable) columnNameValue.get(colName);
                     if (value.compareTo(row.get(colName)) != 0)
                        continue nextRow;

                     toBeDeletedAddresses.add(address);
                     Page page = null;
                     try {
                        page = table.loadPage(address.pageNumber);
                     } catch (Exception e) {
                        e.printStackTrace();
                     }
                     page.Rows.remove(row);
                     table.savePage(page.number);
                  }
               }
               bucket.addresses.removeAll(toBeDeletedAddresses);
               gridCell.saveBucket(bucket, gridCell.formatBucketPath(bucket.bucketId));
            }

            return; // exit from the method
         }
      }
      // old delete Code
      String PrimColName = table.getPrimaryColumn().name;
      boolean PkSearch = false;
      int ColoumnCount = columnNameValue.size();
      int pageNumber = table.getPagesCount();
      boolean check = true;
      // iterate using enumeration object
      Enumeration<String> enumeration = columnNameValue.keys();
      while (enumeration.hasMoreElements()) {
         String key = enumeration.nextElement();
         if (PrimColName.equals(key)) {
            PkSearch = true;
         }
      }
      if (PkSearch) {
         // binary then loop other coloumns
         Page p = table.BinarySearchForPage(columnNameValue.get(PrimColName));
         if (p != null) {
            Integer n = table.BinarySearchForRow(p, columnNameValue.get(PrimColName));
            if (n != null) {
               // System.out.println(n);
               Row r = p.Rows.get(n);
               Enumeration<String> e = columnNameValue.keys();
               while (e.hasMoreElements()) {
                  String key = e.nextElement();
                  if (!r.get(key).equals(columnNameValue.get(key))) {
                     check = false;
                  }
               }
               if (check) {
                  table.indiciesRemoveRow(r, r.get(table.getPrimaryColumn().name));
                  p.Rows.remove((int) n);
                  p.updatePageMinMaxRemoveRow(r);
                  table.saveTable();
               }

            }
         }

      } else {
         // linear then loop other coloumns
         int pageCount = table.getPagesCount();
         boolean flag = true;
         Page p = null;
         for (int i = 0; i < pageCount; i++) {
            try {
               // System.out.println(t.PagesBuffer);
               p = table.loadPage(i);
            } catch (Exception e) {
               throw new DBAppException(e.getMessage());
            }

            int size = p.Rows.size();
            for (int n = 0; n < p.Rows.size(); n++) {
               flag = true;
               Row r = p.Rows.get(n);
               Enumeration<String> e = columnNameValue.keys();
               while (e.hasMoreElements()) {
                  String key = e.nextElement();
                  if (!r.get(key).equals(columnNameValue.get(key))) {
                     flag = false;
                  }
               }
               if (flag) {
                  p.Rows.remove(n);
                  table.indiciesRemoveRow(r, r.get(table.getPrimaryColumn().name));
                  p.updatePageMinMaxRemoveRow(r);
                  n--;
               }
            }
         }
      }

      table.saveTable();

   }

   @Override
   public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

      String TableName = sqlTerms[0]._strTableName;
      Table table = Table.loadTable(TableName);
      // if (sqlTerms.length == 1)
      // return linearQuery(sqlTerms[0]).iterator();

      Vector<Object> queryTerms = new Vector<Object>();
      Vector<String> queryOps = new Vector<String>();
      for (Object term : sqlTerms) {
         SQLTerm sqlTerm = (SQLTerm) term;
         if (!(sqlTerm._strTableName.equals(TableName)))
            throw new DBAppException("Selet from Table, can't handle multi Table queries");
         queryTerms.add(term);
      }
      for (String op : arrayOperators) {
         if (!("AND".contains(op) | "OR".contains(op) | "XOR".contains(op)))
            throw new DBAppException("Invalid operator");
         queryOps.add(op);
      }

      Query query = new Query(sqlTerms, arrayOperators);

      Vector<Row> resultSet = evaluateExp(query, table);

      return resultSet.iterator();
   }

   private Vector<Row> evaluateExp(Query inputQuery, Table t) {
      GridIndex index = t.searchingIndex(inputQuery);
      if (index != null && isQueryIndexed(inputQuery, index)) {
         return index.queryIndex(inputQuery);
      }

      if (inputQuery.operators.contains("XOR"))
         return evaluateEachSegmentOf(inputQuery, "XOR", t);

      else if (inputQuery.operators.contains("OR"))
         return evaluateEachSegmentOf(inputQuery, "OR", t);

      else if (inputQuery.operators.contains("AND"))
         return evaluateEachSegmentOf(inputQuery, "AND", t);

      else if (inputQuery.terms.size() == 1) {

         Vector<Row> result = new Vector<Row>();

         if (inputQuery.terms.get(0) instanceof SQLTerm) {
            SQLTerm term = (SQLTerm) inputQuery.terms.get(0);

            Query singleTermQuery = new Query();
            singleTermQuery.terms.add(term);

            if (isIndexedColumnName(index, term._strColumnName) && term._strOperator != "!=") {
               // Single I
               result = index.queryIndex(singleTermQuery);
            } else {
               // Single n
               result = linearQuery((SQLTerm) inputQuery.terms.get(0));
            }
            return result;
         } else {
            // It's the final Results
            result = (Vector<Row>) inputQuery.terms.get(0);
            return result;
         }

      } else {
         System.out.println("something went wrong while porccesing the query");
         System.exit(1);
      }

      return null;
   }

   private Vector<Row> evaluateEachSegmentOf(Query inputQuery, String operator, Table table) {
      int opIndex = inputQuery.operators.indexOf(operator);
      Query leftQuery = new Query();
      for (int i = 0; i <= opIndex; i++)
         leftQuery.terms.add(inputQuery.terms.get(i));
      for (int i = 0; i < opIndex; i++)
         leftQuery.operators.add(inputQuery.operators.get(i));
      // T1.T2,T3.T4
      Query rightQuery = new Query();
      for (int i = opIndex + 1; i < inputQuery.size(); i++)
         rightQuery.terms.add(inputQuery.terms.get(i));
      for (int i = opIndex + 1; i < inputQuery.operators.size(); i++)
         rightQuery.operators.add(inputQuery.operators.get(i));

      Vector<Row> leftResult = evaluateExp(leftQuery, table);
      Vector<Row> rightResult = evaluateExp(rightQuery, table);

      Vector<Row> resultQuery = performOperator(leftResult, rightResult, operator);

      return resultQuery;
   }

   private boolean isQueryIndexed(Query inputQuery, GridIndex index) {
      for (Object element : inputQuery.terms) {
         if (element instanceof SQLTerm) {
            SQLTerm term = (SQLTerm) element;
            if (!index.dimensions.containsKey(term._strColumnName))
               return false;
         }
      }
      return true;
   }

   private Vector<Row> performOperator(Vector<Row> leftVector, Vector<Row> rightVector, String op) {
      switch (op) {
         case "AND":
            Vector<Row> toBeRemoved = new Vector<Row>();
            for (Row element : rightVector)
               if (!leftVector.contains(element))
                  toBeRemoved.add(element);

            rightVector.removeAll(toBeRemoved);
            return rightVector;

         case "OR":
            for (Row row : rightVector)
               if (!leftVector.contains(row))
                  leftVector.add(row);
            return leftVector;

         case "XOR":
            Vector<Row> result = new Vector<Row>();
            for (Row intRangeValue : rightVector)
               if (!leftVector.contains(intRangeValue))
                  result.add(intRangeValue);
               else
                  leftVector.remove(intRangeValue);

            result.addAll(leftVector);
            return result;
      }

      System.out.println("Syntax Error something is wrong!!");
      System.exit(1);
      return null;
   }

   private Vector<Row> linearQuery(SQLTerm term) {
      String TableName = term._strTableName;
      String column = term._strColumnName;
      String op = term._strOperator;
      Comparable value = (Comparable) term._objValue;
      Table table = Table.loadTable(TableName);

      Vector<Row> result = new Vector<Row>();

      Enumeration pNumberEnum = table.PagesPath.keys();
      while (pNumberEnum.hasMoreElements()) {
         Page page = null;
         try {
            page = table.loadPage((int) pNumberEnum.nextElement());
         } catch (Exception e) {
            e.printStackTrace();
         }
         for (Row row : page.Rows) {
            Object comp = row.get(column);
            switch (op) {
               case "=":
                  if (value.compareTo(comp) == 0)
                     result.add(row);
                  break;
               case ">":
                  if (value.compareTo(comp) < 0)
                     result.add(row);
                  break;
               case "<":
                  if (value.compareTo(comp) > 0)
                     result.add(row);
                  break;
               case ">=":
                  if (value.compareTo(comp) <= 0)
                     result.add(row);
                  break;
               case "<=":
                  if (value.compareTo(comp) >= 0)
                     result.add(row);
                  break;
               case "!=":
                  if (value.compareTo(comp) != 0)
                     result.add(row);
                  break;
            }
         }
         try {
            table.unloadPage(page);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
      return result;
   }

   private Vector<Object> convertSQLTermToTerm(SQLTerm t) {
      Vector<Object> v = new Vector<>();
      v.add(t._strColumnName);
      v.add(t._strOperator);
      v.add(t._objValue);
      return v;
   }

   private Vector<Vector<SQLTerm>> parseIndexedAndBlocks(GridIndex index, Vector<Object> queryTerms,
         Vector<String> queryOps) {
      Vector<Vector<SQLTerm>> result = new Vector<Vector<SQLTerm>>();

      Vector<SQLTerm> sprintContAnds = new Vector<SQLTerm>();

      for (int i = 0; i < queryOps.size(); i++) {
         String op = queryOps.get(i);
         if (op.equals("AND")) {
            SQLTerm aTerm = (SQLTerm) queryTerms.get(i);
            SQLTerm bTerm = (SQLTerm) queryTerms.get(i + 1);
            if (isIndexedColumnName(index, aTerm._strColumnName) && isIndexedColumnName(index, bTerm._strColumnName)) {
               if (!sprintContAnds.contains(aTerm))
                  sprintContAnds.add(aTerm);
               if (!sprintContAnds.contains(bTerm))
                  sprintContAnds.add(bTerm);
            }
         } else {
            if (sprintContAnds.size() != 0) {
               result.add(sprintContAnds);
               sprintContAnds.clear();
            }
         }
      }
      if (sprintContAnds.size() != 0) {
         result.add(sprintContAnds);
      }
      return result;
   }

   private boolean isIndexedColumnName(GridIndex index, String columnName) {
      if (index == null)
         return false;
      for (Column col : index.columns) {
         if (col.name.equals(columnName))
            return true;
      }
      return false;
   }

   private void updateConstraintsUsingMetadata(Table t) {
      BufferedReader reader = null;
      Vector<String[]> lines = new Vector<>();
      String line = null;
      try {
         reader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

         int i = 0;
         while ((line = reader.readLine()) != null) {
            String[] l = line.split(",");

            if (l[0].equals(t.TableName)) {
               Column col = t.Columns.get(i);
               if (!col.name.equals(l[1])) {
                  col.name = l[1];
                  System.out.println(String.format("constraint change for col %s, with value= %s", col.name, l[1]));
               }
               if (!col.strType.equals(l[2])) {
                  col.strType = l[2];
                  System.out.println(String.format("constraint change for col %s, with value= %s", col.name, l[2]));
               }
               if (col.isPK != Boolean.parseBoolean(l[3])) {
                  col.isPK = Boolean.parseBoolean(l[3]);
                  System.out.println(String.format("constraint change for col %s, with value= %s", col.name, l[3]));
               }
               if (col.indexed != Boolean.parseBoolean(l[4])) {
                  col.indexed = Boolean.parseBoolean(l[4]);
                  System.out.println(String.format("constraint change for col %s, with value= %s", col.name, l[4]));
               }
               Object parsedValueMin = helper.parseStringButRespectType(l[5], col.strType);
               if (helper.compareObjects(col.MinValue, parsedValueMin) != 0) {
                  col.MinValue = (Comparable) parsedValueMin;
                  System.out.println(String.format("constraint change for col %s, with value= %s", col.name, l[5]));

               }
               Object parsedValueMax = helper.parseStringButRespectType(l[6], col.strType);
               if (helper.compareObjects(col.MaxValue, parsedValueMax) != 0) {
                  col.MaxValue = (Comparable) parsedValueMax;
                  System.out.println(String.format("constraint change for col %s, with value= %s", col.name, l[6]));
               }

               t.saveTable();
               i++;
            }

         }

      } catch (Exception e) {
         e.printStackTrace();
      }

   }

   private void updateMetaData(Table table) {
      try {
         // input the file content to the StringBuffer "input"
         BufferedReader metaFile = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
         StringBuffer contentBuffer = new StringBuffer();
         String line;

         while ((line = metaFile.readLine()) != null) {
            contentBuffer.append(line);
            contentBuffer.append('\n');
         }
         metaFile.close();
         String inputStr = contentBuffer.toString();

         // logic to replace lines in the string (could use regex here to be generic)
         inputStr = inputStr.replaceAll(table.TableName + ".+\n", "");

         // display the new file for debugging

         // write the new string with the replaced line OVER the same file
         FileOutputStream fileOut = new FileOutputStream("src/main/resources/metadata.csv");
         fileOut.write(inputStr.getBytes());
         fileOut.close();

         helper.addTableToMetaDataFile(table);

      } catch (IOException e) {
         System.out.println(e.getMessage());
         e.printStackTrace();
      }

   }

   public static void createTranscriptsTable(DBApp dbApp) throws Exception {
      // Double CK
      String tableName = "transcripts";

      Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
      htblColNameType.put("gpa", "java.lang.Double");
      htblColNameType.put("student_id", "java.lang.String");
      htblColNameType.put("course_name", "java.lang.String");
      htblColNameType.put("date_passed", "java.util.Date");

      Hashtable<String, String> minValues = new Hashtable<>();
      minValues.put("gpa", "0.7");
      minValues.put("student_id", "43-0000");
      minValues.put("course_name", "AAAAAA");
      minValues.put("date_passed", "1990-01-01");

      Hashtable<String, String> maxValues = new Hashtable<>();
      maxValues.put("gpa", "5.0");
      maxValues.put("student_id", "99-9999");
      maxValues.put("course_name", "zzzzzz");
      maxValues.put("date_passed", "2020-12-31");

      dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
   }

   public static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
      BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
      String record;
      Hashtable<String, Object> row = new Hashtable<>();
      int c = limit;
      if (limit == -1) {
         c = 1;
      }
      while ((record = transcriptsTable.readLine()) != null && c > 0) {
         String[] fields = record.split(",");

         row.put("gpa", Double.parseDouble(fields[0].trim()));
         row.put("student_id", fields[1].trim());
         row.put("course_name", fields[2].trim());

         String date = fields[3].trim();
         int year = Integer.parseInt(date.substring(0, 4));
         int month = Integer.parseInt(date.substring(5, 7));
         int day = Integer.parseInt(date.substring(8));

         Date dateUsed = new Date(year - 1900, month - 1, day);
         row.put("date_passed", dateUsed);

         dbApp.insertIntoTable("transcripts", row);
         row.clear();

         if (limit != -1) {
            c--;
         }
      }

      transcriptsTable.close();
   }

   public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
      net.sf.jsqlparser.statement.Statement statement = null;
      try {
         statement = CCJSqlParserUtil.parse(strbufSQL.toString());
      } catch (JSQLParserException e) {
         throw new DBAppException("SQL Syntax Exception " + e.getMessage());
      }
      if (statement instanceof Insert) {
         // String tableName, Hashtable<String, Object> colNameValue
         Insert insert = (Insert) statement;
         String tableName = insert.getTable().toString();
         Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
         List<Expression> valuesExpressionList = ((ExpressionList) insert.getItemsList()).getExpressions();

         for (int i = 0; i < insert.getColumns().size(); i++)
            colNameValue.put(insert.getColumns().get(i).getColumnName(), evalExpr(valuesExpressionList.get(i)));

         this.insertIntoTable(tableName, colNameValue);
      } else if (statement instanceof Update) {
         // updateTable(String tableName, String clusteringKeyValue, Hashtable<String,
         // Object> columnNameValue)
         Update update = (Update) statement;
         String tableName = update.getTable().toString();
         List<Expression> valuesExpressionList = update.getExpressions();

         Hashtable<String, Object> columnNameValue = new Hashtable<String, Object>();

         for (int i = 0; i < update.getColumns().size(); i++)
            columnNameValue.put(update.getColumns().get(i).getColumnName(), evalExpr(valuesExpressionList.get(i)));

         String clusteringKeyValue = String.valueOf(evalExpr(((EqualsTo) update.getWhere()).getRightExpression()));

         this.updateTable(tableName, clusteringKeyValue, columnNameValue);
      } else if (statement instanceof Delete) {
         // deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue)
         Delete delete = (Delete) statement;
         Hashtable<String, Object> columnNameValue = new Hashtable<String, Object>();
         String tableName = delete.getTable().toString();
         Query whereSt = parseExpression(delete.getWhere());
         // id=2 and name=ahmed
         // gpa>41 and name=roy
         for (String compOp : whereSt.operators)
            if (!compOp.equals("AND"))
               throw new DBAppException("SQL Syntax Exception, Delete can't contain other than ANDed statements");

         for (Object item : whereSt.terms) {
            EqualsTo expr = (EqualsTo) item;
            columnNameValue.put((String) evalExpr(expr.getLeftExpression()), evalExpr(expr.getRightExpression()));
         }

         this.deleteFromTable(tableName, columnNameValue);
      } else if (statement instanceof Select) {
         // public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators)
         // throws DBAppException {

         Select select = (Select) statement;
         Hashtable<String, Object> columnNameValue = new Hashtable<String, Object>();
         String tableName = ((PlainSelect) select.getSelectBody()).getFromItem().toString();
         Query whereSt = parseExpression(((PlainSelect) select.getSelectBody()).getWhere());
         String[] arrayOperators;
         SQLTerm[] sqlTerms;
         Vector<SQLTerm> convertedTerms = new Vector<SQLTerm>();

         arrayOperators = whereSt.operators.toArray(new String[whereSt.operators.size()]);
         // col1 = 10 AND col2 > 20 AND col3 != 30

         for (Object item : whereSt.terms) {
            BinaryExpression expr = (BinaryExpression) item;
            expr.getStringExpression(); // = != > <=
            SQLTerm sqlTerm = new SQLTerm();
            sqlTerm._strTableName = tableName;
            sqlTerm._strColumnName = (String) evalExpr(expr.getLeftExpression());
            sqlTerm._strOperator = expr.getStringExpression();
            sqlTerm._objValue = evalExpr(expr.getRightExpression());
            convertedTerms.add(sqlTerm);
         }

         sqlTerms = convertedTerms.toArray(new SQLTerm[whereSt.terms.size()]);

         return this.selectFromTable(sqlTerms, arrayOperators);
      } else if (statement instanceof CreateTable) {
         // public void createTable(String tableName, String clusteringKey,
         // Hashtable<String, String> colNameType,
         // Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax)
         // throws DBAppException {

         CreateTable cTable = (CreateTable) statement;
         String tableName = cTable.getTable().getName();
         String clusteringKey = cTable.getIndexes().get(0).getColumns().get(0).columnName;
         Hashtable<String, String> colNameType = new Hashtable<String, String>();
         Hashtable<String, String> colNameMin = new Hashtable<String, String>();
         Hashtable<String, String> colNameMax = new Hashtable<String, String>();

         List<ColumnDefinition> columns = cTable.getColumnDefinitions();
         for (ColumnDefinition col : columns) {
            String columnName = col.getColumnName();
            String type = "";
            String min = null;
            String max = null;
            switch (col.getColDataType().getDataType().toLowerCase()) {
               case "varchar":
                  type = "java.lang.String";
                  min = "AAAAAA";
                  max = "zzzzzz";
                  break;
               case "int":
                  type = "java.lang.Integer";
                  min = "0";
                  max = "1000000";
                  break;
               case "double":
                  type = "java.lang.Double";
                  min = "0.0";
                  max = "1000000.0";
                  break;
               case "float":
                  type = "java.lang.Double";
                  min = "0.0";
                  max = "1000000.0";
                  break;
               case "decimal":
                  type = "java.lang.Double";
                  min = "0.0";
                  max = "1000000.0";
                  break;
               case "date":
                  type = "java.util.Date";
                  min = "1990-01-01";
                  max = "2030-01-01";
                  break;
               case "datetime":
                  type = "java.util.Date";
                  min = "1990-01-01";
                  max = "2030-01-01";
                  break;
               default:
                  throw new DBAppException("Un Supported Data Type");
            }

            colNameType.put(columnName, type);
            colNameMin.put(columnName, min);
            colNameMax.put(columnName, max);
         }
         // varchar ==> string
         // INT ==> Integer
         // DOUBLE, FLOAT, DECIMAL ==> double
         // DATE, DATETIME ==> date

         this.createTable(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

      } else if (statement instanceof CreateIndex) {
         // public void createIndex(String tableName, String[] columnNames) throws

         CreateIndex cIndex = (CreateIndex) statement;
         String tableName = cIndex.getTable().getName();
         Vector<String> colNames = new Vector<String>();

         for (ColumnParams col : cIndex.getIndex().getColumns())
            colNames.add(col.getColumnName());

         String[] columnNames = colNames.toArray(new String[colNames.size()]);

         this.createIndex(tableName, columnNames);
      } else {
         System.err.println("SQL Syntax Exception");
         System.exit(1);
      }

      return null;
   }

   private static Query parseExpression(Expression expr) {
      // String whereClause = "z!=3 AND a=3 OR b>4";

      if (expr instanceof ComparisonOperator) {
         Query result = new Query();
         result.terms.add(expr);
         return result;
      }
      BinaryExpression bexpr = (BinaryExpression) expr;
      Query leftQuery = parseExpression(bexpr.getLeftExpression());
      Query rightQuery = parseExpression(bexpr.getRightExpression());

      String compType = "";
      if (bexpr instanceof AndExpression) {
         compType = "AND";
      }
      if (bexpr instanceof OrExpression) {
         compType = "OR";
      }

      leftQuery.concatQuery(rightQuery, compType);
      return leftQuery;
   }

   private Object evalExpr(Expression expr) {
      if (expr instanceof StringValue)
         return ((StringValue) expr).getValue();
      else if (expr instanceof DateValue)
         return ((DateValue) expr).getValue();
      else if (expr instanceof DoubleValue)
         return ((DoubleValue) expr).getValue();
      else if (expr instanceof LongValue)
         return ((LongValue) expr).getValue();
      else if (expr instanceof net.sf.jsqlparser.schema.Column)
         return ((net.sf.jsqlparser.schema.Column) expr).getColumnName();
      return null;
   }

   public static void main(String[] args) throws Exception {
      DBApp db = new DBApp();
      db.init();

      // helper.delFile("src/main/resources/metadata.csv");
      // helper.deleteDirectoryFiles(new File("src/main/resources/data/"));

      // createTranscriptsTable(db);
      // long start = System.currentTimeMillis();
      // insertTranscriptsRecords(db, 7);
      // db.createIndex("transcripts", new String[] { "student_id", "gpa" });
      // db.createIndex("transcripts", new String[] { "student_id", "gpa",
      // "date_passed" });

      // insertTranscriptsRecords(db, 500);

      // long end = System.currentTimeMillis();
      // long elapsedTime = end - start;

      Table t = Table.loadTable("courses");
      System.out.println(t);
      // for (Column column : t.Columns) {
      //    column.isPK = true;
      //    column.indexed = true;
      // }
      // db.updateMetaData(t);

      // System.out.println(elapsedTime / 1000 + " Sec");

      // System.out.println(t);

      SQLTerm[] arrSQLTerms;
      arrSQLTerms = new SQLTerm[1];
      arrSQLTerms[0] = new SQLTerm();
      arrSQLTerms[0]._strTableName = "transcripts";
      arrSQLTerms[0]._strColumnName = "gpa";
      arrSQLTerms[0]._strOperator = "!=";
      arrSQLTerms[0]._objValue = 3.8925;

      // arrSQLTerms[1] = new SQLTerm();
      // arrSQLTerms[1]._strTableName = "transcripts";
      // arrSQLTerms[1]._strColumnName = "date_passed";
      // arrSQLTerms[1]._strOperator = ">";
      // arrSQLTerms[1]._objValue = new Date(1999 - 1900, 10 - 1, 11);

      // arrSQLTerms[2] = new SQLTerm();
      // arrSQLTerms[2]._strTableName = "transcripts";
      // arrSQLTerms[2]._strColumnName = "gpa";
      // arrSQLTerms[2]._strOperator = "=";
      // arrSQLTerms[2]._objValue = 1.8344;

      // arrSQLTerms[3] = new SQLTerm();
      // arrSQLTerms[3]._strTableName = "transcripts";
      // arrSQLTerms[3]._strColumnName = "student_id";
      // arrSQLTerms[3]._strOperator = "=";
      // arrSQLTerms[3]._objValue = "70-6806";

      // arrSQLTerms[4] = new SQLTerm();
      // arrSQLTerms[4]._strTableName = "transcripts";
      // arrSQLTerms[4]._strColumnName = "course_name";
      // arrSQLTerms[4]._strOperator = "=";
      // arrSQLTerms[4]._objValue = "KohMCm";

      // arrSQLTerms[5] = new SQLTerm();
      // arrSQLTerms[5]._strTableName = "transcripts";
      // arrSQLTerms[5]._strColumnName = "student_id";
      // arrSQLTerms[5]._strOperator = "=";
      // arrSQLTerms[5]._objValue = "63-4484";

      // String[] strarrOperators = new String[0];
      // strarrOperators[0] = "AND";
      // strarrOperators[1] = "OR";
      // strarrOperators[2] = "AND";
      // strarrOperators[3] = "XOR";
      // strarrOperators[4] = "AND";

      // gpa>3.8 AND data> 1999,10,11 OR gpa = 1.8344 AND ID = 70-6806
      // XOR (course_name ="KohMCm" AND ID = 63-4484)

      // select * from Student where name = “John Noor” or gpa = 1.5;

      // Iterator resultSet = db.selectFromTable(arrSQLTerms, strarrOperators);

      // Hashtable<String, Object> ht = new Hashtable<String, Object>();
      // ht.put("gpa", 3.8925);
      // db.deleteFromTable("transcripts", ht);

      // System.out.println(t);
      // Iterator resultSet = db.selectFromTable(arrSQLTerms, strarrOperators);
      // StringBuffer sb = new StringBuffer();
      // sb.append("SELECT * FROM transcripts WHERE gpa>4.0 and
      // student_id>'45-0000';");
      // sb.append("INSERT INTO transcripts(gpa,student_id) VALUES (4.5,'63-4499')");
      // sb.append("UPDATE transcripts SET student_id='99-9999' WHERE gpa=3.1964");
      // sb.append("DELETE FROM transcripts WHERE gpa=3.1964 AND course_name='iQrcnv'");
      // sb.append("CREATE INDEX idx ON transcripts (gpa,student_id)");
      // sb.append("CREATE TABLE sami (gpa decimal(5,2),student_id varchar(25),PRIMARY KEY (gpa))");
      // Iterator iter = db.parseSQL(sb);
      // db.parseSQL((new StringBuffer()).append("INSERT INTO sami(gpa,student_id)
      // VALUES (4.5,'aaaaaa')"));

      // Table newTable = Table.loadTable("sami");
      // Table newTable = Table.loadTable("transcripts");
      // System.out.println(newTable);
      // System.out.println(newTable.indices.size());

      // if(iter != null)
      // while (iter.hasNext()) {
      // System.out.println(iter.next());
      // }
   }
}
