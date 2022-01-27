import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import net.sf.jsqlparser.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
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
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.schema.Column;
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

public class test implements Serializable {
    public static void main(String[] args) throws JSQLParserException {

        // http://jsqlparser.sourceforge.net/example.php

        CCJSqlParserManager pm = new CCJSqlParserManager();

        String sql = "SELECT * FROM MY_TABLE1, MY_TABLE2, (SELECT * FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 "
                + " WHERE ID = (SELECT MAX(ID) FROM AHMED) AND ID2 IN (SELECT * FROM KHALIED)";
        net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(sql));
        /*
         * now you should use a class that implements StatementVisitor to decide what to
         * do based on the kind of the statement, that is SELECT or INSERT etc. but here
         * we are only interested in SELECTS
         */

        Select stmt = (Select) CCJSqlParserUtil
                .parse("SELECT col1 AS a, col2 AS b, col3 AS c FROM table WHERE col1 = 10 AND col2 = 20 AND col3 = 30");

        // Map<String, Expression> map = new HashMap<>();
        // for (SelectItem selectItem :
        // ((PlainSelect)stmt.getSelectBody()).getSelectItems()) {
        // selectItem.accept(new SelectItemVisitorAdapter() {
        // @Override
        // public void visit(SelectExpressionItem item) {
        // map.put(item.getAlias().getName(), item.getExpression());
        // }
        // });
        // }

        // System.out.println("map " + map);

        // Insert insert = (Insert)CCJSqlParserUtil.parse("insert into mytable (col1,col2) values (1,2)");
        // // System.out.println(insert.toString());
        // System.out.println(insert.getTable().toString());
        // System.out.println(insert.getColumns());
        // System.out.println(insert.getItemsList());
        // List<Expression> valuesExpressionList = ((ExpressionList)insert.getItemsList()).getExpressions();
        

        Update update = (Update)CCJSqlParserUtil.parse("update mytable set gpa=44,firstName=13 where id=2;");
        // Update update = (Update)CCJSqlParserUtil.parse("update mytable set gpa=44,firstName=13 where name=khalid;");
        // System.out.println(insert.toString());
        System.out.println(update.getTable());
        System.out.println(update.getColumns());
        System.out.println(update.getExpressions());
        System.out.println(update.getWhere().getClass().getName());
        System.out.println(update.getWhere());
        System.out.println(((EqualsTo)update.getWhere()).getRightExpression());
        System.out.println(((EqualsTo)update.getWhere()).getLeftExpression());
        
        // System.out.println(((BinaryExpression)update.getWhere()).getRightExpression());
        // System.out.println(((StringValue)update.getWhere()).getValue());

        // String whereClause = "a=3 OR b=4 AND c=5 OR d>5 AND x<10";
        // String whereClause = "z!=3 AND a=3 OR b>4";
        // String whereClause = "a=3 OR b>4";
        // String whereClause = "a=3";
        // Expression expr = CCJSqlParserUtil.parseCondExpression(whereClause);
        
        // Query result = parseExpression(expr);

        
        // Delete delete = (Delete)CCJSqlParserUtil.parse("delete from mytable where id=2 and name=ahmed");
        // // System.out.println(insert.toString());
        // System.out.println(delete.getTable());
        // System.out.println(delete.getWhere());

        // Select select = (Select)CCJSqlParserUtil.parse("SELECT * FROM mytable WHERE col1 = 10 AND col2 = 20 AND col3 = 30");
        // // System.out.println(select.getSelectBody());
        // System.out.println(((PlainSelect)select.getSelectBody()).getFromItem());
        // System.out.println(((PlainSelect)select.getSelectBody()).getWhere());

        // CreateTable cTable = (CreateTable)CCJSqlParserUtil.parse("create table mytable(name double(50,3), gpa INT, PRIMARY KEY (gpa) );");
        // // System.out.println(insert.toString());
        // System.out.println(cTable.getTable());
        // System.out.println(cTable.getColumnDefinitions());
        // System.out.println(cTable.getColumnDefinitions().get(0).getColumnName());
        // System.out.println(cTable.getColumnDefinitions().get(0).getColDataType());
        // System.out.println(cTable.getColumnDefinitions().get(0).getColDataType().getDataType());
        // System.out.println(cTable.getIndexes().get(0).getColumns().get(0).columnName);
        // varchar ==> string
        // INT ==> Integer
        // DOUBLE, FLOAT, DECIMAL ==> double
        // DATE, DATETIME ==> date
        
        // System.out.println(cTable.getIndexes());

        CreateIndex cIndex = (CreateIndex)CCJSqlParserUtil.parse("CREATE INDEX idx ON myTable (name, gpa);");
        // System.out.println(insert.toString());
        System.out.println(cIndex.getTable());
        System.out.println(cIndex.getIndex().getName());
        System.out.println(cIndex.getIndex().getColumns().get(0).columnName);


    // private static Query parseExpression(Expression expr) {

    //     expr = (BinaryExpression) expr;

    //     //
    //     return null;
    // }
    }


    private Object typeCasetExpression(Expression expr) {
        if(expr instanceof StringValue){
            return ((StringValue)expr).getValue();
        }else
        if(expr instanceof StringValue){
            return ((DateValue) expr).getValue();
        }else
        if(expr instanceof StringValue){
            return ((DoubleValue) expr).getValue();
        }else
        if(expr instanceof StringValue){
            return ((LongValue) expr).getValue();
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
        // if (bexpr instanceof XOrExpression) {
        //     compType = "OR";
        // }

        leftQuery.concatQuery(rightQuery, compType);
        return leftQuery;
    }

}
 // ExpressionVisitorAdapter expressionParser = new ExpressionVisitorAdapter() {
        // // Vector list = new Vector();
        // Query query = new Query();

        // @Override
        // protected void visitBinaryExpression(BinaryExpression expr) {
        // System.out.println(expr.toString() + "::" + expr.getClass().getName());

        // if (expr instanceof ComparisonOperator) { //BASE case single item
        // // System.out.println("left=" + expr.getLeftExpression() + " op=" +
        // expr.getStringExpression() + " right=" + expr.getRightExpression());
        // // System.out.println( expr.getLeftExpression().getClass().getName() );
        // // System.out.println( expr.getRightExpression().getClass().getName() );
        // // System.out.println(expr.getStringExpression());
        // System.out.println("ee");
        // }

        // if(expr instanceof OrExpression){

        // }
        // if(expr instanceof AndExpression){

        // }

        // super.visitBinaryExpression(expr);

        // }
        // };

        // expr.accept(expressionParser);

        // ExpressionVisitorAdapter adapter = new ExpressionVisitorAdapter(expr);
        // adapter.visit(expr);

    