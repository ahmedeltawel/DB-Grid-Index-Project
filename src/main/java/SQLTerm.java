
/**
    arrSQLTerms = new SQLTerm[2]; 
    arrSQLTerms[0]._strTableName = "Student"; 
    arrSQLTerms[0]._strColumnName= "name"; 
    arrSQLTerms[0]._strOperator  = "="; 
    arrSQLTerms[0]._objValue     = "John Noor"; 
    SELECT * FROM Student WHERE name = john noor
*/
public class SQLTerm {

    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

}
