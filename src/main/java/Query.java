import java.util.Vector;

/**
 * query
 */
public class Query {
    Vector<Object> terms; // rowvector or sqlterm
    Vector<String> operators;
    int iteratorPointer = 0;
    boolean hasMoreNext = true;

    public Query() {
        terms = new Vector<Object>();
        operators = new Vector<String>();
    }

    public static Query fromSQLTerm(SQLTerm term) {
        Query newQuery = new Query();
        newQuery.terms.add(term);
        return newQuery;
    }

    public void addUniqueTerm(Object term) {
        if (!terms.contains(term))
            terms.add(term);
    }

    public void fillOperatorsOfSize(int size, String operator) {
        for (int i = 0; i < size; i++) {
            operators.add(operator);
        }
    }

    public Query(SQLTerm[] sqlTerms, String[] arrayOperators) {
        terms = new Vector<>();
        operators = new Vector<>();
        for (SQLTerm term : sqlTerms)
            terms.add(term);

        for (String op : arrayOperators)
            operators.add(op);

    }
    


    public int size() {
        return terms.size();
    }

    public void resetNext() {
        hasMoreNext = true;
        iteratorPointer = 0;
    }

    public Object getNext() {
        if (iteratorPointer == terms.size() - 1)
            hasMoreNext = false;
        return (terms.get(iteratorPointer++));
    }

    public int indexOf(Object element) {
        return terms.indexOf(element);
    }

    public Object getTerm(int index) {
        return terms.get(index);
    }

    public String getOperator(int index) {
        return operators.get(index);
    }

    public void concatQuery(Query inputQuery, String opInBetween) {
        for (Object term : inputQuery.terms)
            this.terms.add(term);
        this.operators.add(opInBetween);
        for (String op : inputQuery.operators)
            this.operators.add(op);
    }

    public Query replaceRange(int startIndex, int endIndex, Object replacemeent) {
        try {

            for (int i = startIndex; i <= endIndex; i++) {
                terms.remove(startIndex);
            }

            for (int i = startIndex; i < endIndex; i++) {
                operators.remove(startIndex);
            }

        } catch (Exception e) {

        }

        terms.add(startIndex, replacemeent);

        iteratorPointer = startIndex + 1;

        return this;
    }

    public static Query subQuery(Query inputQuery, int startIndex, int endIndex) {
        Query newQuery = new Query();
        for (int i = startIndex; i <= endIndex; i++)
            newQuery.terms.add(inputQuery.terms.get(i));

        for (int i = startIndex; i < endIndex; i++)
            newQuery.operators.add(inputQuery.operators.get(i));

        return newQuery;
    }

    public void removeFirstOperators(int i) {
        for (int j = 0; j < i; j++) {
            operators.remove(0);
        }
    }

    // public static void main(String[] args) {
    // Vector<Integer> v = new Vector<Integer>();
    // v.add(3);
    // v.add(5);
    // v.add(4);

    // Iterator i = v.iterator();
    // Iterable ic = i;
    // while (i.hasNext()) {
    // System.out.println(i.next());
    // }

    // while (i.hasNext()) {
    // System.out.println(i.next());
    // }
    // }

}