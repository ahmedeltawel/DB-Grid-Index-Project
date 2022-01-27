

import java.util.Hashtable;
import java.util.function.BiFunction;

public class Row extends Hashtable<String, Object> {
    PageActionListener actionListener;
    
    
    public void setActionListener(PageActionListener listener) {
        this.actionListener = listener;
    }
    

    @Override
    public synchronized void replaceAll(BiFunction<? super String, ? super Object, ? extends Object>  function) {
        // the super hashtable Applies the replace
        
        // Row oldRow = (Row) this.clone();
        super.replaceAll(function);

        // try {
        //     actionListener.verifyValidRow(this);
        // } catch (Exception e) {

        //     this.clear();
        //     this.putAll(oldRow);

        //     System.out.println(e.getMessage());
        //     e.printStackTrace();
        // }
        
    }






    
    

}
