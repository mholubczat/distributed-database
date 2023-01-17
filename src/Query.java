import java.util.ArrayList;

public class Query {
    private boolean open = true;
    private String result;
    private Integer resultInt;
    private final ArrayList<String> involved = new ArrayList<>();

    public Query(String involved) {
        this.involved.add(involved);
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public synchronized void close() {
        this.open = false;
        this.notifyAll();
    }

    public boolean isOpen() {
        return open;
    }

    public void addInvolved(String involved){
        this.involved.add(involved);
    }
    public void removeInvolved(String involved){
        this.involved.remove(involved);
    }
    public ArrayList<String> getInvolved() {
        return involved;
    }

    public void setResultInt(int resultInt) {
        this.resultInt = resultInt;
    }

    public Integer getResultInt() {
        return this.resultInt;
    }
}
