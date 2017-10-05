import java.util.ArrayList;
import java.util.List;

/**
 * Created by Gundam on 2017/10/5.
 */
public class TNode implements Comparable<TNode>{
    private String itemName;
    private int count;
    private TNode parent;
    private List<TNode> children;
    private TNode next;
    public TNode()
    {
        this.children = new ArrayList<TNode>();
    }
    public TNode(String name){
        this.itemName = name;
        this.count = 1;
        this.children = new ArrayList<TNode>();
    }

    public TNode(String name,int count){
        this.itemName = name;
        this.count = count;
        this.children = new ArrayList<TNode>();
    }

    public TNode findChildren(String childName){
        for(TNode node:this.getChildren()){
            if(node.getItemName().equals(childName)){
                return node;
            }
        }
        return null;
    }

    public TNode getNext()
    {
        return next;
    }
    public TNode getParent()
    {
        return parent;
    }
    public void setNext(TNode next)
    {
        this.next = next;
    }
    public void increaseCount(int num)
    {
        count += num;
    }
    public int getCount()
    {
        return count;
    }
    public String getItemName()
    {
        return itemName;
    }
    public List<TNode> getChildren()
    {
        return children;
    }
    public void setParent(TNode parent)
    {
        this.parent = parent;
    }

    @Override
    public int compareTo(TNode o)
    {
        return o.getCount() - this.getCount();
    }
}
