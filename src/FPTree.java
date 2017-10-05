import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;


public class FPTree {
    public static int SUPPORT_DEGREE = 4;
    public static String SEPARATOR = " ";

    public static void main(String[] args){
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        args = new String[]{"hdfs://master:9000/data/input/wordcounts.txt", "hdfs://master:9000/data/output"};
        if(args.length != 2){
            System.err.println("USage:<Datapath> <Output>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("frequence parttern growth").setMaster("local[4]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(args[0],1).map(
                arg->arg.substring(arg.indexOf(" ")+1).trim()
        );
        JavaPairRDD<String, Integer> transactions = constructTransactions(lines);
        FPGrowth(transactions, null, ctx);
        ctx.close();
    }

    private static JavaPairRDD<String,Integer> constructTransactions(JavaRDD<String> lines){
        return lines.mapToPair(x-> new Tuple2<>(x, 1)).reduceByKey((x, y)->x+y);
    }

    public static void FPGrowth(JavaPairRDD<String, Integer> transactions, final List<String> postPattern, JavaSparkContext ctx){
        JavaRDD<TNode> headTable = bulidHeadTable(transactions);
        List<TNode> headlist = headTable.collect();
        TNode tree = bulidFPTree(headlist, transactions);
        if(tree.getChildren() == null || tree.getChildren().size() == 0){
            return;
        }
        if(postPattern!=null){
            headTable.foreach(head->{
                System.out.print(head.getCount() + " " + head.getItemName());
                for (String item : postPattern)
                {
                    System.out.print(" " + item);
                }
                System.out.println();
            });
        }

        headTable.foreach(head->{
            List<String> newPostPattern = new ArrayList<String>();
            newPostPattern.add(head.getItemName());
            if (postPattern != null)
            {
                newPostPattern.addAll(postPattern);
            }
            List<String> newTransactionsList = new ArrayList<String>();
            TNode nextNode = head.getNext();
            while(nextNode != null){
                int count = nextNode.getCount();
                TNode parent = nextNode.getParent();
                StringBuilder tlines = new StringBuilder();
                while(parent.getItemName()!= null){
                    tlines.append(parent.getItemName()).append(" ");
                    parent = parent.getParent();
                }
                while((count--) > 0)
                {
                    newTransactionsList.add(tlines.toString());  //添加模式基的前缀 ，因此最终的频繁项为:  parentNodes -> newPostPattern
                }
                nextNode = nextNode.getNext();
                JavaPairRDD<String, Integer> newTransactions = constructTransactions(ctx.parallelize(newTransactionsList));
                FPGrowth(newTransactions, newPostPattern, ctx);
            }
        });
    }

    public static JavaRDD<TNode> bulidHeadTable(JavaPairRDD<String, Integer> transactions) {
        return transactions.flatMapToPair((PairFlatMapFunction<Tuple2<String, Integer>, String, Integer>) arg0 -> {
            List<Tuple2<String, Integer>> t2list = new ArrayList<>();
            String[] items = arg0._1.split(SEPARATOR);
            int count = arg0._2;
            for (String item : items) {
                t2list.add(new Tuple2<>(item, count));
            }
            return t2list.iterator();
        }).reduceByKey((x, y)->x+y).mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) t -> new Tuple2<>(t._2, t._1))
                //filter out items which satisfies the minimum support_degree.
                .filter((Function<Tuple2<Integer, String>, Boolean>) v1 -> v1._1 >= SUPPORT_DEGREE)
                //sort items in descent.
                .sortByKey(false)
                //convert transactions to TNode.
                .map((Function<Tuple2<Integer, String>, TNode>) v1 -> new TNode(v1._2, v1._1));
    }

    public static TNode bulidFPTree(List<TNode> headTable,JavaPairRDD<String,Integer> transactions){
        final TNode rootNode = new TNode();
        final List<TNode> headItems = headTable;
        JavaPairRDD<LinkedList<String>,Integer> transactionsDesc = transactions.mapToPair((PairFunction<Tuple2<String,Integer>,LinkedList<String>,Integer>) t ->{
            LinkedList<String> descItems = new LinkedList<>();
            List<String> items = Arrays.asList(t._1.split(SEPARATOR));
            for(TNode node:headItems){
                String headName = node.getItemName();
                if(items.contains(headName))
                {
                    descItems.add(headName);
                }
            }
            return new Tuple2<>(descItems, t._2);
        }).filter((Function<Tuple2<LinkedList<String>,Integer>,Boolean>) v -> v._1.size()>0);

        transactionsDesc.foreach(t ->{
            LinkedList<String> itemsDesc = t._1;
            int count = t._2;
            TNode subtreeRoot = rootNode;
            if(subtreeRoot.getChildren().size()!=0){
                TNode tempNode =subtreeRoot.findChildren(itemsDesc.peek());
                while(!itemsDesc.isEmpty()&&tempNode!=null){
                    tempNode.increaseCount(count);
                    subtreeRoot = tempNode;
                    itemsDesc.poll();
                    tempNode = subtreeRoot.findChildren(itemsDesc.peek());
                }
            }
            addSubTree(headItems, subtreeRoot, itemsDesc, count);
        });
        return rootNode;
    }

    public static void addSubTree(List<TNode> headItems,TNode subtreeRoot,LinkedList<String> itemsDesc, int count){
        if(itemsDesc.size()>0){
            final TNode thisNode = new TNode(itemsDesc.pop(),count);
            subtreeRoot.getChildren().add(thisNode);
            thisNode.setParent(subtreeRoot);
            for(TNode t : headItems)
            {
                if(t.getItemName().equals(thisNode.getItemName()))
                {
                    TNode lastNode = t;
                    while(lastNode.getNext() != null)
                    {
                        lastNode = lastNode.getNext();
                    }
                    lastNode.setNext(thisNode);
                }
            }
            subtreeRoot = thisNode;//update thisNode as the current subtreeRoot
            //add the left items in itemsDesc recursively
            addSubTree(headItems, subtreeRoot, itemsDesc, count);
        }
    }
}
