import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;

import java.io.IOException;
import java.util.*;

public class LineageUtils implements NodeProcessor {

    // 存放输入表
    TreeSet<String> inputTableList = new TreeSet<String>();

    // 存放目标表
    TreeSet<String> outputTableList = new TreeSet<String>();

    //存放with子句中的别名, 最终的输入表是 inputTableList减去withTableList
    TreeSet<String> withTableList = new TreeSet<String>();

    public TreeSet getInputTableList() {
        return inputTableList;
    }

    public TreeSet getOutputTableList() {
        return outputTableList;
    }

    public TreeSet getWithTableList() {
        return withTableList;
    }

    @Override
    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;
        System.out.println(pt.token);
        switch (pt.getToken().getType()) {

            //create语句
            case HiveParser.TOK_CREATETABLE: {
                String createName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                outputTableList.add(createName);
                break;
            }

            //insert语句
            case HiveParser.TOK_TAB: {
                // System.out.println(pt.getChildCount() + "tab");
                String insertName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                outputTableList.add(insertName);
                //  System.out.println("insertName  " + insertName);
                break;
            }

            //from语句
            case HiveParser.TOK_TABREF: {
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String fromName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                inputTableList.add(fromName);
                break;
            }

            // with.....语句
            case HiveParser.TOK_CTE: {
                for (int i = 0; i < pt.getChildCount(); i++) {
                    ASTNode temp = (ASTNode) pt.getChild(i);
                    String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    withTableList.add(cteName);
                }
                break;
            }
        }
        return null;
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {

        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        inputTableList.clear();
        outputTableList.clear();
        withTableList.clear();

        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        ArrayList topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }


    //进行测试，sql语句是瞎写的，但是语法是对的
    public static void main(String[] args) throws IOException, ParseException, SemanticException {
        //String query = "insert into qc.tables_lins_cnt partition(dt='2016-09-15') select a.x from (select x from cc group by x) a left  join yy b on a.id = b.id left join (select * from zz where id=1) c on c.id=b.id";
        //String query ="from (select id,name from xx where id=1) a insert overwrite table  dsl.dwm_all_als_active_d partition (dt='main') select id group by id insert overwrite table  dsl.dwm_all_als_active_d2 partition (dt='main') select name group by name";
        String query="insert into c select * from a left join cx_dwd_safe.b on a.id=b.id i" ;
        LineageUtils lep = new LineageUtils();
        lep.getLineageInfo(query);
        System.out.println("Input tables = " + lep.getInputTableList());
        System.out.println("Output tables = " + lep.getOutputTableList());
        System.out.println("with tables = " + lep.getWithTableList());
    }
}

