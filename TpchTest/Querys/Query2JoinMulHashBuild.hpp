//
// Created by anonymous on 6/27/24.
//

#ifndef OLVP_QUERY2JOINMULHASHBUILD_HPP
#define OLVP_QUERY2JOINMULHASHBUILD_HPP




#include "../../Query/RegQuery.h"

/*
 * select count(*) from lineitem,orders where lineitem.l_orderkey=orders.o_orderkey;
 *
 * */


class Query2JoinMulHashBuild:public RegQuery
{

public:
    Query2JoinMulHashBuild(){

    }
    string getSql()  {return TpchSqls::Q2J();}
    PlanNode* getPlanTree()
    {

        LookupJoinNode *join = createLineitemJoinOrders();

        PartialAggregationNode *pagg = createPartialAgg();
        pagg->addSource(join);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(pagg);


        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));

        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,pagg);


        FinalAggregationNode *fagg = createFinalAgg();
        fagg->addSource(exchange);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(fagg);
        return (PlanNode*)output;
    }



    LookupJoinNode *createLineitemJoinOrders()
    {


        PlanNode *tableScanLineitem = this->createLineitemTableScan();
        PlanNode *tableScanOrders = this->createOrdersTableScan();


        vector<FieldDesc> lineitemProbeSchema = {FieldDesc("l_orderkey","int64")};


        vector<FieldDesc> ordersBuildSchema = {FieldDesc("o_orderkey","int64")};

        vector<FieldDesc> ordersBuildOutputSchema = {FieldDesc("o_orderkey","int64")};


        vector<int> lineitemprobeOutputChannels = {0};
        vector<int> lineitemprobeHashChannels = {0};
        vector<int> ordersbuildOutputChannels = {0};
        vector<int> ordersbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(lineitemProbeSchema,lineitemprobeHashChannels,lineitemprobeOutputChannels,ordersBuildSchema,ordersbuildHashChannels,ordersbuildOutputChannels,ordersBuildOutputSchema);
        LookupJoinNode *LOJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanOrders);


        LocalExchangeNode *lexchange = new LocalExchangeNode("localExchange","hash",{0});
        lexchange->addSource(buildExchange);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        LocalExchangeNode *taleScanLocalExchange = new LocalExchangeNode("taleScanLocalExchange");
        taleScanLocalExchange->addSource(probeExchange);

        LOJoin->addProbe(probeExchange);
        LOJoin->addBuild(lexchange);


        return LOJoin;


    }



    TableScanNode *createOrdersTableScan()
    {
        //and o_orderdate < date '[DATE]'

        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));
        return tableScanOrders;

    }





    TableScanNode *createLineitemTableScan()
    {
        //l_commitdate < l_receiptdate
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));
        return tableScanLineitem;
    }


    PartialAggregationNode *createPartialAgg()
    {


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"count_all",/*inputKey=*/"",/*outputName=*/"pcount_orderkey")},
                /*groupByKeys=*/{});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"pcount_orderkey",/*outputName=*/"fcount_orderkey")},
                /*groupByKeys=*/{});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }


};






#endif //OLVP_QUERY2JOINMULHASHBUILD_HPP
