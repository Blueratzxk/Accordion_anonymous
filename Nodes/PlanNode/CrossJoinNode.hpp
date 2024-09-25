//
// Created by anonymous on 6/28/23.
//

#ifndef OLVP_CROSSJOINNODE_HPP
#define OLVP_CROSSJOINNODE_HPP



#include "../../Descriptor/CrossJoinDescriptor.hpp"

class CrossJoinNode :public PlanNode
{

    CrossJoinDescriptor desc;
    PlanNode *probe;
    PlanNode *build;
public:


    CrossJoinNode(string id,CrossJoinDescriptor desc):PlanNode("CrossJoinNode",id)
    {
        this->desc = desc;
    }


    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitCrossJoinNode(this,context);
    }
    void addProbe(PlanNode *node)
    {
        this->probe = node;
    }
    void addBuild(PlanNode *node)
    {
        this->build = node;
    }
    CrossJoinDescriptor getCrossJoinDescriptor()
    {
        return this->desc;
    }
    PlanNode* getProbe(){
        return this->probe;
    }
    PlanNode* getBuild(){
        return this->build;
    }
    void addSources(vector<PlanNode*> nodes)
    {
        if(nodes.size() != 2){cout << "hash join node need 2 source!" << endl;exit(0);}
        this->probe = nodes[0];
        this->build = nodes[1];
    }
    void addSources(PlanNode *node){}
    vector<PlanNode*> getSources(){
        vector<PlanNode*> sources{this->probe,this->build};
        return sources;
    }
    string getId()
    {
        return PlanNode::getId();
    }

    PlanNode* replaceChildren(vector<PlanNode*> newChildren){
        CrossJoinNode *hashJoin = new CrossJoinNode(this->getId(),this->desc);
        hashJoin-> addProbe(newChildren[0]);
        hashJoin->addBuild(newChildren[1]);
        return hashJoin;
    }
};




#endif //OLVP_CROSSJOINNODE_HPP
