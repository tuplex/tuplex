//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TGRAPH_H
#define TUPLEX_TGRAPH_H

#include <vector>
#include <map>
#include <queue>
#include <graphviz/GraphVizBuilder.h>
#include <cassert>
#include <Logger.h>
#include <TSet.h>
#include <stack>

/*!
 * template class to model a graph. Internally, a graph is represented via adjacency lists
 * might be a slow implementation for now.
 * @tparam T
 */
template<typename T> class TGraph {
private:
    // id corresponds to index in first vector...
    // allows for quick lookup!
    int _id;
    std::vector<std::vector<int> > _adjList;

    std::map<T, int> _keyMap;

    enum class VisitState {
        WHITE, // unvisited
        GRAY, // about to explore
        BLACK // visited
    };
public:

    TGraph(): _id(0)    {}

    /*!
     * adds a new node to the graph and stores data internally.
     * @param key indetifies node with this key
     * @return ID of the added node
     */
    int addNode(const T& key) {

        // check if already in keyMap
        auto it = _keyMap.find(key);
        if(it == _keyMap.end()) {
            // add
            _keyMap[key] = _id;
            _adjList.push_back(std::vector<int>());
            _id++;
            return _id - 1;
        } else
            return _keyMap[key];
    }

    /*!
     * adds a new edge to the graph connecting node u -> v
     * @param u node ID from which edge originates
     * @param v node ID to which edge originates
     */
    void addEdge(const T& u, const T& v) {
        addNode(u); addNode(v);

        int i = _keyMap[u];
        int j = _keyMap[v];
        // add to u's adjacency list v, if not contained
        if(std::find(_adjList[i].begin(), _adjList[i].end(), j) == _adjList[i].end())
            _adjList[i].push_back(j);
    }

    /*!
     * checks if a node with key exists already within the graph
     * @param data
     * @return
     */
    bool contains(const T& key) const {
        return _keyMap.find(key) != _keyMap.end();
    }

    /*!
     * checks whether node dest can be reached from node src. True if src == dest
     * @param src
     * @param dest
     * @return
     */
    bool isReachable(const T& src, const T& dest) const {
        // check whether nodes are contained, if not => error
        if(!contains(src) || !contains(dest)) {
            Logger::instance().defaultLogger().debug("invalid access to node members");
            return false;
        }

        // bfs via queue
        int start = _keyMap.at(src);
        int end = _keyMap.at(dest);

        // hack to make src = dest faster
        if(start == end)
            return true;

        // solve via BFS (no weights here)
        // setup visited array
        bool *visited = new bool[numNodes()];
        for(int i = 0; i < numNodes(); ++i)visited[i] = false;

        std::queue<int> q;
        q.push(start);
        while(!q.empty()) {
            // take element from queue, mark as visited
            int k = q.front();
            assert(k < numNodes());
            q.pop();
            visited[k] = true;

            // break if dest found
            if(k == end) {
                delete [] visited;
                return true;
            }

            // else add all (unvisited) neighbors to queue
            for(int n : _adjList[k]) {
                assert(n < numNodes());
                if(!visited[n])
                    q.push(n);
            }
        }

        delete [] visited;
        return false;
    }

    int numNodes() const {
        return _adjList.size();
    }

    int numEdges() const {
        int num = 0;
        for(auto L : _adjList)
            num += L.size();
        return num;
    }

    /*!
     * builds a graphviz graph.
     * Calls for each node a function to generate a label
     * @param builder graphviz builder which holds the graph description
     * @param label function to provide a label for each key
     * @param html specifies whether result of label function should be treated as string or html label
     */
    void buildViz(GraphVizBuilder& builder,
                  std::function<std::string(const T& val)> label,
                  bool html=false) {

        // mapping internal ids to graphbuilder ids
        std::map<int, int> M;

        // add nodes
        for(auto it = _keyMap.begin(); it != _keyMap.end(); ++it) {
            if(html)
                M[it->second] = builder.addHTMLNode(label(it->first));
            else
            M[it->second] = builder.addNode(label(it->first));
        }

        // add edges
        for(int i = 0; i < _adjList.size(); i++) {
            for(int j : _adjList[i]) {
                builder.addEdge(M[i], M[j]);
            }
        }
    }

    /*!
     * returns a set of all nodes
     * @return
     */
    TSet<T> getNodes() const {
        TSet<T> S;
        for(auto it : _keyMap) {
            S.add(it.first);
        }
        return S;
    }

    /*!
     * determines whether graph is a directed acyclic graph (DAG)
     * @return true if it is a DAG else false
     */
    bool isDAG() const {
        // empty graph
        if(0 == numNodes())
            return true;

        // determine via DFS. A graph is acyclic iff there are no backedges when performing dfs
        std::stack<int> S;

        VisitState *state = new VisitState[numNodes()];
        for(int i = 0; i < numNodes(); ++i)
            state[i] = VisitState::WHITE;

        // repeat for all strongly connected components. I.e. define a graph to be DAG
        // iff all its strongly connected components are DAGs.
        bool allVisited = false;

        S.push(0);
        while(!allVisited) {
            // add first edge


            while(S.size() > 0) {
                // pop node & visit
                int v = S.top();
                // v = gray
                S.pop();
                state[v] = VisitState::GRAY;

                // fetch all neighbors
                assert(v < _adjList.size());
                for(auto n : _adjList[v]) {
                    assert(n < _adjList.size());

                    // only add neighbor, if not visited yet
                    if(state[n] == VisitState::WHITE)
                        S.push(n);

                    // if neighbor is gray, a back edge was found
                    if(state[n] == VisitState::GRAY) {
                        delete [] state;
                        return false;
                    }

                }
                // node has been visited
                state[v] = VisitState::BLACK;
            }

            // perform check whether all nodes were visited, if not push first non-visited node
            allVisited = true;
            int idxNonVisited = 0;
            for(int i = 0; i < numNodes(); i++) {
                if(state[i] == VisitState::WHITE) {
                    allVisited = false;
                    idxNonVisited = i;
                    break;
                }
            }

            if(!allVisited) {
                S.push(idxNonVisited);
            }
        }

        delete [] state;
        return true;
    }
};

#endif //TUPLEX_TGRAPH_H