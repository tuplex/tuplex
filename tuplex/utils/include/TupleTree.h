//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TUPLETREE_H
#define TUPLEX_TUPLETREE_H

#include <Tuple.h>
#include <Logger.h>

#include "parameters.h"

namespace tuplex {

    /*!
     * helper structure to pack/unpack nested tuples (represent them as tree)
     */
    template<typename T> class TupleTreeNode {
    public:
        ~TupleTreeNode() {
            if(!children.empty()) {
                for(auto& c : children) {
                    delete c;
                    c = nullptr;
                }
            }
        }
        python::Type type;
        T data;
        std::vector<TupleTreeNode*> children;
        bool isLeaf()   { return children.size() == 0; }
    };

/*!
 * helper class for flattening python tuples. Note that empty tuple will be treated as single value!
 * @tparam T
 */
    template<typename T> class TupleTree {
    private:
        int _numElements;
        TupleTreeNode<T> *_root;
        python::Type _tupleType;

        TupleTreeNode<T>* createTupleTreeR(TupleTreeNode<T>* root, python::Type type) {
            // !!! logic like in lookup helper
            assert(root);

            // option type?
            if(type.isOptionType()) {

                auto rtype = type.getReturnType();

                // primitive type?
                // ==> add index!
                if(rtype.isPrimitiveType() || rtype.isDictionaryType() || rtype.isListType() || rtype.isExceptionType()) {
                    assert(!rtype.isTupleType());
                    // end recursive descent, just return the root
                    root->type = type;
                    return root;
                } else {
                    if(rtype.isTupleType()) {
                        // treat as primitive
                        root->type = type;
                        return root;
                    } else {
                        Logger::instance().defaultLogger().error("fatal error: TupleTree can be only constructed using nested tuples so far! Given type is " + type.desc());
                        return nullptr;
                    }
                }
            } else {
                // primitive type?
                // ==> add index!
                if(type.isPrimitiveType() || type.isDictionaryType() || type.isListType() ||
                   type == python::Type::PYOBJECT || type.isConstantValued() || type.isExceptionType()) {
                    assert(!type.isTupleType());
                    // end recursive descent, just return the root
                    root->type = type;
                    return root;
                } else {
                    if(type.isTupleType()) {
                        // recursively call children & append as children!
                        int num_params = type.parameters().size();
                        for(int i = 0; i < num_params; ++i) {
                            TupleTreeNode<T> *child = new TupleTreeNode<T>();
                            root->children.push_back(createTupleTreeR(child, type.parameters()[i]));
                        }
                        root->type = type;
                        return root;
                    } else if(PARAM_USE_ROW_TYPE && type.isRowType()) {
                        // turn row type into tuple for physical representation.
                        // -> this loses column name information.
                        auto column_types = type.get_column_types();
                        for(unsigned i = 0; i < column_types.size(); ++i) {
                            TupleTreeNode<T> *child = new TupleTreeNode<T>();
                            root->children.push_back(createTupleTreeR(child, column_types[i]));
                        }
                        root->type = type;
                        return root;
                    }

                    Logger::instance().defaultLogger().error("fatal error: TupleTree can be only constructed using nested tuples so far! Given type is " + type.desc());
                    return nullptr;
                }
            }
        }

        int countLeaves(TupleTreeNode<T>* root) {
            if(!root)
                return 0;

            if(root->isLeaf())
                return 1;

            int sum = 0;
            for(const auto& c : root->children)
                sum += countLeaves(c);
            return sum;
        }

        void init(const python::Type tupleType) {
            if(_root)
                delete _root;

            _root = new TupleTreeNode<T>();
            _root = createTupleTreeR(_root, tupleType);
            _numElements = countLeaves(_root);
            _tupleType = tupleType;
        }

        void setHelper(TupleTreeNode<T> *node, std::vector<int> index, T val) const {
            assert(node);

            if(index.size() == 0) {
                node->data = val;
            } else {
                assert(node->children.size() > 0);
                assert(index.front() >= 0);
                assert(index.front() < node->children.size());
                setHelper(node->children[index.front()],
                          std::vector<int>(index.begin() + 1,
                                          index.end()), val);
            }
        }

        TupleTreeNode<T>* getHelper(TupleTreeNode<T> *node, std::vector<int> index) const {
            assert(node);
            if(index.size() == 0) {
                return node;
            } else {
                assert(node->children.size() > 0);
                assert(index.front() >= 0);
                assert(index.front() < node->children.size());
                return getHelper(node->children[index.front()],
                                 std::vector<int>(index.begin() + 1,
                                           index.end()));
            }
        }

        template<typename S, typename Functor> void mapLeaves(TupleTreeNode<T>* node, std::vector<S>& v, Functor f) const {
            if(!node)
                return;

            if(node->isLeaf()) {
                v.push_back(f(node));
            } else {
                for(const auto& c : node->children)
                    mapLeaves(c, v, f);
            }
        }

        void updateLeaves(TupleTreeNode<T>* node, const std::vector<T>& v, int& i) {
            if(!node)
                return;
            if(node->isLeaf()) {
                assert(i >= 0 && i < v.size());
                node->data = v[i];
                i++;
            } else {
                for(const auto& c : node->children)
                    updateLeaves(c, v, i);
            }
        }

        TupleTreeNode<T>* searchNthLeave(TupleTreeNode<T>* node, int& i, const int n) const {
            assert(i <= n);
            assert(0 <= i && i < numElements());
            assert(n < numElements());

            if(!node)
                return nullptr;

            if(node->isLeaf()) {
                if (i == n)
                    return node;
                i++;
                return nullptr;
            } else {
                TupleTreeNode<T>* ret = nullptr;
                for(const auto& c : node->children)
                    if((ret = searchNthLeave(c, i, n)))
                        return ret;
                return ret;
            }
        }

        void collectIndices(TupleTreeNode<T>* node,
                            std::vector<std::vector<int>>& collection,
                            std::vector<int> cur={}) const {
            if(!node)
                return;
            if(node->isLeaf()) {
                collection.push_back(cur);
            } else {
                for(int i = 0; i < node->children.size(); ++i) {
                    const auto& c = node->children[i];
                    auto copy = cur;
                    copy.push_back(i);
                    collectIndices(c, collection, copy);
                }
            }
        }

        TupleTreeNode<T>* findSubtreeRoot(const std::vector<int>& index) {
            TupleTreeNode<T>* root = _root;
            int i = 0;
            while(i != index.size()) {
                if(root->children.size() > i)           // account for the case single element and index = {0}
                    root = _root->children[index[i]];
                ++i;
            }
            return root;
        }

    public:
        TupleTree() : _numElements(0), _root(nullptr), _tupleType(python::Type::UNKNOWN) {}

        ~TupleTree() {
            if(_root)
               delete _root;
            _root = nullptr;
            _numElements = 0;
        }

        TupleTree(python::Type tupleType) : _root(nullptr) {
            init(tupleType);
        }

        TupleTree(const TupleTree<T>& other) {
            _root = nullptr;
            _numElements = other._numElements;

            // deep copy elements
            if(other.numElements() > 0) {
                init(other.tupleType());
                setElements(other.elements());
            }
        }

        TupleTree<T> operator = (const TupleTree<T>& other) {
            //deepcopy
            if(other.numElements() > 0 ) {
                init(other.tupleType());
                setElements(other.elements());
            } else {
                if(_root)
                    delete _root;
                _root = nullptr;
                _numElements = 0;
                _tupleType = python::Type::UNKNOWN;
            }

            return *this;
        }

        python::Type tupleType()    const    { return _tupleType; }

        /*!
         * access element via multilevel index
         * @param index
         * @param val
         */
        void set(std::vector<int> index, T val) const { setHelper(_root, index, val); }
        T get(std::vector<int> index) const { return getHelper(_root, index)->data; }

        python::Type fieldType(std::vector<int> index) const { return getHelper(_root, index)->type; }

        /*!
         * access element via simple index
         * @param index
         * @return
         */
        T get(int index) const {
            if(index < 0 || index >= numElements()) {
                throw std::runtime_error("invalid index " + std::to_string(index) + " used in TupleTree of " + pluralize(numElements(), "element"));
            }

            assert(index >= 0 && index < numElements());
            int i = 0;
            auto ret = searchNthLeave(_root, i, index);
            assert(ret);
            return ret->data;
        }

        python::Type fieldType(int index) const {
            assert(index >= 0 && index < numElements());
            int i = 0;
            auto ret = searchNthLeave(_root, i, index);
            assert(ret);


            if(ret->type.isOptionType())
                assert(ret->type.getReturnType().isPrimitiveType() ||
                ret->type.getReturnType() == python::Type::EMPTYTUPLE ||
                ret->type.getReturnType().isDictionaryType() ||
                ret->type.getReturnType().isListType() ||
                ret->type.getReturnType().isTupleType());
            else
                assert(ret->type.isPrimitiveType() || ret->type == python::Type::EMPTYTUPLE ||
                ret->type.isDictionaryType() || ret->type.isListType() || ret->type.isConstantValued() || ret->type == python::Type::PYOBJECT);
            assert(ret->isLeaf());
            return ret->type;
        }

        void set(int index, const T data) const {
            assert(index >= 0 && index < numElements());
            int i = 0;
            auto ret = searchNthLeave(_root, i, index);
            assert(ret);
            assert(ret->isLeaf());
            ret->data = data;
        }

        friend Tuple flattenTupleTreeHelper(TupleTreeNode<Field> *node);
        friend Tuple flattenToTuple(const TupleTree<Field>& tree);

        int numElements() const { return _numElements; }

        /*!
         * returns flattened representation of the elements as vector.
         * @return
         */
        std::vector<T> elements() const {
            // iterate over tree and add elements
            std::vector<T> v;
            mapLeaves(_root, v, [] (TupleTreeNode<T>* node) { return node->data; });
            assert(v.size() == numElements());
            return v;
        }

        /*!
         * update internal storage with flattened representation
         * @param v
         */
        void setElements(const std::vector<T>& v) {
            assert(v.size() == numElements());
            int index = 0;
            updateLeaves(_root, v, index);
        }


        std::vector<python::Type> fieldTypes() const {
            std::vector<python::Type> v;
            mapLeaves(_root, v, [] (TupleTreeNode<T>* node) { return node->type; });
            assert(v.size() == numElements());
            return v;
        };

        std::vector<std::vector<int>> getMultiIndices() const {
            std::vector<std::vector<int>> indices;
            collectIndices(_root, indices);
            assert(indices.size() == numElements());
            return indices;
        }

        TupleTree<T> subTree(const std::vector<int>& index) {

            TupleTreeNode<T>* root = findSubtreeRoot(index);

            // copy out subtree reusing functions
            TupleTree<T> dummy(root->type);
            dummy._root = root;
            auto elements = dummy.elements();
            dummy._root = nullptr; // make sure deconstructor does not call on this
            dummy._numElements = 0;
            dummy._tupleType = python::Type::UNKNOWN;
            TupleTree<T> sub(root->type);
            sub.setElements(elements);
            return sub;
        }

        void setSubTree(const std::vector<int>& index, const TupleTree<T>& other) {
            TupleTreeNode<T>* root = findSubtreeRoot(index);
            assert(root);
            assert(other.tupleType() == root->type);


            // assign using same method as from setElements
            auto v = other.elements();
            int i = 0;
            updateLeaves(root, v, i);
        }
    };

    extern Tuple flattenToTuple(const TupleTree<Field>& tree);
    extern TupleTree<Field> tupleToTree(const Tuple& t);

    /*!
     * returns the flattened type of a (potentitally nested) python type
     * @param type type to flatten
     * @return flattened type, i.e. a tuple with only primitive elements
     */
    extern python::Type flattenedType(const python::Type& type);
}

#endif //TUPLEX_TUPLETREE_H