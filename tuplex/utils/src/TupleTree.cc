//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <TupleTree.h>
#include <Field.h>
#include <Utils.h>

namespace tuplex {
    Tuple flattenTupleTreeHelper(TupleTreeNode<Field> *node) {
        //straight forward flattening algorithm
        // basically check what kind of type children are, if all leaves ==> flatten node
        // else recursive flattening
        assert(node);
        std::vector<Field> elements;
        for(auto& c : node->children) {
            // is leave?
            if(c->isLeaf()) {
                elements.push_back(c->data);
            } else {
                // recursive call & store result
                elements.push_back(Field(flattenTupleTreeHelper(c)));
            }
        }

        return Tuple::from_vector(elements);
    }

    Tuple flattenToTuple(const TupleTree<Field>& tree) {
        return flattenTupleTreeHelper(tree._root);
    }

    TupleTree<Field> tupleToTree(const Tuple& t) {
        TupleTree<Field> tree(t.getType());

        for(auto index : tree.getMultiIndices()) {
            // special case: index is empty, then the field must be emptytuple. Avoid if by assigning here
            // as default
            Field f = Field(Tuple());

            if(index.size() > 0) {
                // extract element (first go through nesting, then retrieve field from tuple
                Tuple tt = t;
                // new approach
                for(int j : index) {
                    auto lookup = tt.getField(j);
                    if(lookup.getType().isTupleType()) {
                        tt = *((Tuple *) tt.getField(j).getPtr());
                    }
                    f = lookup;
                }
                // make sure field is not a tuple type except empty tuple
                assert(!f.getType().isTupleType() || f.getType() == python::Type::EMPTYTUPLE);
            }

            tree.set(index, f);
        }
        return tree;
    }

    python::Type flattenedType(const python::Type& type) {
        TupleTree<int> tree(type);

        return python::Type::makeTupleType(tree.fieldTypes());
    }
}