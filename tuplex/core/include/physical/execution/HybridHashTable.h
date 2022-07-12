//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HYBRIDHASHTABLE_H
#define TUPLEX_HYBRIDHASHTABLE_H

#include <Python.h>
#include <hashmap.h>
#include <physical/execution/TransformTask.h>
#include <TypeSystem.h>

namespace tuplex {
    // define hybrid Tuplex/Python hashtable object
    struct HybridLookupTable {
        PyObject_HEAD;
        // Type-specific fields follow
        HashTableSink* sink;
        python::Type hmElementType; // what type is stored in the hashmap (compare when inserting)
        python::Type hmBucketType; // whatever is stored within the bucket, must be tuple type!
        PyObject* backupDict; // pure python backup dictionary holding all other keys...

        HybridLookupTable() : sink(nullptr) {}

        // hack: we know that C++ this comes first, so we can use this to hack the struct together
        Py_ssize_t length();
        PyObject* getItem(PyObject* key);

        int putItem(PyObject* key, PyObject* value);

        int putKey(PyObject* key); // for unique to simulate set

        /*!
         * returns the number of rows/elements in the backup dict.
         * @return
         */
        size_t backupItemCount() const;
    };

    extern PyObject* decodeBucketToPythonList(const uint8_t* bucket, const python::Type& bucketType);

    extern HybridLookupTable* CreatePythonHashMapWrapper(HashTableSink& sink, const python::Type& elementType, const python::Type& bucketType);
}


//// define here mapping methods, that's all what is required for this internal python object
//static PyMappingMethods hybrid_as_mapping {
//        (lenfunc)TplxHybridDict_length,             // mp_length
//        (binaryfunc)TplxHybridDict_GetItem,         // mp_subscript
//        (objobjargproc)TplxHybridDict_SetOrDelItem  // mp_ass_subscript
//};
static Py_ssize_t wrapCCLength(PyObject *o) { assert(o); return ((tuplex::HybridLookupTable*)o)->length(); }
static PyObject* wrapCCGetItem(PyObject *o, PyObject* key) { assert(o); return ((tuplex::HybridLookupTable*)o)->getItem(key); }
static int wrapCCSetItem(PyObject *o, PyObject* key, PyObject* v) { assert(o); return ((tuplex::HybridLookupTable*)o)->putItem(key, v); }

// define here mapping methods, that's all what is required for this internal python object
static PyMappingMethods hybrid_as_mapping {
        (lenfunc)wrapCCLength,             // mp_length
        (binaryfunc)wrapCCGetItem,         // mp_subscript
        (objobjargproc)wrapCCSetItem  // mp_ass_subscript
};


// this only works with Clang but not GCC
//#ifdef BOOST_CLANG
//static PyTypeObject InternalHybridTableType {
//        PyVarObject_HEAD_INIT(NULL, 0)
//        .tp_name = "internal.InternalHybridTable",
//        .tp_doc = "internal hybrid hashmap",
//        .tp_basicsize = sizeof(tuplex::HybridLookupTable),
//        .tp_itemsize = 0,
//        .tp_flags = Py_TPFLAGS_DEFAULT,
//        .tp_new = PyType_GenericNew,
//        .tp_as_mapping = &hybrid_as_mapping,
//};
//#else

//  @StarterProject: Add to the internal hybrid table left out functions
//                   A good overview of how this is done can be found under https://github.com/python/cpython/blob/master/Objects/unicodeobject.c
static PyTypeObject InternalHybridTableType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "internal.InternalHybridTable",                        /* tp_name */
    sizeof(tuplex::HybridLookupTable),      /* tp_basicsize */
    0,                            /* tp_itemsize */
    /* Slots */
    (destructor)nullptr,          /* tp_dealloc */
    0,                            /* tp_vectorcall_offset */
    0,                            /* tp_getattr */
    0,                            /* tp_setattr */
    0,                            /* tp_as_async */
    0,                            /* tp_repr */
    NULL,                         /* tp_as_number */
    NULL,                         /* tp_as_sequence */
    &hybrid_as_mapping,           /* tp_as_mapping */
    (hashfunc) NULL,              /* tp_hash*/
    0,                            /* tp_call*/
    (reprfunc) NULL,              /* tp_str */
    PyObject_GenericGetAttr,      /* tp_getattro */
    0,                            /* tp_setattro */
    0,                            /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    "internal hybrid hashmap",    /* tp_doc */
    0,                            /* tp_traverse */
    0,                            /* tp_clear */
    NULL,                         /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    NULL,                         /* tp_iter */
    0,                            /* tp_iternext */
    NULL,                         /* tp_methods */
    0,                            /* tp_members */
    0,                            /* tp_getset */
    &PyBaseObject_Type,           /* tp_base */
    0,                            /* tp_dict */
    0,                            /* tp_descr_get */
    0,                            /* tp_descr_set */
    0,                            /* tp_dictoffset */
    0,                            /* tp_init */
    0,                            /* tp_alloc */
    PyType_GenericNew,            /* tp_new */
    PyObject_Del,                 /* tp_free */
};
//#endif

// compatible version



#endif //TUPLEX_HYBRIDHASHTABLE_H