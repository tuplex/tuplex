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
#include <physical/TransformTask.h>
#include <TypeSystem.h>

namespace tuplex {

// #error "need to define value storage mode for lookup table. I.e., a list of values or values? For join/groupby use list of values, for aggregateByKey use value."


    extern PyObject* unwrapRow(PyObject *o);

    enum LookupStorageMode {
        UNKNOWN=0,
        VALUE=1,
        LISTOFVALUES=2
    };

    // binary layout of value mode is value size + value
    // binary layout of listofvalues is more complex.


    // define hybrid Tuplex/Python hashtable object
    struct HybridLookupTable {
        PyObject_HEAD;
        // Type-specific fields follow
        HashTableSink* sink;
        python::Type hmElementType; // what type is stored in the hashmap (compare when inserting)
        python::Type hmBucketType; // whatever is stored within the bucket, must be tuple type!
        PyObject* backupDict; // pure python backup dictionary holding all other keys...

        LookupStorageMode valueMode; // how values are stored

        HybridLookupTable() : sink(nullptr), valueMode(LookupStorageMode::UNKNOWN) {}

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

        PyObject* setDefault(PyObject* key, PyObject* value);

        /*!
         * retrieves underlying python dictionary
         * @param remove whether to reset dict or not
         * @return the underlying python backup dictionary
         */
        PyObject* pythonDict(bool remove= false);

        /*!
         * update internal structure with given dict object
         * @param dictObject
         */
        void update(PyObject* dictObject);

        /*!
         * release all memory hold by this hybrid.
         */
        void free();

    private:
        /*
         * checks for key existence throughout all structures...
         */
        bool _key_exists(PyObject* key);
    };

    extern PyObject* decodeBucketToPythonList(const uint8_t* bucket, const python::Type& bucketType);

    extern HybridLookupTable* CreatePythonHashMapWrapper(HashTableSink& sink, const python::Type& elementType,
                                                         const python::Type& bucketType, const LookupStorageMode& valueMode);
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

static PyObject* hybrid_dict_setdefault(PyObject* self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *return_value = nullptr;
    PyObject* key = nullptr;
    PyObject *default_value = Py_None;

#if PY_MAJOR_VERSION >=3 && PY_MINOR_VERSION >= 8
    if(!_PyArg_CheckPositional("setdefault", nargs, 1, 2)) {
        goto exit_func;
    }
#else
    if (!_PyArg_UnpackStack(args, nargs, "setdefault",
        1, 2,
        &key, &default_value)) {
        goto exit_func;
    }
#endif

    key = args[0];
    if(nargs < 2) {
        goto skip_optional;
    }
    default_value = args[1];

skip_optional:
    return_value = ((tuplex::HybridLookupTable*)self)->setDefault(key, default_value);
exit_func:
    return return_value;
}

// define here mapping methods, that's all what is required for this internal python object
static PyMappingMethods hybrid_as_mapping {
        (lenfunc)wrapCCLength,             // mp_length
        (binaryfunc)wrapCCGetItem,         // mp_subscript
        (objobjargproc)wrapCCSetItem  // mp_ass_subscript
};

// from https://github.com/python/cpython/blob/6f6a4e6cc5cd76af4a53ffbb62b686142646ac9a/Objects/clinic/dictobject.c.h
// #define DICT_SETDEFAULT_METHODDEF    \
//    {"setdefault", _PyCFunction_CAST(dict_setdefault), METH_FASTCALL, dict_setdefault__doc__},

static PyMethodDef hybrid_methods[] = {
        {"setdefault", (PyCFunction)(void(*)(void))(hybrid_dict_setdefault), METH_FASTCALL, ""},
        {NULL, NULL} // sentinel
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
    hybrid_methods,               /* tp_methods */
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