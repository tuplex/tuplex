#ifndef TRACE_ROW_OBJECT_HEADER_
#define TRACE_ROW_OBJECT_HEADER_

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include <vector>
#include <string>
#include "TypeSystem.h"

// helper object to store row information (similar to generated row class)
// for tracer. Need to implement get/set protocols for this one.
namespace tuplex {
    struct TraceRowObject {
        PyObject_HEAD;
        std::vector<PyObject *> items;
        std::vector<std::string> columns;

        TraceRowObject(const std::vector<PyObject *> _items, const std::vector<std::string> &_columns = {}) : items(
                _items), columns(_columns) {
            for (auto item: items)
                Py_XINCREF(item);
        }

        // we know that C++ this-pointer comes first, so we can use this to hack the struct together
        Py_ssize_t length();

        int putItem(PyObject* key, PyObject* value);
        PyObject* getItem(PyObject* key);

        PyObject* setDefault(PyObject* key, PyObject* value);

        static TraceRowObject* create(const std::vector<PyObject*> items, const std::vector<std::string>& columns={});

        python::Type rowType(bool autoUpcast=false) const;
    };

}

static Py_ssize_t tr_wrapCCLength(PyObject *o) { assert(o); return ((tuplex::TraceRowObject*)o)->length(); }
static PyObject* tr_wrapCCGetItem(PyObject *o, PyObject* key) { assert(o); return ((tuplex::TraceRowObject*)o)->getItem(key); }
static int tr_wrapCCSetItem(PyObject *o, PyObject* key, PyObject* v) { assert(o); return ((tuplex::TraceRowObject*)o)->putItem(key, v); }

static void tr_dealloc(tuplex::TraceRowObject *self) {
        if(!self->items.empty()) {
            for(auto item : self->items)
                Py_XDECREF(item);
            self->items.clear();
        }
        self->columns.clear();
        Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject* tr_setdefault(PyObject* self, PyObject *const *args, Py_ssize_t nargs) {
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
    return_value = ((tuplex::TraceRowObject*)self)->setDefault(key, default_value);
    exit_func:
    return return_value;
}

// template impl. for get
// static PyObject *
//dict_get(PyDictObject *self, PyObject *const *args, Py_ssize_t nargs)
//{
//    PyObject *return_value = NULL;
//    PyObject *key;
//    PyObject *default_value = Py_None;
//
//    if (!_PyArg_CheckPositional("get", nargs, 1, 2)) {
//        goto exit;
//    }
//    key = args[0];
//    if (nargs < 2) {
//        goto skip_optional;
//    }
//    default_value = args[1];
//skip_optional:
//    return_value = dict_get_impl(self, key, default_value);
//
//exit:
//    return return_value;
//}
static PyObject* tr_get(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
{
    PyObject *return_value = NULL;
    PyObject *key;
    PyObject *default_value = Py_None;

    if (!_PyArg_CheckPositional("get", nargs, 1, 2)) {
        goto exit;
    }
    key = args[0];
    if (nargs < 2) {
        goto skip_optional;
    }
    default_value = args[1];
skip_optional:
    return_value = tr_wrapCCGetItem(self, key);
    if(!return_value) {
        return_value = default_value;
        Py_XINCREF(default_value);
    }
exit:
    return return_value;
}

// define here mapping methods, that's all what is required for this internal python object
static PyMappingMethods tr_as_mapping {
        (lenfunc)tr_wrapCCLength,             // mp_length
        (binaryfunc)tr_wrapCCGetItem,         // mp_subscript
        (objobjargproc)tr_wrapCCSetItem  // mp_ass_subscript
};

static PyMethodDef tr_methods[] = {
        {"setdefault", (PyCFunction)(void(*)(void))(tr_setdefault), METH_FASTCALL, ""},
        {"get", (PyCFunction)(void(*)(void))(tr_get), METH_FASTCALL, ""},
        {NULL, NULL} // sentinel
};

static PyTypeObject TraceRowObjectType = {
        PyVarObject_HEAD_INIT(&PyType_Type, 0)
        "internal.TraceRow",                        /* tp_name */
        sizeof(tuplex::TraceRowObject),      /* tp_basicsize */
        0,                            /* tp_itemsize */
        /* Slots */
        (destructor)tr_dealloc,          /* tp_dealloc */
        0,                            /* tp_vectorcall_offset */
        0,                            /* tp_getattr */
        0,                            /* tp_setattr */
        0,                            /* tp_as_async */
        0,                            /* tp_repr */
        NULL,                         /* tp_as_number */
        NULL,                         /* tp_as_sequence */
        &tr_as_mapping,           /* tp_as_mapping */
        (hashfunc) NULL,              /* tp_hash*/
        0,                            /* tp_call*/
        (reprfunc) NULL,              /* tp_str */
        PyObject_GenericGetAttr,      /* tp_getattro */
        0,                            /* tp_setattro */
        0,                            /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,           /* tp_flags */
        "internal trace row object",    /* tp_doc */
        0,                            /* tp_traverse */
        0,                            /* tp_clear */
        NULL,                         /* tp_richcompare */
        0,                            /* tp_weaklistoffset */
        NULL,                         /* tp_iter */
        0,                            /* tp_iternext */
        tr_methods,               /* tp_methods */
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

namespace tuplex {

//    static void
//    Custom_dealloc(CustomObject *self)
//    {
//        Py_XDECREF(self->first);
//        Py_XDECREF(self->last);
//        Py_TYPE(self)->tp_free((PyObject *) self);
//    }
//
//    static PyObject *
//    Custom_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
//    {
//        CustomObject *self;
//        self = (CustomObject *) type->tp_alloc(type, 0);
//        if (self != NULL) {
//            self->first = PyUnicode_FromString("");
//            if (self->first == NULL) {
//                Py_DECREF(self);
//                return NULL;
//            }
//            self->last = PyUnicode_FromString("");
//            if (self->last == NULL) {
//                Py_DECREF(self);
//                return NULL;
//            }
//            self->number = 0;
//        }
//        return (PyObject *) self;
//    }
//
//    static int
//    Custom_init(CustomObject *self, PyObject *args, PyObject *kwds)
//    {
//        static char *kwlist[] = {"first", "last", "number", NULL};
//        PyObject *first = NULL, *last = NULL, *tmp;
//
//        if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OOi", kwlist,
//                                         &first, &last,
//                                         &self->number))
//            return -1;
//
//        if (first) {
//            tmp = self->first;
//            Py_INCREF(first);
//            self->first = first;
//            Py_XDECREF(tmp);
//        }
//        if (last) {
//            tmp = self->last;
//            Py_INCREF(last);
//            self->last = last;
//            Py_XDECREF(tmp);
//        }
//        return 0;
//    }
//
//    static PyMemberDef Custom_members[] = {
//        {"first", T_OBJECT_EX, offsetof(CustomObject, first), 0,
//         "first name"},
//        {"last", T_OBJECT_EX, offsetof(CustomObject, last), 0,
//         "last name"},
//        {"number", T_INT, offsetof(CustomObject, number), 0,
//         "custom number"},
//        {NULL}  /* Sentinel */
//    };
//
//    static PyObject *
//    Custom_name(CustomObject *self, PyObject *Py_UNUSED(ignored))
//    {
//        if (self->first == NULL) {
//            PyErr_SetString(PyExc_AttributeError, "first");
//            return NULL;
//        }
//        if (self->last == NULL) {
//            PyErr_SetString(PyExc_AttributeError, "last");
//            return NULL;
//        }
//        return PyUnicode_FromFormat("%S %S", self->first, self->last);
//    }
//
//    static PyMethodDef Custom_methods[] = {
//        {"name", (PyCFunction) Custom_name, METH_NOARGS,
//         "Return the name, combining the first and last name"
//        },
//        {NULL}  /* Sentinel */
//    };
//
//    static PyTypeObject CustomType = {
//        PyVarObject_HEAD_INIT(NULL, 0)
//        .tp_name = "custom2.Custom",
//        .tp_doc = PyDoc_STR("Custom objects"),
//        .tp_basicsize = sizeof(CustomObject),
//        .tp_itemsize = 0,
//        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
//        .tp_new = Custom_new,
//        .tp_init = (initproc) Custom_init,
//        .tp_dealloc = (destructor) Custom_dealloc,
//        .tp_members = Custom_members,
//        .tp_methods = Custom_methods,
//    };
}

#endif