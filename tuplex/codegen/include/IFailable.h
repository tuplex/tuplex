//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IFAILABLE_H
#define TUPLEX_IFAILABLE_H

#include <Base.h>
#include <Logger.h>

/*!
 * error handling for unsupported language features (i.e. valid python UDF codes but not supported yet in Tuplex)
 */
enum class CompileError {
    COMPILE_ERROR_NONE,
    TYPE_ERROR_LIST_OF_LISTS,
    TYPE_ERROR_RETURN_LIST_OF_TUPLES,
    TYPE_ERROR_RETURN_LIST_OF_DICTS,
    TYPE_ERROR_RETURN_LIST_OF_LISTS,
    TYPE_ERROR_RETURN_LIST_OF_MULTITYPES,
    TYPE_ERROR_LIST_OF_MULTITYPES,
    TYPE_ERROR_ITER_CALL_WITH_NONHOMOGENEOUS_TUPLE,
    TYPE_ERROR_ITER_CALL_WITH_DICTIONARY,
    TYPE_ERROR_RETURN_ITERATOR,
    TYPE_ERROR_NEXT_CALL_DIFFERENT_DEFAULT_TYPE,
    TYPE_ERROR_MIXED_ASTNODETYPE_IN_FOR_LOOP_EXPRLIST, // exprlist contains a mix of tuple/list of identifiers and single identifier
};

/*!
 * helper interface/trait especially useful for visitors that may or may not fail
 * when executed. Provides a silent and an explicit mode for logging errors/warnings/etc.
 */
class IFailable {
private:
    bool _succeeded;
    bool _silentMode; // don't issue warnings
    std::vector<std::tuple<std::string, std::string>> _messages; //! stores messages in silent mode
    std::vector<CompileError> _compileErrors;

protected:
    /*!
     * logs an error. this will automatically set the status to failure
     * @param message
     * @param logger optional logger to specify
     */
    virtual void error(const std::string& message, const std::string& logger="");

    virtual void fatal_error(const std::string& message, const std::string& logger="") {
        error(message, logger);
        throw std::runtime_error(message);
    }

    void reset() {
        _succeeded = true;
        _messages.clear();
        _compileErrors.clear();
    }

    /*!
     * add all CompileErrors in err to _compileErrors
     * @param err
     */
    void addCompileErrors(const std::vector<CompileError> &err) {_compileErrors.insert(_compileErrors.begin(), err.begin(), err.end());}

    /*!
     * add single CompileError to _compileErrors
     * @param err
     */
    void addCompileError(const CompileError& err) {_compileErrors.push_back(err);}

public:

    IFailable(bool silentMode=false) : _succeeded(true), _silentMode(silentMode)  {}

    bool failed() const { return !_succeeded;}
    bool succeeded() const { return _succeeded; }

    void setFailingMode(bool silentMode)    { _silentMode = silentMode; }

    /*!
     * if operated in silent mode, this allows to log out all messages (deletes them from internal buffer)
     */
    void logMessages();

    std::vector<std::tuple<std::string, std::string>> getErrorMessages() const { return _messages; }

    /*!
     * return all type errors (errors generated from unsupported types) encountered for the current class instance.
     * @return
     */
    std::vector<CompileError> getCompileErrors() {return _compileErrors;}

    /*!
     * return CompileError of returning list of lists/tuples/dicts/multi-types. If no such error exists, return COMPILE_ERROR_NONE.
     * @return
     */
    CompileError getReturnError();

    /*!
     * clear all compile errors (errors generated from unsupported language features) for the current class instance.
     */
    void clearCompileErrors() {_compileErrors.clear();}

    /*!
     * return detailed error message of a CompileError.
     * @param err
     * @return
     */
    std::string compileErrorToStr(const CompileError& err);
};

#endif //TUPLEX_IFAILABLE_H