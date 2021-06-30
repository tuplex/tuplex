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
 * helper interface/trait especially useful for visitors that may or may not fail
 * when executed. Provides a silent and an explicit mode for logging errors/warnings/etc.
 */
class IFailable {
private:
    bool _succeeded;
    bool _silentMode; // don't issue warnings
    std::vector<std::tuple<std::string, std::string>> _messages; //! stores messages in silent mode
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
    }
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

};

#endif //TUPLEX_IFAILABLE_H