//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TSINGLETON_H
#define TUPLEX_TSINGLETON_H

#include <mutex>
#include <functional>
#include <memory>
#include <utility>
// singleton template for easier declaration of singleton classes
// inspired by https://gist.github.com/bianjiang/5846512

namespace core {
    class noncopyable {
    protected:
        noncopyable() {}

        ~noncopyable() {}

    private:
        noncopyable(const noncopyable &);

        const noncopyable &operator=(const noncopyable &);
    };

    using std::call_once;
    using std::once_flag;

    /*!
     * template to declare singleton classes fast
     * @tparam T
     */
    template<class T> class TSingleton : private noncopyable {
    public:
        template<typename... Args>
        static T &instance(Args &&... args) {
            call_once(get_once_flag(),
                      [](Args &&... args) {

                            if(_instance)
                                delete _instance;

                          _instance = new T(std::forward<Args>(args)...);
                      }, std::forward<Args>(args)...);

            return *_instance;
        }
    protected:
        explicit TSingleton<T>() {}

        virtual ~TSingleton<T>() {
            if(_instance)
                delete _instance;
            _instance = nullptr;
        }

    private:
        static T *_instance;

        static once_flag &get_once_flag() {
            static once_flag _once;
            return _once;
        }
};

    template<class T> T* TSingleton<T>::_instance = nullptr;
}



#endif //TUPLEX_TSINGLETON_H