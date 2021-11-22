//
// Created by Leonhard Spiegelberg on 11/22/21.
//

#include <main.h>

int main(int argc, char* argv[]) {
    using namespace std;
    using namespace tuplex;

    cout<<"Hello world!"<<endl;

    // init Worker with default settings
    auto app = make_unique<WorkerApp>(WorkerSettings());

    return app->messageLoop();

    return 0;
}