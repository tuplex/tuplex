//
// Created by Leonhard Spiegelberg on 9/14/22.
//

#include "TestUtils.h"
#include "JsonStatistic.h"

#include <AccessPathVisitor.h>

class Github : public PyTest {};

TEST_F(Github, BasicRead) {
    // explore change on 4th January 2022: The field Push.Pusher changed named to Push.User.
    using namespace tuplex;


}