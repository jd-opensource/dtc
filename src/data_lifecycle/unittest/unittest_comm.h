#ifndef READ_UNITEST_COMMON_H_
#define READ_UNITEST_COMMON_H_

#include <malloc.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#define UNITEST_NAMESPACE_BEGIN     namespace testing{
#define UNITEST_NAMESPACE_END       }

#define DELETE(pointer) \
    do \
    { \
    if(pointer) { \
        delete pointer; \
        pointer = 0; \
    } \
    } while(0)

#endif