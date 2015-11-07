cmake_minimum_required (VERSION 2.8.12)
project (UniCAP)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set(CMAKE_BUILD_TYPE "Debug")
#set(CMAKE_BUILD_TYPE "Release")
#set(CMAKE_VERBOSE_MAKEFILE ON)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
    message("The compiler ${CMAKE_CXX_COMPILER} support c++11")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -std=c++11")

set(LINK_DIR /lib /usr/lib /usr/local/lib ${CMAKE_SOURCE_DIR}/src/tools/lib)

set(CMAKE_CXX_COMPILER "g++")
INCLUDE_DIRECTORIES(/usr/local/include)

FIND_LIBRARY(libyaml-cpp NAMES "yaml-cpp" PATHS ${LINK_DIR})
IF(NOT libyaml-cpp)
MESSAGE(FATAL_ERROR “libyaml-cpp not found”)
ENDIF(NOT libyaml-cpp)
MESSAGE(--${libyaml-cpp})

FIND_LIBRARY(libhdfs NAMES "hdfs" PATHS ${LINK_DIR})
IF(NOT libhdfs)
MESSAGE(FATAL_ERROR “libhdfs not found”)
ENDIF(NOT libhdfs)
MESSAGE(--${libhdfs})

FIND_LIBRARY(libthrift NAMES "thrift" PATHS ${LINK_DIR})
IF(NOT libthrift)
MESSAGE(FATAL_ERROR “libthrift not found”)
ENDIF(NOT libthrift)
MESSAGE(--${libthrift})

FIND_LIBRARY(libglog NAMES "glog" PATHS ${LINK_DIR})
IF(NOT libglog)
MESSAGE(FATAL_ERROR “libglog not found”)
ENDIF(NOT libglog)
MESSAGE(--${libglog})

FIND_LIBRARY(libleveldb NAMES "leveldb" PATHS ${LINK_DIR})
IF(NOT libleveldb)
MESSAGE(FATAL_ERROR “libleveldb not found”)
ENDIF(NOT libleveldb)
MESSAGE(--${libleveldb})

FIND_LIBRARY(libsnappy NAMES "snappy" PATHS ${LINK_DIR})
IF(NOT libsnappy)
MESSAGE(FATAL_ERROR “libsnappy not found”)
ENDIF(NOT libsnappy)
MESSAGE(--${libsnappy})

FIND_LIBRARY(libmpi NAMES "mpi" PATHS ${LINK_DIR})
IF(NOT libmpi)
MESSAGE(FATAL_ERROR “libmpi not found”)
ENDIF(NOT libmpi)
MESSAGE(--${libmpi})

FIND_PACKAGE( Boost 1.5 COMPONENTS system filesystem thread REQUIRED )
INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )

FIND_PACKAGE( OpenCV REQUIRED)

add_subdirectory(src/common)
add_subdirectory(src/storage)
add_subdirectory(src/gen)
add_subdirectory(src/job_tracker)
add_subdirectory(src/computing)
add_subdirectory(app)


set(CMAKE_CXX_COMPILER "mpic++")
add_subdirectory(src/task_tracker)

