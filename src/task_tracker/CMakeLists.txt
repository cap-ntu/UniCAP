aux_source_directory(. DIR_LIB_SRCS)
#add_library (TaskTracker ${DIR_LIB_SRCS})
add_executable(uniCAPSlave ${DIR_LIB_SRCS})
target_link_libraries(uniCAPSlave Computing Common Storage Gen  ${libthrift} ${libglog} ${libleveldb} ${libsnappy} ${libhdfs} ${libyaml-cpp} ${OpenCV_LIBS}  ${Boost_LIBRARIES})

