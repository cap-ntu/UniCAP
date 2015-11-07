aux_source_directory(. DIR_LIB_SRCS)
add_executable(uniCAPMaster job_tracker.cpp)
target_link_libraries(uniCAPMaster JobTracker Computing Common Storage Gen ${libthrift} ${libglog} ${libleveldb} ${libsnappy} ${libhdfs} ${libyaml-cpp} ${OpenCV_LIBS} ${Boost_LIBRARIES})
