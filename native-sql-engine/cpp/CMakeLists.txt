cmake_minimum_required(VERSION 3.16)
project(spark_columnar_plugin)

#add_definitions(-DSKIPWRITE -DSKIPCOMPRESS -DPROCESSROW)
add_definitions(-DPROCESSROW)

add_compile_options(-g)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(root_directory ${PROJECT_BINARY_DIR})
add_subdirectory(src)
