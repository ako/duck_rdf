# duckgraphlake: one executable serving a folder of data files over SQL (Quack),
# SPARQL 1.1 Protocol and MCP. Built inside the DuckDB build, so it links DuckDB
# and every statically configured extension (see extension_config.cmake).
include(FetchContent)

FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.18.7)
FetchContent_GetProperties(httplib)
if(NOT httplib_POPULATED)
  FetchContent_Populate(httplib)
endif()

FetchContent_Declare(
  tomlplusplus
  GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
  GIT_TAG v3.4.0)
FetchContent_GetProperties(tomlplusplus)
if(NOT tomlplusplus_POPULATED)
  FetchContent_Populate(tomlplusplus)
endif()

# The web UI, compiled into the binary.
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/ui/index.html" UI_HTML)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/ui/ui_page.hpp.in" "${CMAKE_CURRENT_BINARY_DIR}/generated/ui_page.hpp" @ONLY)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/ui/index.html")

# The help topics (duckgraphlake help, /api/guide, MCP guide), compiled into the binary.
file(GLOB GUIDE_FILES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/guide/*.md")
list(SORT GUIDE_FILES)
set(GUIDE_ENTRIES "")
foreach(guide_file ${GUIDE_FILES})
  get_filename_component(guide_stem "${guide_file}" NAME_WE)
  string(REGEX REPLACE "^[0-9]+-" "" guide_topic "${guide_stem}")
  file(READ "${guide_file}" guide_text)
  string(APPEND GUIDE_ENTRIES "    {\"${guide_topic}\", R\"GUIDE(${guide_text})GUIDE\"},\n")
endforeach()
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/guide/guide_topics.hpp.in"
               "${CMAKE_CURRENT_BINARY_DIR}/generated/guide_topics.hpp" @ONLY)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${GUIDE_FILES})

file(GLOB SERVER_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
add_executable(duckgraphlake ${SERVER_SOURCES})
target_include_directories(
  duckgraphlake PRIVATE src "${CMAKE_CURRENT_BINARY_DIR}/generated" ${httplib_SOURCE_DIR} ${tomlplusplus_SOURCE_DIR}/include
                        ${DUCKDB_MODULE_BASE_DIR}/third_party/yyjson/include
                        ${DUCKDB_MODULE_BASE_DIR}/third_party/miniz ${DUCKDB_MODULE_BASE_DIR}/third_party/zstd/include)
target_compile_definitions(duckgraphlake PRIVATE CPPHTTPLIB_THREAD_POOL_COUNT=16
                                         CPPHTTPLIB_FORM_URL_ENCODED_PAYLOAD_MAX_LENGTH=4194304)
target_link_libraries(duckgraphlake duckdb_static yaml-cpp ${DUCKDB_EXTRA_LINK_FLAGS})
link_threads(duckgraphlake "")
link_extension_libraries(duckgraphlake "")
set_target_properties(duckgraphlake PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}")
