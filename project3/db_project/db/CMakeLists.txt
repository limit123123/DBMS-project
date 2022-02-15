# Sources
set(DB_SOURCE_DIR src)
set(DB_SOURCES
  ${DB_SOURCE_DIR}/file.cc
  ${DB_SOURCE_DIR}/bpt.cc
  # Add your sources here
  # ${DB_SOURCE_DIR}/foo/bar/your_source.cc
  ${DB_SOURCE_DIR}/disk_based_bpt.cc
  ${DB_SOURCE_DIR}/buffer.cc
  )

# Headers
set(DB_HEADER_DIR include)
set(DB_HEADERS
  ${DB_HEADER_DIR}/file.h
  ${DB_HEADER_DIR}/bpt.h
  # Add your headers here
  # ${DB_HEADER_DIR}/foo/bar/your_header.h
  ${DB_HEADER_DIR}/page.h
  ${DB_HEADER_DIR}/disk_based_bpt.h
  ${DB_HEADER_DIR}/buffer.h
  )
add_library(db STATIC ${DB_HEADERS} ${DB_SOURCES})

target_include_directories(db
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${DB_HEADER_DIR}"
  )

