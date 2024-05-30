# Orthanc - A Lightweight, RESTful DICOM Store
# Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
# Department, University Hospital of Liege, Belgium
# Copyright (C) 2017-2023 Osimis S.A., Belgium
# Copyright (C) 2024-2024 Orthanc Team SRL, Belgium
# Copyright (C) 2021-2024 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
#
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


if (STATIC_BUILD OR NOT USE_SYSTEM_UNIX_ODBC)
  include(CheckFunctionExists)
  include(CheckTypeSize)
  
  set(VERSION "2.3.9")  # Used in "config.h.in"
  set(UNIX_ODBC_SOURCES_DIR ${CMAKE_BINARY_DIR}/unixODBC-${VERSION})
  set(UNIX_ODBC_MD5 "06f76e034bb41df5233554abe961a16f")
  set(UNIX_ODBC_URL "https://orthanc.uclouvain.be/downloads/third-party-downloads/unixODBC-${VERSION}.tar.gz")

  DownloadPackage(${UNIX_ODBC_MD5} ${UNIX_ODBC_URL} "${UNIX_ODBC_SOURCES_DIR}")

  if (STATIC_BUILD OR NOT USE_SYSTEM_LTDL)
    add_definitions(
      -DLT_OBJDIR=".libs/"
      -DLTDL  # Necessary for LT_SCOPE to be properly defined
      #-DLT_DEBUG_LOADERS  # Get debug messages
      )

    include_directories(
      ${UNIX_ODBC_SOURCES_DIR}/libltdl
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/libltdl/
      )

    list(APPEND LTDL_SOURCES
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/loaders/dlopen.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/loaders/preopen.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/lt__alloc.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/lt__strl.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/lt_dlloader.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/lt_error.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/ltdl.c
      ${UNIX_ODBC_SOURCES_DIR}/libltdl/slist.c
      )

    if (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
      set(OSXHEADER 1)
      set(__error_t_defined 1)
      set(error_t int)

      # NB: The lines below might also be used for compatibility with
      # LSB target version 4.0 instead of 5.0 (untested)
      configure_file(
        ${UNIX_ODBC_SOURCES_DIR}/libltdl/libltdl/lt__argz_.h
        ${UNIX_ODBC_SOURCES_DIR}/libltdl/libltdl/lt__argz.h
        COPYONLY)
      list(APPEND LTDL_SOURCES
        ${UNIX_ODBC_SOURCES_DIR}/libltdl/lt__argz.c
        )
    endif()
  else()
    check_include_file("libltdl/lt_dlloader.h"  HAVE_LT_DLLOADER_H)
    if (NOT HAVE_LT_DLLOADER_H)
      message(FATAL_ERROR "Please install the libltdl-dev package")
    endif()

    link_libraries(ltdl)    
  endif()


  include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}/AUTOGENERATED
    ${UNIX_ODBC_SOURCES_DIR}/include
    ${UNIX_ODBC_SOURCES_DIR}/DriverManager
    )

  file(GLOB UNIX_ODBC_SOURCES
    ${UNIX_ODBC_SOURCES_DIR}/cur/*.c
    ${UNIX_ODBC_SOURCES_DIR}/DriverManager/*.c
    ${UNIX_ODBC_SOURCES_DIR}/odbcinst/*.c
    ${UNIX_ODBC_SOURCES_DIR}/ini/*.c
    ${UNIX_ODBC_SOURCES_DIR}/log/*.c
    ${UNIX_ODBC_SOURCES_DIR}/lst/*.c
    )

  list(REMOVE_ITEM UNIX_ODBC_SOURCES
    ${UNIX_ODBC_SOURCES_DIR}/cur/SQLConnect.c
    ${UNIX_ODBC_SOURCES_DIR}/cur/SQLGetDiagRec.c
    )


  set(ASCII_ENCODING "auto-search")
  set(SYSTEM_FILE_PATH "/etc")
  set(DEFLIB_PATH "/usr/lib")
  set(ENABLE_DRIVER_ICONV ON)  # Enables support for encodings

  set(STDC_HEADERS 1)
  set(UNIXODBC ON)
  set(UNIXODBC_SOURCE ON)   # This makes "intptr_t" to be defined
  set(ICONV_CONST ON)
  set(STRICT_ODBC_ERROR ON)

  if (CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(PLATFORM64 1)
  endif()

  list(GET CMAKE_FIND_LIBRARY_SUFFIXES 0 SHLIBEXT)

  check_include_file("alloca.h"               HAVE_ALLOCA_H)
  check_include_file("argz.h"                 HAVE_ARGZ_H)
  check_include_file("crypt.h"                HAVE_CRYPT_H)
  check_include_file("dirent.h"               HAVE_DIRENT_H)
  check_include_file("dlfcn.h"                HAVE_DLFCN_H)
  check_include_file("inttypes.h"             HAVE_INTTYPES_H)
  check_include_file("langinfo.h"             HAVE_LANGINFO_H)
  check_include_file("crypt.h"                HAVE_CRYPT_H)
  check_include_file("limits.h"               HAVE_LIMITS_H)
  check_include_file("locale.h"               HAVE_LOCALE_H)
  check_include_file("malloc.h"               HAVE_MALLOC_H)
  check_include_file("memory.h"               HAVE_MEMORY_H)
  check_include_file("pwd.h"                  HAVE_PWD_H)
  check_include_file("stdarg.h"               HAVE_STDARG_H)
  check_include_file("stdlib.h"               HAVE_STDLIB_H)
  check_include_file("string.h"               HAVE_STRING_H)
  check_include_file("strings.h"              HAVE_STRINGS_H)
  check_include_file("time.h"                 HAVE_TIME_H)
  check_include_file("sys/sem.h"              HAVE_SYS_SEM_H)
  check_include_file("sys/stat.h"             HAVE_SYS_STAT_H)
  check_include_file("sys/time.h"             HAVE_SYS_TIME_H)
  check_include_file("sys/timeb.h"            HAVE_SYS_TIMEB_H)
  check_include_file("unistd.h"               HAVE_UNISTD_H)
  check_include_file("readline/readline.h"    HAVE_READLINE_H)
  check_include_file("readline/history.h"     HAVE_READLINE_HISTORY_H)

  check_symbol_exists(alloca "alloca.h"         HAVE_ALLOCA)
  check_symbol_exists(argz_add "argz.h"         HAVE_ARGZ_ADD)
  check_symbol_exists(argz_append "argz.h"      HAVE_ARGZ_APPEND)
  check_symbol_exists(argz_count "argz.h"       HAVE_ARGZ_COUNT)
  check_symbol_exists(argz_create_sep "argz.h"  HAVE_ARGZ_CREATE_SEP)
  check_symbol_exists(argz_insert "argz.h"      HAVE_ARGZ_INSERT)
  check_symbol_exists(argz_next "argz.h"        HAVE_ARGZ_NEXT)
  check_symbol_exists(argz_stringify "argz.h"   HAVE_ARGZ_STRINGIFY)

  check_function_exists(atoll HAVE_ATOLL)
  check_function_exists(closedir HAVE_CLOSEDIR)
  check_function_exists(endpwent HAVE_ENDPWENT)

  if (HAVE_ARGZ_H)
    set(HAVE_WORKING_ARGZ 1)
  endif()

  find_package(Threads)
  if (Threads_FOUND)
    set(HAVE_LIBPTHREAD 1)
  endif ()

  set(CMAKE_REQUIRED_LIBRARIES)
  if (HAVE_DLFCN_H)
    list(APPEND CMAKE_REQUIRED_LIBRARIES "dl")
  endif()
  if (HAVE_CRYPT_H)
    list(APPEND CMAKE_REQUIRED_LIBRARIES "crypt")
  endif()
  if (HAVE_READLINE_H)
    list(APPEND CMAKE_REQUIRED_LIBRARIES "readline")
  endif()
  if (HAVE_LT_DLLOADER_H)
    set(HAVE_LIBDLLOADER 0)  # to improve
    set(HAVE_LTDL 1)  # to improve
  endif()

  check_function_exists(dlerror        HAVE_DLERROR)
  check_function_exists(dlloader_init  HAVE_LIBDLLOADER)
  check_function_exists(dlopen         HAVE_LIBDL)
  check_function_exists(encrypt        HAVE_LIBCRYPT)
  check_function_exists(ftime          HAVE_FTIME)
  check_function_exists(getpwuid       HAVE_GETPWUID)
  check_function_exists(gettimeofday   HAVE_GETTIMEOFDAY)
  check_function_exists(gettimeofday   HAVE_GETTIMEOFDAY)
  check_function_exists(getuid         HAVE_GETUID)
  check_function_exists(iconv          HAVE_ICONV)
  check_function_exists(localtime_r    HAVE_LOCALTIME_R)
  check_function_exists(opendir        HAVE_OPENDIR)
  check_function_exists(putenv         HAVE_PUTENV)
  check_function_exists(readdir        HAVE_READDIR)
  check_function_exists(readline       HAVE_READLINE)
  check_function_exists(setenv         HAVE_SETENV)
  check_function_exists(setlocale      HAVE_SETLOCALE)
  check_function_exists(socket         HAVE_SOCKET)
  check_function_exists(strcasecmp     HAVE_STRCASECMP)
  check_function_exists(strchr         HAVE_STRCHR)
  check_function_exists(strdup         HAVE_STRDUP)
  check_function_exists(strncasecmp    HAVE_STRNCASECMP)
  check_function_exists(strstr         HAVE_STRSTR)
  check_function_exists(strtol         HAVE_STRTOL)
  check_function_exists(strtoll        HAVE_STRTOLL)
  check_function_exists(time           HAVE_TIME)
  check_function_exists(vprintf        HAVE_VPRINTF)
  check_function_exists(vsnprintf      HAVE_VSNPRINTF)

  set(CMAKE_EXTRA_INCLUDE_FILES)
  if (HAVE_ARGZ_H)
    list(APPEND CMAKE_EXTRA_INCLUDE_FILES "argz.h")
  endif()

  check_type_size("long" SIZEOF_LONG)
  check_type_size("long int" SIZEOF_LONG_INT)

  check_type_size("error_t" HAVE_ERROR_T)
  if (DEFINED HAVE_ERROR_T)
    set(HAVE_ERROR_T 1)
  endif()

  check_type_size("long long" HAVE_LONG_LONG)
  if (DEFINED HAVE_LONG_LONG)
    set(HAVE_LONG_LONG 1)
  endif()

  check_type_size("nl_langinfo" HAVE_LANGINFO_CODESET)
  if (DEFINED HAVE_LANGINFO_CODESET)
    set(HAVE_LANGINFO_CODESET 1)  # to improve
    set(HAVE_NL_LANGINFO 1)
  endif()

  check_type_size("ptrdiff_t" HAVE_PTRDIFF_T)
  if (DEFINED HAVE_PTRDIFF_T)
    set(HAVE_PTRDIFF_T 1)
  endif()

  configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/../Odbc/config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/AUTOGENERATED/config.h
    )

  configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/../Odbc/config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/AUTOGENERATED/unixodbc_conf.h
    )

  add_definitions(
    -DHAVE_CONFIG_H=1
    )

else()
  check_include_file("sqlext.h" HAVE_UNIX_ODBC_H)
  if (NOT HAVE_UNIX_ODBC_H)
    message(FATAL_ERROR "Please install the unixodbc-dev package")
  endif()

  check_include_file("libltdl/lt_dlloader.h"  HAVE_LT_DLLOADER_H)
  if (NOT HAVE_LT_DLLOADER_H)
    message(FATAL_ERROR "Please install the libltdl-dev package")
  endif()

  link_libraries(odbc ltdl)
endif()
