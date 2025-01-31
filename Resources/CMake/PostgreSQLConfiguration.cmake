# Orthanc - A Lightweight, RESTful DICOM Store
# Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
# Department, University Hospital of Liege, Belgium
# Copyright (C) 2017-2023 Osimis S.A., Belgium
# Copyright (C) 2024-2025 Orthanc Team SRL, Belgium
# Copyright (C) 2021-2025 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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


#####################################################################
## PostgreSQL
#####################################################################

INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckFunctionExists)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckStructHasMember)
INCLUDE(CheckTypeSize)


macro(PrepareCMakeConfigurationFile Source Target)
  execute_process(
    COMMAND 
    ${PYTHON_EXECUTABLE}
    "${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/PrepareCMakeConfigurationFile.py" "${Source}" "${Target}"
    ERROR_VARIABLE tmp
    OUTPUT_VARIABLE out
    )

  if (tmp)
    message(FATAL_ERROR "Cannot find ${Source}")
  endif()
endmacro()


if (STATIC_BUILD OR NOT USE_SYSTEM_LIBPQ)
  add_definitions(-DORTHANC_POSTGRESQL_STATIC=1)

  SET(LIBPQ_MAJOR 13)
  SET(LIBPQ_MINOR 1)
  SET(LIBPQ_VERSION ${LIBPQ_MAJOR}.${LIBPQ_MINOR})

  SET(LIBPQ_SOURCES_DIR ${CMAKE_BINARY_DIR}/postgresql-${LIBPQ_VERSION})
  DownloadPackage(
    "551302a823a1ab48b4ed14166beebba9"
    "https://orthanc.uclouvain.be/downloads/third-party-downloads/postgresql-${LIBPQ_VERSION}.tar.gz"
    "${LIBPQ_SOURCES_DIR}")

  
  ##
  ## Platform-specific configuration
  ##
  
  if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
    if (EXISTS ${AUTOGENERATED_DIR}/pg_config_os.h)
      set(FirstRun OFF)
    else()
      set(FirstRun ON)
    endif()

    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/port/win32.h
      ${AUTOGENERATED_DIR}/pg_config_os.h
      COPYONLY)

    if (FirstRun)
      # This is needed on MSVC2008 to have an implementation of "isinf()" and "isnan()"
      file(APPEND ${AUTOGENERATED_DIR}/pg_config_os.h "
#if defined(_MSC_VER)
#  include <math.h>
#  include <float.h>
#  if !defined(isinf)
#    define isinf(d) ((_fpclass(d) == _FPCLASS_PINF) ? 1 : ((_fpclass(d) == _FPCLASS_NINF) ? -1 : 0))
#  endif
#  if !defined(isnan)
#    define isnan(d) (_isnan(d))
#  endif
#endif
")
    endif()

  elseif (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(
      -D_GNU_SOURCE
      )

    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/port/linux.h
      ${AUTOGENERATED_DIR}/pg_config_os.h
      COPYONLY)

  elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions(
      -D_GNU_SOURCE
      -D_THREAD_SAFE
      -D_POSIX_PTHREAD_SEMANTICS
      )

    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/port/darwin.h
      ${AUTOGENERATED_DIR}/pg_config_os.h
      COPYONLY)

  elseif (CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/port/openbsd.h
      ${AUTOGENERATED_DIR}/pg_config_os.h
      COPYONLY)

  elseif (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/port/freebsd.h
      ${AUTOGENERATED_DIR}/pg_config_os.h
      COPYONLY)

  else()
    message(FATAL_ERROR "Support your platform here")
  endif()


  ##
  ## Generation of "pg_config.h"
  ## 
  
  set(PG_VERSION "\"${LIBPQ_MAJOR}.${LIBPQ_MINOR}\"")
  math(EXPR PG_VERSION_NUM "${LIBPQ_MAJOR} * 10000 + ${LIBPQ_MINOR}")

  include(${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/func_accept_args.cmake)
  include(${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/CheckTypeAlignment.cmake)

  check_include_file("execinfo.h"           HAVE_EXECINFO_H) 
  check_include_file("getopt.h"             HAVE_GETOPT_H) 
  check_include_file("ifaddrs.h"            HAVE_IFADDRS_H) 
  check_include_file("inttypes.h"           HAVE_INTTYPES_H) 
  check_include_file("langinfo.h"           HAVE_LANGINFO_H)
  check_include_file("memory.h"             HAVE_MEMORY_H) 
  check_include_file("netinet/tcp.h"        HAVE_NETINET_TCP_H)
  check_include_file("readline/history.h"   HAVE_READLINE_HISTORY_H)
  check_include_file("readline/readline.h"  HAVE_READLINE_READLINE_H)
  check_include_file("stdbool.h"            HAVE_STDBOOL_H)
  check_include_file("stdlib.h"             HAVE_STDLIB_H)
  check_include_file("string.h"             HAVE_STRING_H)
  check_include_file("strings.h"            HAVE_STRINGS_H)
  check_include_file("sys/epoll.h"          HAVE_SYS_EPOLL_H)
  check_include_file("sys/event.h"          HAVE_SYS_EVENT_H)
  check_include_file("sys/ipc.h"            HAVE_SYS_IPC_H)
  check_include_file("sys/prctl.h"          HAVE_SYS_PRCTL_H)
  check_include_file("sys/resource.h"       HAVE_SYS_RESOURCE_H)
  check_include_file("sys/select.h"         HAVE_SYS_SELECT_H)
  check_include_file("sys/sem.h"            HAVE_SYS_SEM_H)
  check_include_file("sys/shm.h"            HAVE_SYS_SHM_H)
  check_include_file("sys/stat.h"           HAVE_SYS_STAT_H)
  check_include_file("sys/termios.h"        HAVE_SYS_TERMIOS_H)
  check_include_file("sys/types.h"          HAVE_SYS_TYPES_H)
  check_include_file("sys/un.h"             HAVE_SYS_UN_H)
  check_include_file("termios.h"            HAVE_TERMIOS_H)
  check_include_file("unistd.h"             HAVE_UNISTD_H)
  check_include_file("wctype.h"             HAVE_WCTYPE_H)

  check_type_size("long long int" SIZEOF_LONG_LONG_INT)
  if (SIZEOF_LONG_LONG_INT EQUAL 8)
    set(HAVE_LONG_LONG_INT_64 1)
    set(PG_INT64_TYPE "long long int")
  endif()

  check_type_size("long int" SIZEOF_LONG_INT)
  if (SIZEOF_LONG_INT EQUAL 8)
    set(HAVE_LONG_INT_64 1)
    set(PG_INT64_TYPE "long int")
  endif()
  
  if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    set(ALIGNOF_DOUBLE 8)
    set(ALIGNOF_INT 4)
    set(ALIGNOF_LONG 4)
    set(ALIGNOF_LONG_LONG_INT 8)
    set(ALIGNOF_SHORT 2)
  else()
    check_type_alignment(double ALIGNOF_DOUBLE)
    check_type_alignment(int ALIGNOF_INT)
    check_type_alignment(long ALIGNOF_LONG)
    check_type_alignment("long long int" ALIGNOF_LONG_LONG_INT)
    check_type_alignment(short ALIGNOF_SHORT)
  endif()
  
  set(MAXIMUM_ALIGNOF ${ALIGNOF_LONG})
  if (MAXIMUM_ALIGNOF LESS ALIGNOF_DOUBLE)
    set(MAXIMUM_ALIGNOF ${ALIGNOF_DOUBLE})
  endif()
  if (HAVE_LONG_LONG_INT_64 AND (MAXIMUM_ALIGNOF LESS HAVE_LONG_LONG_INT_64))
    set(MAXIMUM_ALIGNOF ${HAVE_LONG_LONG_INT_64})
  endif()
  
  if (CMAKE_COMPILER_IS_GNUCXX OR
      "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(PG_PRINTF_ATTRIBUTE "gnu_printf")
    set(pg_restrict "__restrict")
    set(restrict "__restrict")
  else()
    # The empty string below wouldn't work (it would do an #undef)
    set(pg_restrict " ")
    set(restrict " ")
  endif()

  if (MSVC)
    set(inline "__inline")
  endif()
  
  if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    set(CMAKE_EXTRA_INCLUDE_FILES "sys/types.h;winsock2.h;ws2tcpip.h;float.h;math.h")
  else()
    set(CMAKE_EXTRA_INCLUDE_FILES "sys/types.h;sys/socket.h;netdb.h;float.h;math.h")
  endif()

  check_type_size("size_t" SIZEOF_SIZE_T)
  check_type_size("struct addrinfo" HAVE_STRUCT_ADDRINFO)    
  check_type_size("struct sockaddr_storage" HAVE_STRUCT_SOCKADDR_STORAGE)
  check_struct_has_member("struct sockaddr_storage" ss_family
    "${CMAKE_EXTRA_INCLUDE_FILES}" HAVE_STRUCT_SOCKADDR_STORAGE_SS_FAMILY)

  check_function_exists(gethostbyname_r HAVE_GETHOSTBYNAME_R)
  check_function_exists(getopt HAVE_GETOPT)
  check_function_exists(getopt_long HAVE_GETOPT_LONG)
  check_function_exists(getpwuid_r HAVE_GETPWUID_R)
  check_function_exists(gettimeofday HAVE_GETTIMEOFDAY)
  check_function_exists(random HAVE_RANDOM)
  check_function_exists(srandom HAVE_SRANDOM)
  check_function_exists(strchrnul HAVE_STRCHRNUL)
  check_function_exists(strerror HAVE_STRERROR)
  check_function_exists(strerror_r HAVE_STRERROR_R)
  check_function_exists(unsetenv HAVE_UNSETENV)
  check_function_exists(strlcat HAVE_STRLCAT)
  check_function_exists(strlcpy HAVE_STRLCPY)  
  check_function_exists(getpeereid HAVE_GETPEEREID)
  check_function_exists(getpeerucred HAVE_GETPEERUCRED)
  check_function_exists(isinf HAVE_ISINF)
  check_function_exists(isnan HAVE_ISNAN)

  check_symbol_exists(strlcpy "stdio.h;string.h" HAVE_DECL_STRLCPY)
  if (NOT HAVE_DECL_STRLCPY)
    set(HAVE_DECL_STRLCPY 0)
  endif()
  check_symbol_exists(strlcat "stdio.h;string.h" HAVE_DECL_STRLCAT)
  if (NOT HAVE_DECL_STRLCAT)
    set(HAVE_DECL_STRLCAT 0)
  endif()
  check_symbol_exists(snprintf "stdio.h;string.h" HAVE_DECL_SNPRINTF)
  if (NOT HAVE_DECL_SNPRINTF)
    set(HAVE_DECL_SNPRINTF 0)
  endif()
  check_symbol_exists(vsnprintf "stdio.h;string.h" HAVE_DECL_VSNPRINTF)
  if (NOT HAVE_DECL_VSNPRINTF)
    set(HAVE_DECL_VSNPRINTF 0)
  endif()
  
  check_c_source_compiles("
	#include <sys/time.h>
	int main(void){
		struct timeval *tp;
		struct timezone *tzp;
		gettimeofday(tp,tzp);
		return 0;
	}
  " GETTIMEOFDAY_2ARG)

  if(NOT GETTIMEOFDAY_2ARG)
    set(GETTIMEOFDAY_1ARG 1)
  endif(NOT GETTIMEOFDAY_2ARG)
  
  check_c_source_compiles("
	#include <time.h>
	int main(void){
		int res;
	#ifndef __CYGWIN__
		res = timezone / 60;
	#else
		res = _timezone / 60;
	#endif
		return 0;
	}
  " HAVE_INT_TIMEZONE)
  
  # Hardcoded stuff from "./configure" of libpq
  set(MEMSET_LOOP_LIMIT 1024)
  set(BLCKSZ 8192)
  set(XLOG_BLCKSZ 8192)
  set(DEF_PGPORT 5432)
  set(DEF_PGPORT_STR "\"${DEF_PGPORT}\"")
  set(PG_KRB_SRVNAM "\"postgres\"")

  # Assume that zlib and openssl are always present
  set(HAVE_LIBZ 1)
  set(HAVE_LIBSSL 1)
  set(HAVE_OPENSSL_INIT_SSL 1)
  set(USE_OPENSSL 1)
  set(USE_OPENSSL_RANDOM 1)
  
  PrepareCMakeConfigurationFile(
    ${LIBPQ_SOURCES_DIR}/src/include/pg_config_ext.h.in
    ${AUTOGENERATED_DIR}/pg_config_ext.h.in)
  
  PrepareCMakeConfigurationFile(
    ${LIBPQ_SOURCES_DIR}/src/include/pg_config.h.in
    ${AUTOGENERATED_DIR}/pg_config.h.in)
  
  configure_file(
    ${AUTOGENERATED_DIR}/pg_config_ext.h.in
    ${AUTOGENERATED_DIR}/pg_config_ext.h)

  configure_file(
    ${AUTOGENERATED_DIR}/pg_config.h.in
    ${AUTOGENERATED_DIR}/pg_config.h)



  ##
  ## Generic configuration
  ##

  file(WRITE
    ${AUTOGENERATED_DIR}/pg_config_paths.h
    "")

  add_definitions(
    -DFRONTEND
    -DSYSCONFDIR=""
    -DTCP_NODELAY    # For performance

    # Must be set for OpenSSL 1.1, not for OpenSSL 1.0??
    -DHAVE_BIO_GET_DATA=1
    -DHAVE_BIO_METH_NEW=1
    )

  include_directories(
    ${LIBPQ_SOURCES_DIR}/src/backend
    ${LIBPQ_SOURCES_DIR}/src/include
    ${LIBPQ_SOURCES_DIR}/src/include/libpq
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq
    )

  set(LIBPQ_SOURCES
    # Don't use files from the "src/backend/" folder
    ${LIBPQ_SOURCES_DIR}/src/common/base64.c
    ${LIBPQ_SOURCES_DIR}/src/common/encnames.c
    ${LIBPQ_SOURCES_DIR}/src/common/ip.c
    ${LIBPQ_SOURCES_DIR}/src/common/link-canary.c
    ${LIBPQ_SOURCES_DIR}/src/common/md5.c
    ${LIBPQ_SOURCES_DIR}/src/common/saslprep.c
    ${LIBPQ_SOURCES_DIR}/src/common/scram-common.c
    ${LIBPQ_SOURCES_DIR}/src/common/sha2_openssl.c
    ${LIBPQ_SOURCES_DIR}/src/common/string.c
    ${LIBPQ_SOURCES_DIR}/src/common/unicode_norm.c
    ${LIBPQ_SOURCES_DIR}/src/common/wchar.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-auth-scram.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-auth.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-connect.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-exec.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-lobj.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-misc.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-print.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-protocol2.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-protocol3.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-secure-common.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-secure-openssl.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-secure.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/libpq-events.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/pqexpbuffer.c
    ${LIBPQ_SOURCES_DIR}/src/port/chklocale.c
    ${LIBPQ_SOURCES_DIR}/src/port/explicit_bzero.c
    ${LIBPQ_SOURCES_DIR}/src/port/getaddrinfo.c
    ${LIBPQ_SOURCES_DIR}/src/port/inet_net_ntop.c
    ${LIBPQ_SOURCES_DIR}/src/port/noblock.c
    ${LIBPQ_SOURCES_DIR}/src/port/pg_strong_random.c
    ${LIBPQ_SOURCES_DIR}/src/port/pgstrcasecmp.c
    ${LIBPQ_SOURCES_DIR}/src/port/pqsignal.c
    ${LIBPQ_SOURCES_DIR}/src/port/snprintf.c
    ${LIBPQ_SOURCES_DIR}/src/port/strerror.c
    ${LIBPQ_SOURCES_DIR}/src/port/thread.c
    )

  if (NOT HAVE_STRLCPY)
    LIST(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/port/strlcpy.c  # Doesn't work on OS X
      )
  endif()

  if (NOT HAVE_GETPEEREID)
    LIST(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/port/getpeereid.c  # Doesn't work on OS X
      )
  endif()

  if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
    link_libraries(secur32)
    
    include_directories(
      ${LIBPQ_SOURCES_DIR}/src/include/port/win32
      ${LIBPQ_SOURCES_DIR}/src/port
      )
    
    LIST(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/pthread-win32.c
      ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/win32.c
      ${LIBPQ_SOURCES_DIR}/src/port/dirmod.c
      ${LIBPQ_SOURCES_DIR}/src/port/inet_aton.c
      ${LIBPQ_SOURCES_DIR}/src/port/open.c
      ${LIBPQ_SOURCES_DIR}/src/port/pgsleep.c
      ${LIBPQ_SOURCES_DIR}/src/port/system.c
      ${LIBPQ_SOURCES_DIR}/src/port/win32setlocale.c
      )

    if (CMAKE_COMPILER_IS_GNUCXX OR 
        (MSVC AND MSVC_VERSION GREATER 1800))
      # Starting Visual Studio 2013 (version 1800), it is necessary to also add "win32error.c"
      LIST(APPEND LIBPQ_SOURCES ${LIBPQ_SOURCES_DIR}/src/port/win32error.c)
    endif()

    if (MSVC)
      include_directories(
        ${LIBPQ_SOURCES_DIR}/src/include/port/win32_msvc
        )
      
      LIST(APPEND LIBPQ_SOURCES
        ${LIBPQ_SOURCES_DIR}/src/port/dirent.c 
        )
    endif()
  endif()

  source_group(ThirdParty\\PostgreSQL REGULAR_EXPRESSION ${LIBPQ_SOURCES_DIR}/.*)

else()
  set(PostgreSQL_ADDITIONAL_VERSIONS
    "17" "16" "15" "14" "13" "12" "11" "10" "9.6" "9.5" "9.4" "9.3" "9.2" "9.1" "9.0" "8.4" "8.3" "8.2" "8.1" "8.0")
  if (NOT WIN32)
    foreach (suffix ${PostgreSQL_ADDITIONAL_VERSIONS})
      list(APPEND PostgreSQL_ADDITIONAL_SEARCH_PATHS
        "/usr/include/postgresql/${suffix}"
        "/usr/include/postgresql/${suffix}/server"
        "/usr/local/include/postgresql/${suffix}"
        )
    endforeach()
  endif()

  include(FindPostgreSQL)
  include_directories(
    ${PostgreSQL_INCLUDE_DIR}
    ${PostgreSQL_TYPE_INCLUDE_DIR}
    )
  link_libraries(${PostgreSQL_LIBRARY})
endif()
