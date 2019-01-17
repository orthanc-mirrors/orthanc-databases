# Orthanc - A Lightweight, RESTful DICOM Store
# Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
# Department, University Hospital of Liege, Belgium
# Copyright (C) 2017-2019 Osimis S.A., Belgium
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

INCLUDE(CheckTypeSize)
INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckFunctionExists)
INCLUDE(CheckStructHasMember)


if (STATIC_BUILD OR NOT USE_SYSTEM_LIBPQ)
  add_definitions(-DORTHANC_POSTGRESQL_STATIC=1)

  SET(LIBPQ_MAJOR 9)
  SET(LIBPQ_MINOR 6)
  SET(LIBPQ_REVISION 1)
  SET(LIBPQ_VERSION ${LIBPQ_MAJOR}.${LIBPQ_MINOR}.${LIBPQ_REVISION})

  SET(LIBPQ_SOURCES_DIR ${CMAKE_BINARY_DIR}/postgresql-${LIBPQ_VERSION})
  DownloadPackage(
    "eaa7e267e89ea1ed2693d2b88d3cd290"
    "http://orthanc.osimis.io/ThirdPartyDownloads/postgresql-${LIBPQ_VERSION}.tar.gz"
    "${LIBPQ_SOURCES_DIR}")

  
  ##
  ## Platform-specific configuration
  ##
  
  if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
    add_definitions(
      -DEXEC_BACKEND
      )

    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/port/win32.h
      ${AUTOGENERATED_DIR}/pg_config_os.h
      COPYONLY)

  elseif (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(
      -D_GNU_SOURCE
      -D_THREAD_SAFE
      -D_POSIX_PTHREAD_SEMANTICS
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
  
  if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/pg_config_ext.h.win32
      ${AUTOGENERATED_DIR}/pg_config_ext.h
      COPYONLY)

    configure_file(
      ${LIBPQ_SOURCES_DIR}/src/include/pg_config.h.win32
      ${AUTOGENERATED_DIR}/pg_config.h
      COPYONLY)

    if (CMAKE_COMPILER_IS_GNUCXX)  # MinGW
      add_definitions(
        -DPG_PRINTF_ATTRIBUTE=gnu_printf
        -DHAVE_GETTIMEOFDAY
        -DHAVE_LONG_LONG_INT_64
        -DHAVE_STRUCT_ADDRINFO
        -DHAVE_STRUCT_SOCKADDR_STORAGE
        -DHAVE_STRUCT_SOCKADDR_STORAGE_SS_FAMILY
        )
    endif()
    
    if (ENABLE_SSL)
      add_definitions(
        -DHAVE_LIBSSL=1
        -DUSE_OPENSSL=1
        )
    endif()

  elseif(CROSS_COMPILING)
    message(FATAL_ERROR "Cannot auto-generate the configuration file cross-compiling")
    
  else()
    configure_file(
      ${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/pg_config_ext.h
      ${AUTOGENERATED_DIR}/pg_config_ext.h
      COPYONLY
      )

    set(CMAKE_EXTRA_INCLUDE_FILES "sys/socket.h;netdb.h;sys/types.h")

    include(${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/func_accept_args.cmake)
    set(ACCEPT_TYPE_ARG3 ${ACCEPT_TYPE_ARG3})

    check_type_size("long int" SIZE_LONG_INT)
    if (SIZE_LONG_INT EQUAL 8)
      set(HAVE_LONG_INT_64 1)
    endif()

    check_type_size("long long int" SIZE_LONG_LONG_INT)
    if (SIZE_LONG_LONG_INT EQUAL 8)
      set(HAVE_LONG_LONG_INT_64 1)
    endif()

    file(READ ${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/c_flexmember.c SOURCE)
    check_c_source_compiles("${SOURCE}" c_flexmember)
    if (c_flexmember)
      set(FLEXIBLE_ARRAY_MEMBER "/**/")
    endif()

    if (CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR
        CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR
        CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
      set(PG_PRINTF_ATTRIBUTE "printf")
    else()
      file(READ ${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/printf_archetype.c SOURCE)
      check_c_source_compiles("${SOURCE}" printf_archetype)
      if (printf_archetype)
        set(PG_PRINTF_ATTRIBUTE "gnu_printf")
      else()
        set(PG_PRINTF_ATTRIBUTE "printf")
      endif()
    endif()

    check_function_exists("isinf" HAVE_ISINF)
    check_function_exists("getaddrinfo" HAVE_GETADDRINFO)
    check_function_exists("gettimeofday" HAVE_GETTIMEOFDAY)
    check_function_exists("snprintf" HAVE_DECL_SNPRINTF)
    check_function_exists("srandom" HAVE_SRANDOM)
    check_function_exists("strlcat" HAVE_DECL_STRLCAT)
    check_function_exists("strlcpy" HAVE_DECL_STRLCPY)
    check_function_exists("unsetenv" HAVE_UNSETENV)
    check_function_exists("vsnprintf" HAVE_DECL_VSNPRINTF)

    check_type_size("struct addrinfo" SIZE_STRUCT_ADDRINFO)
    if (HAVE_SIZE_STRUCT_ADDRINFO)
      set(HAVE_STRUCT_ADDRINFO 1)
    endif()

    check_type_size("struct sockaddr_storage" SIZE_STRUCT_SOCKADDR_STORAGE)
    if (HAVE_SIZE_STRUCT_SOCKADDR_STORAGE)
      set(HAVE_STRUCT_SOCKADDR_STORAGE 1)
    endif()

    set(MEMSET_LOOP_LIMIT 1024)            # This is hardcoded in "postgresql-9.6.1/configure"
    set(DEF_PGPORT 5432)                   # Default port number of PostgreSQL
    set(DEF_PGPORT_STR "\"5432\"")         # Same as above, as a string
    set(PG_VERSION "\"${LIBPQ_VERSION}\"") # Version of PostgreSQL, as a string

    # Version of PostgreSQL, as a number
    math(EXPR PG_VERSION_NUM "${LIBPQ_MAJOR} * 10000 + ${LIBPQ_MINOR} * 100 + ${LIBPQ_REVISION}")
    
    set(HAVE_STRUCT_SOCKADDR_STORAGE_SS_FAMILY 1)   # TODO Autodetection

    # Compute maximum alignment of any basic type.
    # We assume long's alignment is at least as strong as char, short, or int;
    # but we must check long long (if it exists) and double.
    check_type_size("long" SIZE_LONG)
    check_type_size("long long" SIZE_LONG_LONG)
    check_type_size("double" SIZE_DOUBLE)
    set(MAXIMUM_ALIGNOF ${SIZE_LONG})
    if(SIZE_LONG_LONG AND SIZE_LONG_LONG GREATER MAXIMUM_ALIGNOF)
      set(MAXIMUM_ALIGNOF ${SIZE_LONG_LONG})
    endif()
    if(SIZE_DOUBLE GREATER MAXIMUM_ALIGNOF)
      set(MAXIMUM_ALIGNOF ${SIZE_DOUBLE})
    endif()

    check_include_file("poll.h" HAVE_POLL_H)
    check_include_file("net/if.h" HAVE_NET_IF_H)
    check_include_file("netinet/in.h" HAVE_NETINET_IN_H)
    check_include_file("netinet/tcp.h" HAVE_NETINET_TCP_H)
    check_include_file("sys/ioctl.h" HAVE_SYS_IOCTL_H)
    check_include_file("sys/un.h" HAVE_SYS_UN_H)

    If (NOT HAVE_NET_IF_H)  # This is the case of OpenBSD
      unset(HAVE_NET_IF_H CACHE)
      check_include_files("sys/socket.h;net/if.h" HAVE_NET_IF_H)
    endif()

    if (NOT HAVE_NETINET_TCP_H)  # This is the case of OpenBSD
      unset(HAVE_NETINET_TCP_H CACHE)
      check_include_files("sys/socket.h;netinet/tcp.h" HAVE_NETINET_TCP_H)
    endif()

    if (ENABLE_SSL)
      set(HAVE_LIBSSL 1)
      set(HAVE_SSL_GET_CURRENT_COMPRESSION 1)
      set(USE_OPENSSL 1)
    endif()

    execute_process(
      COMMAND 
      ${PYTHON_EXECUTABLE}
      "${CMAKE_CURRENT_LIST_DIR}/../PostgreSQL/PrepareCMakeConfigurationFile.py"
      "${LIBPQ_SOURCES_DIR}/src/include/pg_config.h.in"
      "${AUTOGENERATED_DIR}/pg_config.h.in"
      ERROR_VARIABLE NO_PG_CONFIG
      OUTPUT_VARIABLE out
      )

    if (NO_PG_CONFIG)
      message(FATAL_ERROR "Cannot find pg_config.h.in")
    endif()
    
    configure_file(
      ${AUTOGENERATED_DIR}/pg_config.h.in
      ${AUTOGENERATED_DIR}/pg_config.h)
  endif()



  ##
  ## Generic configuration
  ##

  file(WRITE
    ${AUTOGENERATED_DIR}/pg_config_paths.h
    "")

  add_definitions(
    -D_REENTRANT
    -DFRONTEND
    -DUNSAFE_STAT_OK
    -DSYSCONFDIR=""
    )

  include_directories(
    ${LIBPQ_SOURCES_DIR}/src/include
    ${LIBPQ_SOURCES_DIR}/src/include/libpq
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq
    )

  set(LIBPQ_SOURCES
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-auth.c 
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-connect.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-exec.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-lobj.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-misc.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-print.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-protocol2.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-protocol3.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-secure.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/libpq-events.c
    ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/pqexpbuffer.c

    # libpgport C files we always use
    ${LIBPQ_SOURCES_DIR}/src/port/chklocale.c
    ${LIBPQ_SOURCES_DIR}/src/port/inet_net_ntop.c
    ${LIBPQ_SOURCES_DIR}/src/port/noblock.c
    ${LIBPQ_SOURCES_DIR}/src/port/pgstrcasecmp.c
    ${LIBPQ_SOURCES_DIR}/src/port/pqsignal.c
    ${LIBPQ_SOURCES_DIR}/src/port/thread.c

    ${LIBPQ_SOURCES_DIR}/src/backend/libpq/ip.c
    ${LIBPQ_SOURCES_DIR}/src/backend/libpq/md5.c
    ${LIBPQ_SOURCES_DIR}/src/backend/utils/mb/encnames.c
    ${LIBPQ_SOURCES_DIR}/src/backend/utils/mb/wchar.c
    )

  if (ENABLE_SSL)
    list(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/fe-secure-openssl.c
      )
  endif()


  if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    LIST(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/port/strlcpy.c
      )      

  elseif (CMAKE_SYSTEM_NAME STREQUAL "Windows")
    link_libraries(secur32)
    
    include_directories(
      ${LIBPQ_SOURCES_DIR}/src/include/port/win32
      ${LIBPQ_SOURCES_DIR}/src/port
      )
    
    LIST(APPEND LIBPQ_SOURCES
      # libpgport C files that are needed if identified by configure
      ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/win32.c
      ${LIBPQ_SOURCES_DIR}/src/port/crypt.c 
      ${LIBPQ_SOURCES_DIR}/src/port/inet_aton.c
      ${LIBPQ_SOURCES_DIR}/src/port/open.c
      ${LIBPQ_SOURCES_DIR}/src/port/pgsleep.c
      ${LIBPQ_SOURCES_DIR}/src/port/snprintf.c
      ${LIBPQ_SOURCES_DIR}/src/port/system.c 
      ${LIBPQ_SOURCES_DIR}/src/port/win32setlocale.c
      ${LIBPQ_SOURCES_DIR}/src/port/getaddrinfo.c
      ${LIBPQ_SOURCES_DIR}/src/port/strlcpy.c
      )
      
    if (CMAKE_COMPILER_IS_GNUCXX OR 
        (MSVC AND MSVC_VERSION GREATER 1800))
      # Starting Visual Studio 2013 (version 1800), it is necessary to also add "win32error.c"
      LIST(APPEND LIBPQ_SOURCES ${LIBPQ_SOURCES_DIR}/src/port/win32error.c)
    endif()

    if (MSVC)
      LIST(APPEND LIBPQ_SOURCES ${LIBPQ_SOURCES_DIR}/src/interfaces/libpq/pthread-win32.c)
    endif()
  endif()

  if (CMAKE_COMPILER_IS_GNUCXX AND
      NOT CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    LIST(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/port/getpeereid.c
      )

  elseif (MSVC)
    include_directories(
      ${LIBPQ_SOURCES_DIR}/src/include/port/win32_msvc
      )
    
    LIST(APPEND LIBPQ_SOURCES
      ${LIBPQ_SOURCES_DIR}/src/port/dirent.c 
      ${LIBPQ_SOURCES_DIR}/src/port/dirmod.c 
      )
  endif()

  source_group(ThirdParty\\PostgreSQL REGULAR_EXPRESSION ${LIBPQ_SOURCES_DIR}/.*)

else()
  include(${CMAKE_CURRENT_LIST_DIR}/FindPostgreSQL.cmake)
  include_directories(
    ${PostgreSQL_INCLUDE_DIR}
    ${PostgreSQL_TYPE_INCLUDE_DIR}
    )
  link_libraries(${PostgreSQL_LIBRARY})
endif()
