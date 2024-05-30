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



include(${CMAKE_CURRENT_LIST_DIR}/DatabasesFrameworkConfiguration.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/../Orthanc/CMake/AutoGeneratedCode.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/../Orthanc/Plugins/OrthancPluginsExports.cmake)

if (STATIC_BUILD OR NOT USE_SYSTEM_ORTHANC_SDK)
  if (NOT ORTHANC_SDK_VERSION STREQUAL "framework")
    list(FIND ORTHANC_SDK_COMPATIBLE_VERSIONS ${ORTHANC_SDK_VERSION} tmp)
    if (tmp EQUAL -1)
      message(FATAL_ERROR "This database plugin is not compatible with Orthanc SDK ${ORTHANC_SDK_VERSION}")
    endif()
  endif()

  if (ORTHANC_SDK_VERSION STREQUAL "0.9.5")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-0.9.5)
  elseif (ORTHANC_SDK_VERSION STREQUAL "1.4.0")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-1.4.0)
  elseif (ORTHANC_SDK_VERSION STREQUAL "1.5.2")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-1.5.2)
  elseif (ORTHANC_SDK_VERSION STREQUAL "1.5.4")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-1.5.4)
  elseif (ORTHANC_SDK_VERSION STREQUAL "1.9.2")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-1.9.2)
  elseif (ORTHANC_SDK_VERSION STREQUAL "1.12.0")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-1.12.0)
  elseif (ORTHANC_SDK_VERSION STREQUAL "1.12.3")
    set(ORTHANC_SDK_ROOT ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Sdk-1.12.3)
  elseif (ORTHANC_SDK_VERSION STREQUAL "framework")
    set(tmp ${ORTHANC_FRAMEWORK_ROOT}/../../OrthancServer/Plugins/Include/)
    message(${tmp})
    if (NOT EXISTS ${tmp}/orthanc/OrthancCDatabasePlugin.h)
      message(FATAL_ERROR "Your copy of the Orthanc framework doesn't contain the Orthanc plugin SDK")
    endif()    
    set(ORTHANC_SDK_ROOT ${tmp})
  else()
    message(FATAL_ERROR "Unsupported version of the Orthanc plugin SDK: ${ORTHANC_SDK_VERSION}")
  endif()
else ()
  find_path(ORTHANC_SDK_ROOT orthanc/OrthancCDatabasePlugin.h
    /usr/include
    /usr/local/include
    )

  if (NOT ORTHANC_SDK_ROOT)
    message(FATAL_ERROR "Please install the headers of the Orthanc plugins SDK")
  endif()
  
  check_include_file(${ORTHANC_SDK_ROOT}/orthanc/OrthancCDatabasePlugin.h HAVE_ORTHANC_H)
  if (NOT HAVE_ORTHANC_H)
    message(FATAL_ERROR "Please install the headers of the Orthanc plugins SDK")
  endif()
endif()

include_directories(${ORTHANC_SDK_ROOT})


if (NOT DEFINED ORTHANC_OPTIMAL_VERSION_MAJOR)
  message(FATAL_ERROR "ORTHANC_OPTIMAL_VERSION_MAJOR is not defined")
endif()

if (NOT DEFINED ORTHANC_OPTIMAL_VERSION_MINOR)
  message(FATAL_ERROR "ORTHANC_OPTIMAL_VERSION_MINOR is not defined")
endif()

if (NOT DEFINED ORTHANC_OPTIMAL_VERSION_REVISION)
  message(FATAL_ERROR "ORTHANC_OPTIMAL_VERSION_REVISION is not defined")
endif()


add_definitions(
  -DHAS_ORTHANC_EXCEPTION=1
  -DORTHANC_BUILDING_SERVER_LIBRARY=0
  -DORTHANC_ENABLE_PLUGINS=1  # To build "DatabaseConstraint.h" imported from Orthanc core
  -DORTHANC_OPTIMAL_VERSION_MAJOR=${ORTHANC_OPTIMAL_VERSION_MAJOR}
  -DORTHANC_OPTIMAL_VERSION_MINOR=${ORTHANC_OPTIMAL_VERSION_MINOR}
  -DORTHANC_OPTIMAL_VERSION_REVISION=${ORTHANC_OPTIMAL_VERSION_REVISION}
  )


if ((STATIC_BUILD OR NOT USE_SYSTEM_PROTOBUF) AND
    CMAKE_SYSTEM_VERSION STREQUAL "LinuxStandardBase")
  # This is necessary, at least on LSB (Linux Standard Base),
  # otherwise the following error is generated: "undefined reference
  # to `__tls_get_addr'"
  add_definitions(-DGOOGLE_PROTOBUF_NO_THREADLOCAL=1)
endif()


list(APPEND DATABASES_SOURCES
  ${ORTHANC_CORE_SOURCES}
  ${ORTHANC_DATABASES_ROOT}/Framework/Plugins/DatabaseBackendAdapterV2.cpp
  ${ORTHANC_DATABASES_ROOT}/Framework/Plugins/DatabaseBackendAdapterV3.cpp
  ${ORTHANC_DATABASES_ROOT}/Framework/Plugins/DatabaseBackendAdapterV4.cpp
  ${ORTHANC_DATABASES_ROOT}/Framework/Plugins/IndexBackend.cpp
  ${ORTHANC_DATABASES_ROOT}/Framework/Plugins/IndexConnectionsPool.cpp
  ${ORTHANC_DATABASES_ROOT}/Framework/Plugins/StorageBackend.cpp
  ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Databases/DatabaseConstraint.cpp
  ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Databases/ISqlLookupFormatter.cpp
  ${ORTHANC_DATABASES_ROOT}/Resources/Orthanc/Plugins/OrthancPluginCppWrapper.cpp
  )
