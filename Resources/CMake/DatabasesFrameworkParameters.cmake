# Orthanc - A Lightweight, RESTful DICOM Store
# Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
# Department, University Hospital of Liege, Belgium
# Copyright (C) 2017-2021 Osimis S.A., Belgium
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
## Import the parameters of the Orthanc Framework
#####################################################################

include(${CMAKE_CURRENT_LIST_DIR}/../Orthanc/CMake/DownloadOrthancFramework.cmake)

if (NOT ORTHANC_FRAMEWORK_SOURCE STREQUAL "system")
  include(${ORTHANC_FRAMEWORK_ROOT}/../Resources/CMake/OrthancFrameworkParameters.cmake)
endif()


#####################################################################
## CMake parameters tunable by the user
#####################################################################

set(USE_SYSTEM_LIBPQ ON CACHE BOOL "Use the system version of the PostgreSQL client library")
set(USE_SYSTEM_MYSQL_CLIENT ON CACHE BOOL "Use the system version of the MySQL client library")


#####################################################################
## Internal CMake parameters to enable the optional subcomponents of
## the database engines
#####################################################################

set(ENABLE_MYSQL_BACKEND OFF)
set(ENABLE_POSTGRESQL_BACKEND OFF)
set(ENABLE_SQLITE_BACKEND OFF)
