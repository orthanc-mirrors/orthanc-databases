/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2020 Osimis S.A., Belgium
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 **/


#pragma once

// These includes are necessary for compilation on OS X
#include <unistd.h>
#include <vector>
#include <map>
#include <cmath>
#include <Enumerations.h>

/**
 * This include must be before including "c.h" from PostgreSQL,
 * otherwise the function "static bool
 * boost::date_time::special_values_parser<date_type,
 * charT>::__builtin_expect()" from Boost clashes with macro
 * "__builtin_expect()" used by PostgreSQL 11.
 **/
#include <boost/date_time/posix_time/posix_time.hpp>

// PostgreSQL includes
#include <pg_config.h>

#if PG_VERSION_NUM >= 110000
#  include <postgres.h>
#  undef LOG  // This one comes from <postgres.h>, and conflicts with <Logging.h>
#endif

#include <libpq-fe.h>
#include <c.h>
#include <catalog/pg_type.h>
