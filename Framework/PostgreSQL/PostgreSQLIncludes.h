/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2019 Osimis S.A., Belgium
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
#include <Core/Enumerations.h>

// PostgreSQL includes
#include <pg_config.h>

#if PG_VERSION_NUM >= 110000
#  include <postgres.h>
#  undef LOG  // This one comes from <postgres.h>, and conflicts with <Core/Logging.h>
#endif

#include <libpq-fe.h>
#include <c.h>
#include <catalog/pg_type.h>