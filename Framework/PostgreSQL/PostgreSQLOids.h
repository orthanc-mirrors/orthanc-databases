/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2024-2025 Orthanc Team SRL, Belgium
 * Copyright (C) 2021-2025 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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

#if PG_VERSION_NUM < 180000
#  error This file shall not be included if you are linking against libpq < 18
#endif
// Object ID type in PostgreSQL
typedef unsigned int Oid;

// Core built-in type OIDs.  
// All these OIDs are guaranteed not to change.
// By defining them here, we avoid including server only headers
#define BOOLOID        16
#define BYTEAOID       17
#define CHAROID        18
#define NAMEOID        19
#define INT8OID        20
#define INT2OID        21
#define INT4OID        23
#define TEXTOID        25
#define OIDOID         26
#define VARCHAROID     1043
#define TIMESTAMPOID   1114
#define TIMESTAMPTZOID 1184
#define VOIDOID        2278