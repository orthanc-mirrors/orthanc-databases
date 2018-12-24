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

#include "../Common/DatabaseManager.h"

#include <OrthancServer/ServerEnumerations.h>

namespace OrthancDatabases
{
  bool LookupGlobalProperty(std::string& target /* out */,
                            IDatabase& db,
                            ITransaction& transaction,
                            Orthanc::GlobalProperty property);

  bool LookupGlobalProperty(std::string& target /* out */,
                            DatabaseManager& manager,
                            Orthanc::GlobalProperty property);

  void SetGlobalProperty(IDatabase& db,
                         ITransaction& transaction,
                         Orthanc::GlobalProperty property,
                         const std::string& utf8);

  void SetGlobalProperty(DatabaseManager& manager,
                         Orthanc::GlobalProperty property,
                         const std::string& utf8);

  bool LookupGlobalIntegerProperty(int& target,
                                   IDatabase& db,
                                   ITransaction& transaction,
                                   Orthanc::GlobalProperty property);

  void SetGlobalIntegerProperty(IDatabase& db,
                                ITransaction& transaction,
                                Orthanc::GlobalProperty property,
                                int value);
}
