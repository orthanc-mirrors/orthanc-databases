/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2024-2024 Orthanc Team SRL, Belgium
 * Copyright (C) 2021-2024 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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

#include "OdbcStatement.h"

#include "../Common/IResult.h"

#include <Compatibility.h>

#include <sqltypes.h>
#include <vector>


namespace OrthancDatabases
{
  class OdbcResult : public IResult
  {
  private:
    OdbcStatement&            statement_;
    Dialect                   dialect_;
    bool                      first_;
    bool                      done_;
    std::vector<SQLLEN>       types_;
    std::vector<std::string>  typeNames_;
    std::vector<IValue*>      values_;

    void LoadFirst();

    void SetValue(size_t index,
                  IValue* value);

    void ReadString(std::string& target,
                    size_t column,
                    bool isBinary);
    
  public:
    OdbcResult(OdbcStatement& statement,
               Dialect dialect);
    
    virtual ~OdbcResult();
      
    virtual void SetExpectedType(size_t field,
                                 ValueType type) ORTHANC_OVERRIDE;

    virtual bool IsDone() const ORTHANC_OVERRIDE;

    virtual void Next() ORTHANC_OVERRIDE;
    
    virtual size_t GetFieldsCount() const ORTHANC_OVERRIDE
    {
      return values_.size();
    }        

    virtual const IValue& GetField(size_t field) const ORTHANC_OVERRIDE;
  };
}
