/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2021 Osimis S.A., Belgium
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

#include "../../Framework/Plugins/IndexBackend.h"
#include "../../Framework/MySQL/MySQLParameters.h"

namespace OrthancDatabases
{
  class MySQLIndex : public IndexBackend 
  {
  private:
    class Factory : public IDatabaseFactory
    {
    private:
      MySQLIndex&  that_;

    public:
      Factory(MySQLIndex& that) :
      that_(that)
      {
      }

      virtual Dialect GetDialect() const
      {
        return Dialect_MySQL;
      }

      virtual IDatabase* Open()
      {
        return that_.OpenInternal();
      }

      virtual void GetConnectionRetriesParameters(unsigned int& maxConnectionRetries, unsigned int& connectionRetryInterval)
      {
        maxConnectionRetries = that_.parameters_.GetMaxConnectionRetries();
        connectionRetryInterval = that_.parameters_.GetConnectionRetryInterval();
      }
    };

    MySQLParameters        parameters_;
    bool                   clearAll_;

    IDatabase* OpenInternal();

  public:
    MySQLIndex(OrthancPluginContext* context,
               const MySQLParameters& parameters);

    void SetClearAll(bool clear)
    {
      clearAll_ = clear;
    }

    virtual IDatabaseFactory* CreateDatabaseFactory() ORTHANC_OVERRIDE
    {
      return new Factory(*this);
    }

    virtual int64_t CreateResource(const char* publicId,
                                   OrthancPluginResourceType type)
      ORTHANC_OVERRIDE;

    virtual void DeleteResource(IDatabaseBackendOutput& output,
                                int64_t id) ORTHANC_OVERRIDE;

    virtual int64_t GetLastChangeIndex() ORTHANC_OVERRIDE;

    virtual bool HasCreateInstance() const  ORTHANC_OVERRIDE
    {
      return true;
    }

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void CreateInstance(OrthancPluginCreateInstanceResult& result,
                                const char* hashPatient,
                                const char* hashStudy,
                                const char* hashSeries,
                                const char* hashInstance)
      ORTHANC_OVERRIDE;
#endif
  };
}
