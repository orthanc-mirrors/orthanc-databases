/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2021-2023 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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

#if ORTHANC_ENABLE_POSTGRESQL != 1
#  error PostgreSQL support must be enabled to use this file
#endif

#include "../../Resources/Orthanc/Plugins/OrthancPluginCppWrapper.h"

namespace OrthancDatabases
{
  enum IsolationMode
  {
    IsolationMode_DbDefault = 0,
    IsolationMode_Serializable = 1,
    IsolationMode_ReadCommited = 2
  };

  class PostgreSQLParameters
  {
  private:
    std::string  host_;
    uint16_t     port_;
    std::string  username_;
    std::string  password_;
    std::string  database_;
    std::string  uri_;
    bool         ssl_;
    bool         lock_;
    unsigned int maxConnectionRetries_;
    unsigned int connectionRetryInterval_;
    bool         isVerboseEnabled_;
    IsolationMode isolationMode_;
    void Reset();

  public:
    PostgreSQLParameters();

    explicit PostgreSQLParameters(const OrthancPlugins::OrthancConfiguration& configuration);

    void SetConnectionUri(const std::string& uri);

    std::string GetConnectionUri() const;

    void SetHost(const std::string& host);

    const std::string& GetHost() const
    {
      return host_;
    }

    void SetPortNumber(unsigned int port);

    uint16_t GetPortNumber() const
    {
      return port_;
    }

    void SetUsername(const std::string& username);

    const std::string& GetUsername() const
    {
      return username_;
    }

    void SetPassword(const std::string& password);

    const std::string& GetPassword() const
    {
      return password_;
    }

    void SetDatabase(const std::string& database);

    void ResetDatabase()
    {
      SetDatabase("");
    }

    const std::string& GetDatabase() const
    {
      return database_;
    }

    void SetSsl(bool ssl)
    {
      ssl_ = ssl;
    }

    bool IsSsl() const
    {
      return ssl_;
    }

    void SetLock(bool lock)
    {
      lock_ = lock;
    }

    bool HasLock() const
    {
      return lock_;
    }

    unsigned int GetMaxConnectionRetries() const
    {
      return maxConnectionRetries_;
    }

    unsigned int GetConnectionRetryInterval() const
    {
      return connectionRetryInterval_;
    }

    void SetIsolationMode(IsolationMode isolationMode)
    {
      isolationMode_ = isolationMode;
    }

    const char* GetReadWriteTransactionStatement() const
    {
      switch (isolationMode_)
      {
        case IsolationMode_DbDefault:
          return "";
        case IsolationMode_ReadCommited:
          return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE";
        case IsolationMode_Serializable:
          return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE";
        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
      }
    }

    const char* GetReadOnlyTransactionStatement() const
    {
      switch (isolationMode_)
      {
        case IsolationMode_DbDefault:
          return "";
        case IsolationMode_ReadCommited:
          return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY";
        case IsolationMode_Serializable:
          return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY";
        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
      }
    }

    void SetVerboseEnabled(bool enabled)
    {
      isVerboseEnabled_ = enabled;
    }

    bool IsVerboseEnabled() const
    {
      return isVerboseEnabled_;
    }


    void Format(std::string& target) const;
  };
}
