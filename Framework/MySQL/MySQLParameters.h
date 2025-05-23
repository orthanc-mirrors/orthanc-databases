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

#if ORTHANC_ENABLE_MYSQL != 1
#  error MySQL support must be enabled to use this file
#endif

#include "../../Resources/Orthanc/Plugins/OrthancPluginCppWrapper.h"

namespace OrthancDatabases
{
  class MySQLParameters
  {
  private:
    std::string  host_;
    std::string  username_;
    std::string  password_;
    std::string  database_;
    uint16_t     port_;
    std::string  unixSocket_;
    bool         ssl_;
    bool         verifySslServerCertificates_;
    std::string  sslCaCertificates_;
    bool         lock_;
    unsigned int maxConnectionRetries_;
    unsigned int connectionRetryInterval_;

    void Reset();
    
  public:
    MySQLParameters();

    MySQLParameters(const OrthancPlugins::OrthancConfiguration& pluginConfiguration,
                    const OrthancPlugins::OrthancConfiguration& orthancConfiguration);

    const std::string& GetHost() const
    {
      return host_;
    }

    const std::string& GetUsername() const
    {
      return username_;
    }

    const std::string& GetPassword() const
    {
      return password_;
    }

    const std::string& GetDatabase() const
    {
      return database_;
    }

    const std::string& GetUnixSocket() const
    {
      return unixSocket_;
    }

    uint16_t GetPort() const
    {
      return port_;
    }

    bool IsSsl() const
    {
      return ssl_;
    }

    bool IsVerifyServerCertificates() const
    {
      return verifySslServerCertificates_;
    }

    const char* GetSslCaCertificates() const
    {
      return sslCaCertificates_.c_str();
    }

    void SetHost(const std::string& host);
    
    void SetUsername(const std::string& username);

    void SetPassword(const std::string& password);

    void SetDatabase(const std::string& database);

    void SetPort(unsigned int port);

    void SetUnixSocket(const std::string& socket);

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

    void Format(Json::Value& target) const;
  };
}
