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

#include "../Common/DatabaseManager.h"

#include <orthanc/OrthancCDatabasePlugin.h>


namespace OrthancDatabases
{
  class StorageBackend : public boost::noncopyable
  {
  private:
    std::unique_ptr<DatabaseManager>   manager_;

    DatabaseManager& GetManager();
    
  protected:
    void SetDatabase(IDatabase* database);  // Takes ownership

  public:
    class IFileContentVisitor : public boost::noncopyable
    {
    public:
      virtual ~IFileContentVisitor()
      {
      }

      virtual void Assign(const std::string& content) = 0;

      virtual bool IsSuccess() const = 0;
    };

    class Accessor : public boost::noncopyable
    {
    private:
      DatabaseManager&  manager_;

    public:
      Accessor(StorageBackend& backend) :
        manager_(backend.GetManager())
      {
      }
        
      void Create(const std::string& uuid,
                  const void* content,
                  size_t size,
                  OrthancPluginContentType type);

      void Read(IFileContentVisitor& visitor,
                const std::string& uuid,
                OrthancPluginContentType type);

      void Remove(const std::string& uuid,
                  OrthancPluginContentType type);

      // For unit tests
      void ReadToString(std::string& target,
                        const std::string& uuid,
                        OrthancPluginContentType type);
    };
    
    virtual ~StorageBackend()
    {
    }

    static void Register(OrthancPluginContext* context,
                         StorageBackend* backend);   // Takes ownership

    static void Finalize();
  };
}
