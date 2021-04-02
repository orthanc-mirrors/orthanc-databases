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

  public:
    class IFileContentVisitor : public boost::noncopyable
    {
    public:
      virtual ~IFileContentVisitor()
      {
      }

      virtual void Assign(const std::string& content) = 0;
    };
    
    virtual ~StorageBackend()
    {
    }

    void SetDatabase(IDatabase* database);  // Takes ownership

    DatabaseManager& GetManager();
    
    // NB: These methods will always be invoked in mutual exclusion,
    // as having access to some "DatabaseManager::Transaction" implies
    // that the parent "DatabaseManager" is locked
    virtual void Create(DatabaseManager::Transaction& transaction,
                        const std::string& uuid,
                        const void* content,
                        size_t size,
                        OrthancPluginContentType type) ORTHANC_FINAL;

    virtual void Read(IFileContentVisitor& target,
                      DatabaseManager::Transaction& transaction, 
                      const std::string& uuid,
                      OrthancPluginContentType type) ORTHANC_FINAL;

    virtual void Remove(DatabaseManager::Transaction& transaction,
                        const std::string& uuid,
                        OrthancPluginContentType type) ORTHANC_FINAL;

    static void Register(OrthancPluginContext* context,
                         StorageBackend* backend);   // Takes ownership

    static void Finalize();

    // For unit tests
    void ReadToString(std::string& target,
                      DatabaseManager::Transaction& transaction, 
                      const std::string& uuid,
                      OrthancPluginContentType type);
  };
}
