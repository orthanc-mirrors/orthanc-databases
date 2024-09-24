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

#include "IdentifierTag.h"
#include "IndexBackend.h"

#include <MultiThreading/SharedMessageQueue.h>

#include <list>

namespace OrthancDatabases
{
  class IndexConnectionsPool : public boost::noncopyable
  {
  private:
    class ManagerReference;

    std::unique_ptr<IndexBackend>  backend_;
    OrthancPluginContext*          context_;
    boost::shared_mutex            connectionsMutex_;
    size_t                         countConnections_;
    std::list<DatabaseManager*>    connections_;
    Orthanc::SharedMessageQueue    availableConnections_;

  public:
    IndexConnectionsPool(IndexBackend* backend /* takes ownership */,
                         size_t countConnections);

    ~IndexConnectionsPool();

    OrthancPluginContext* GetContext() const
    {
      return context_;
    }

    void OpenConnections(bool hasIdentifierTags,
                         const std::list<IdentifierTag>& identifierTags);

    void CloseConnections();

    class Accessor : public boost::noncopyable
    {
    private:
      boost::shared_lock<boost::shared_mutex>  lock_;
      IndexConnectionsPool&                    pool_;
      DatabaseManager*                         manager_;
      
    public:
      explicit Accessor(IndexConnectionsPool& pool);

      ~Accessor();

      IndexBackend& GetBackend() const;

      DatabaseManager& GetManager() const;
    };
  };
}
