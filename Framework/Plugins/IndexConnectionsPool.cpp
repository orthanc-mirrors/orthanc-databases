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


#include "IndexConnectionsPool.h"

#include <Logging.h>


namespace OrthancDatabases
{
  class IndexConnectionsPool::ManagerReference : public Orthanc::IDynamicObject
  {
  private:
    DatabaseManager*  manager_;

  public:
    explicit ManagerReference(DatabaseManager& manager) :
      manager_(&manager)
    {
    }

    DatabaseManager& GetManager()
    {
      assert(manager_ != NULL);
      return *manager_;
    }
  };


  void IndexConnectionsPool::HousekeepingThread(IndexConnectionsPool* that)
  {
    boost::posix_time::ptime lastInvocation = boost::posix_time::second_clock::local_time();

    while (that->housekeepingContinue_)
    {
      if (boost::posix_time::second_clock::local_time() - lastInvocation >= that->housekeepingDelay_)
      {
        Accessor accessor(*that);

        try
        {
          accessor.GetBackend().PerformDbHousekeeping(accessor.GetManager());
        }
        catch (Orthanc::OrthancException& e)
        {
          LOG(ERROR) << "Exception during the database housekeeping: " << e.What();
        }
        catch (...)
        {
          LOG(ERROR) << "Native exception during the database houskeeping";
        }

        lastInvocation = boost::posix_time::second_clock::local_time();
      }

      boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    }
  }


  IndexConnectionsPool::IndexConnectionsPool(IndexBackend* backend,
                                             size_t countConnections,
                                             unsigned int houseKeepingDelaySeconds) :
    backend_(backend),
    countConnections_(countConnections),
    housekeepingContinue_(true),
    housekeepingDelay_(boost::posix_time::seconds(houseKeepingDelaySeconds))
  {
    if (countConnections == 0)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange,
                                      "There must be a non-zero number of connections to the database");
    }
    else if (backend == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    else if (backend->HasPerformDbHousekeeping() &&
             houseKeepingDelaySeconds == 0)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange,
                                      "The delay between two executions of housekeeping cannot be zero second");
    }
    else
    {
      context_ = backend_->GetContext();
    }
  }

  
  IndexConnectionsPool::~IndexConnectionsPool()
  {
    for (std::list<DatabaseManager*>::iterator
           it = connections_.begin(); it != connections_.end(); ++it)
    {
      assert(*it != NULL);
      delete *it;
    }
  }


  void IndexConnectionsPool::OpenConnections(bool hasIdentifierTags,
                                             const std::list<IdentifierTag>& identifierTags)
  {
    boost::unique_lock<boost::shared_mutex>  lock(connectionsMutex_);

    if (connections_.size() == 0)
    {
      assert(backend_.get() != NULL);

      {
        std::unique_ptr<DatabaseManager> manager(new DatabaseManager(backend_->CreateDatabaseFactory()));
        manager->GetDatabase();  // Make sure to open the database connection
          
        backend_->ConfigureDatabase(*manager, hasIdentifierTags, identifierTags);
        connections_.push_back(manager.release());
      }

      for (size_t i = 1; i < countConnections_; i++)
      {
        connections_.push_back(new DatabaseManager(backend_->CreateDatabaseFactory()));
        connections_.back()->GetDatabase();  // Make sure to open the database connection
      }

      for (std::list<DatabaseManager*>::iterator
             it = connections_.begin(); it != connections_.end(); ++it)
      {
        assert(*it != NULL);
        availableConnections_.Enqueue(new ManagerReference(**it));
      }

      // Start the housekeeping thread
      housekeepingContinue_ = true;

      if (backend_->HasPerformDbHousekeeping())
      {
        housekeepingThread_ = boost::thread(HousekeepingThread, this);
      }
    }
    else
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
  }


  void IndexConnectionsPool::CloseConnections()
  {
    {
      // Stop the housekeeping thread
      housekeepingContinue_ = false;
      if (housekeepingThread_.joinable())
      {
        housekeepingThread_.join();
      }
    }

    boost::unique_lock<boost::shared_mutex>  lock(connectionsMutex_);

    if (connections_.size() != countConnections_)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else if (availableConnections_.GetSize() != countConnections_)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database, "Some connections are still in use, bug in the Orthanc core");
    }
    else
    {
      for (std::list<DatabaseManager*>::iterator
             it = connections_.begin(); it != connections_.end(); ++it)
      {
        assert(*it != NULL);
        (*it)->Close();
      }
    }
  }


  IndexConnectionsPool::Accessor::Accessor(IndexConnectionsPool& pool) :
    lock_(pool.connectionsMutex_),
    pool_(pool),
    manager_(NULL)
  {
    for (;;)
    {
      std::unique_ptr<Orthanc::IDynamicObject> manager(pool.availableConnections_.Dequeue(100));
      if (manager.get() != NULL)
      {
        manager_ = &dynamic_cast<ManagerReference&>(*manager).GetManager();
        return;
      }
    }
  }

  
  IndexConnectionsPool::Accessor::~Accessor()
  {
    assert(manager_ != NULL);
    pool_.availableConnections_.Enqueue(new ManagerReference(*manager_));
  }

  
  IndexBackend& IndexConnectionsPool::Accessor::GetBackend() const
  {
    return *pool_.backend_;
  }

  
  DatabaseManager& IndexConnectionsPool::Accessor::GetManager() const
  {
    assert(manager_ != NULL);
    return *manager_;
  }
}
