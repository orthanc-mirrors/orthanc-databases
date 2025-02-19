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


// http://www.postgresql.org/docs/9.1/static/lo-interfaces.html#AEN33102

#include "PostgreSQLIncludes.h"  // Must be the first
#include "PostgreSQLLargeObject.h"

#include <Logging.h>
#include <OrthancException.h>

#include <boost/lexical_cast.hpp>
#include <libpq/libpq-fs.h>


namespace OrthancDatabases
{  
  void PostgreSQLLargeObject::Create()
  {
    PGconn* pg = reinterpret_cast<PGconn*>(database_.pg_);

    oid_ = lo_creat(pg, INV_WRITE);
    if (oid_ == 0)
    {
      LOG(ERROR) << "PostgreSQL: Cannot create a large object";
      database_.ThrowException(false);
    }
  }


  void PostgreSQLLargeObject::Write(const void* data, 
                                    size_t size)
  {
    static int MAX_CHUNK_SIZE = 16 * 1024 * 1024;

    PGconn* pg = reinterpret_cast<PGconn*>(database_.pg_);

    int fd = lo_open(pg, oid_, INV_WRITE);
    if (fd < 0)
    {
      database_.ThrowException(true);
    }

    const char* position = reinterpret_cast<const char*>(data);
    while (size > 0)
    {
      int chunk = (size > static_cast<size_t>(MAX_CHUNK_SIZE) ?
                   MAX_CHUNK_SIZE : static_cast<int>(size));
      int nbytes = lo_write(pg, fd, position, chunk);
      if (nbytes <= 0)
      {
        lo_close(pg, fd);
        database_.ThrowException(true);
      }

      size -= nbytes;
      position += nbytes;
    }

    lo_close(pg, fd);
  }


  PostgreSQLLargeObject::PostgreSQLLargeObject(PostgreSQLDatabase& database,
                                               const std::string& s) : 
    database_(database)
  {
    Create();

    if (s.size() != 0)
    {
      Write(s.c_str(), s.size());
    }
    else
    {
      Write(NULL, 0);
    }
  }


  class PostgreSQLLargeObject::Reader
  {
  private: 
    PostgreSQLDatabase& database_;
    int fd_;
    size_t size_;

    void ReadInternal(PGconn* pg,
                      std::string& target)
    {
      for (size_t position = 0; position < target.size(); )
      {
        size_t remaining = target.size() - position;

        int nbytes = lo_read(pg, fd_, &target[position], remaining);
        if (nbytes < 0)
        {
          LOG(ERROR) << "PostgreSQL: Unable to read the large object in the database";
          database_.ThrowException(false);
        }

        position += static_cast<size_t>(nbytes);
      }
    }
    
  public:
    Reader(PostgreSQLDatabase& database,
           const std::string& oid) : 
      database_(database)
    {
      PGconn* pg = reinterpret_cast<PGconn*>(database.pg_);
      Oid id = boost::lexical_cast<Oid>(oid);

      fd_ = lo_open(pg, id, INV_READ);

      if (fd_ < 0 ||
          lo_lseek(pg, fd_, 0, SEEK_END) < 0)
      {
        LOG(ERROR) << "PostgreSQL: No such large object in the database; "
                   << "Make sure you use a transaction";
        database.ThrowException(false);
      }

      // Get the size of the large object
      int size = lo_tell(pg, fd_);
      if (size < 0)
      {
        database.ThrowException(true);
      }
      size_ = static_cast<size_t>(size);
    }

    ~Reader()
    {
      lo_close(reinterpret_cast<PGconn*>(database_.pg_), fd_);
    }

    size_t GetSize() const
    {
      return size_;
    }

    void ReadWhole(std::string& target)
    {
      if (target.size() != size_)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
      
      PGconn* pg = reinterpret_cast<PGconn*>(database_.pg_);

      // Go to the first byte of the object
      lo_lseek(pg, fd_, 0, SEEK_SET);

      ReadInternal(pg, target);
    }

    void ReadRange(std::string& target,
                   uint64_t start)
    {
      PGconn* pg = reinterpret_cast<PGconn*>(database_.pg_);

      // Go to the first byte of the object
      lo_lseek(pg, fd_, start, SEEK_SET);

      ReadInternal(pg, target);
    }
  };
  

  void PostgreSQLLargeObject::ReadWhole(std::string& target,
                                        PostgreSQLDatabase& database,
                                        const std::string& oid)
  {
    Reader reader(database, oid);
    target.resize(reader.GetSize());    

    if (target.size() > 0)
    {
      reader.ReadWhole(target);
    }
  }


  void PostgreSQLLargeObject::ReadRange(std::string& target,
                                        PostgreSQLDatabase& database,
                                        const std::string& oid,
                                        uint64_t start,
                                        size_t length)
  {
    Reader reader(database, oid);

    if (start >= reader.GetSize() ||
        start + length > reader.GetSize())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadRange);
    }
    
    target.resize(length);

    if (target.size() > 0)
    {
      reader.ReadRange(target, start);
    }
  }


  std::string PostgreSQLLargeObject::GetOid() const
  {
    return boost::lexical_cast<std::string>(oid_);
  }


  void PostgreSQLLargeObject::Delete(PostgreSQLDatabase& database,
                                     const std::string& oid)
  {
    PGconn* pg = reinterpret_cast<PGconn*>(database.pg_);
    Oid id = boost::lexical_cast<Oid>(oid);

    if (lo_unlink(pg, id) < 0)
    {
      LOG(ERROR) << "PostgreSQL: Unable to delete the large object from the database";
      database.ThrowException(false);
    }
  }
}
