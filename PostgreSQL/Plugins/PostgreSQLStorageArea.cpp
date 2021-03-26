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


#include "PostgreSQLStorageArea.h"
#include "PostgreSQLDefinitions.h"

#include "../../Framework/PostgreSQL/PostgreSQLTransaction.h"
#include "../../Resources/Orthanc/Plugins/OrthancPluginCppWrapper.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>


namespace OrthancDatabases
{
  IDatabase* PostgreSQLStorageArea::OpenInternal()
  {
    std::unique_ptr<PostgreSQLDatabase> db(new PostgreSQLDatabase(parameters_));

    db->Open();

    if (parameters_.HasLock())
    {
      db->AdvisoryLock(POSTGRESQL_LOCK_STORAGE);
    }

    {
      PostgreSQLDatabase::TransientAdvisoryLock lock(*db, POSTGRESQL_LOCK_DATABASE_SETUP);

      if (clearAll_)
      {
        db->ClearAll();
      }

      {
        PostgreSQLTransaction t(*db, TransactionType_ReadWrite);

        if (!db->DoesTableExist("StorageArea"))
        {
          db->Execute("CREATE TABLE IF NOT EXISTS StorageArea("
                      "uuid VARCHAR NOT NULL PRIMARY KEY,"
                      "content OID NOT NULL,"
                      "type INTEGER NOT NULL)");

          // Automatically remove the large objects associated with the table
          db->Execute("CREATE OR REPLACE RULE StorageAreaDelete AS ON DELETE "
                      "TO StorageArea DO SELECT lo_unlink(old.content);");
        }

        t.Commit();
      }
    }

    return db.release();
  }


  PostgreSQLStorageArea::PostgreSQLStorageArea(const PostgreSQLParameters& parameters) :
    StorageBackend(new Factory(*this)),
    parameters_(parameters),
    clearAll_(false)
  {
  }
}
