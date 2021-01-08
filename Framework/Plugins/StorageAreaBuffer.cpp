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


#include "StorageAreaBuffer.h"

#include <OrthancException.h>

#include <limits>
#include <string.h>


namespace OrthancDatabases
{
  StorageAreaBuffer::StorageAreaBuffer() :
    data_(NULL),
    size_(0)
  {
  }

  
  void StorageAreaBuffer::Clear()
  {
    if (data_ != NULL)
    {
      free(data_);
      data_ = NULL;
      size_ = 0;
    }
  }


  void StorageAreaBuffer::Assign(const std::string& content)
  {
    Clear();
    
    size_ = static_cast<int64_t>(content.size());
    
    if (static_cast<size_t>(size_) != content.size())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory,
                                      "File cannot be stored in a 63bit buffer");
    }

    if (content.empty())
    {
      data_ = NULL;
    }
    else
    {
      data_ = malloc(size_);

      if (data_ == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory);
      }

      memcpy(data_, content.c_str(), size_);
    }
  }


  void* StorageAreaBuffer::ReleaseData()
  {
    void* result = data_;
    data_ = NULL;
    size_ = 0;
    return result;
  }


  void StorageAreaBuffer::ToString(std::string& target)
  {
    target.resize(size_);

    if (size_ != 0)
    {
      memcpy(&target[0], data_, size_);
    }
  }
}
