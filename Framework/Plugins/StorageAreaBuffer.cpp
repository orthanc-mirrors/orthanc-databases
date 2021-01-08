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
#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 0)
  /**
   * If Orthanc SDK >= 1.9.0
   **/
  
  StorageAreaBuffer::StorageAreaBuffer(OrthancPluginContext* context) :
    context_(context)
  {
    buffer_.data = NULL;
    buffer_.size = 0;
  }

  
  void StorageAreaBuffer::Clear()
  {
    if (buffer_.data != NULL)
    {
      if (context_ == NULL)  // Are we running the unit tests?
      {
        free(buffer_.data);
      }
      else
      {
        OrthancPluginFreeMemoryBuffer64(context_, &buffer_);
      }
      
      buffer_.data = NULL;
      buffer_.size = 0;
    }
  }


  int64_t StorageAreaBuffer::GetSize() const
  {
    return buffer_.size;
  }

  
  const void* StorageAreaBuffer::GetData() const
  {
    return buffer_.data;
  }


  void StorageAreaBuffer::Assign(const std::string& content)
  {
    Clear();

    if (context_ == NULL)  // Are we running the unit tests?
    {
      buffer_.size = static_cast<uint64_t>(content.size());

      if (content.empty())
      {
        buffer_.data = NULL;
      }
      else
      {
        buffer_.data = malloc(content.size());
      }
    }
    else
    {
      if (OrthancPluginCreateMemoryBuffer64(context_, &buffer_, static_cast<uint64_t>(content.size())) !=
          OrthancPluginErrorCode_Success)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory);
      }
    }

    if (!content.empty())
    {
      memcpy(buffer_.data, content.c_str(), content.size());
    }
  }


  void StorageAreaBuffer::Move(OrthancPluginMemoryBuffer64* target)
  {
    *target = buffer_;
    buffer_.data = NULL;
    buffer_.size = 0;
  }


  void StorageAreaBuffer::ToString(std::string& target)
  {
    target.resize(buffer_.size);

    if (buffer_.size != 0)
    {
      memcpy(&target[0], buffer_.data, buffer_.size);
    }
  }
  

#else
  /**
   * If Orthanc SDK <= 1.8.2
   **/
  
  StorageAreaBuffer::StorageAreaBuffer(OrthancPluginContext* /* not used in this flavor */) :
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


  int64_t StorageAreaBuffer::GetSize() const
  {
    return size_;
  }

  
  const void* StorageAreaBuffer::GetData() const
  {
    return data_;
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
#endif
}
