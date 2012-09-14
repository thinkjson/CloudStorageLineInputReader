from __future__ import with_statement
#!/usr/bin/env python
#
# Copyright 2012 cloudysunny14
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

'''
Created on 2012/06/27

@author:  cloudysunny14@gmail.com
'''
import logging

from mapreduce.lib import files
from mapreduce import input_readers
from mapreduce import errors

Error = errors.Error
BadReaderParamsError = errors.BadReaderParamsError


class GoogleStorageLineInputReader(input_readers.InputReader):
  """Input reader for files from a stored in the GoogleCloudStorage.

  You requires activate the google cloud storage and create bucket.
  The class shouldn't be instantiated directly. Use the split_input
  class method instead.
  """

  # Maximum number of shards to allow.
  _MAX_SHARD_COUNT = 256

  # Maximum number of file path
  _MAX_FILE_PATHS_COUNT = 1

  # Mapreduce parameters.
  FILE_PATHS_PARAM = "file_paths"
  # Serialyzation parameters.
  INITIAL_POSITION_PARAM = "initial_position"
  START_POSITION_PARAM = "start_position"
  END_POSITION_PARAM = "end_position"
  FILE_PATH_PARAM = "file_path"

  def __init__(self, file_path, start_position, end_position):
    """Initializes this instance with the given file path and character range.

    This GoogleStorageLineInputReader will read from the first record starting
    after strictly after start_position until the first record ending at or
    after end_position (exclusive). As an exception, if start_position is 0,
    then this InputReader starts reading at the first record.

    Args:
      file_path: the file_path that this input reader is processing.
      start_position: the position to start reading at.
      end_position: a position in the last record to read.
    """
    self._file_path = file_path
    self._start_position = start_position
    self._end_position = end_position
    self._has_iterated = False
    self._filestream = None

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Mapper input reader class mismatch")

    params = _get_params(mapper_spec)

    if cls.FILE_PATHS_PARAM not in params:
      raise BadReaderParamsError("Must specify 'file_path' for mapper input")

    file_paths = params[cls.FILE_PATHS_PARAM]

    if isinstance(file_paths, basestring):
      # This is a mechanism to allow multiple file paths (which do not contain
      # commas) in a single string. It may go away.
      file_paths = file_paths.split(",")

    if len(file_paths) > cls._MAX_FILE_PATHS_COUNT:
      raise BadReaderParamsError("Too many 'file_paths' for mapper input")

    if not file_paths:
      raise BadReaderParamsError("No 'file_paths' specified for mapper input")

  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of shard_count input_spec_shards for input_spec.

    Args:
      mapper_spec: The mapper specification to split from. Must contain
          'file_paths' parameter with one or more file paths.

    Returns:
      A list of GoogleStorageLineInputReader corresponding to the
      specified shards.
    """
    params = _get_params(mapper_spec)
    file_paths = params[cls.FILE_PATHS_PARAM]

    if isinstance(file_paths, basestring):
      # This is a mechanism to allow multiple file paths (which do not contain
      # commas) in a single string. It may go away.
      file_paths = file_paths.split(",")

    file_sizes = {}

    for file_path in file_paths:
      fp = files.BufferedFile(file_path)
      fp.seek(0, 2)
      file_sizes[file_path] = fp.tell()

    shard_count = min(cls._MAX_SHARD_COUNT, mapper_spec.shard_count)
    shards_per_file = shard_count // len(file_paths)

    if shards_per_file == 0:
      shards_per_file = 1

    chunks = []

    for file_path, file_size in file_sizes.items():
      file_chunk_size = file_size // shards_per_file
      for i in xrange(shards_per_file - 1):
        chunks.append(GoogleStorageLineInputReader.from_json(
            {cls.FILE_PATH_PARAM: file_path,
             cls.INITIAL_POSITION_PARAM: file_chunk_size * i,
             cls.END_POSITION_PARAM: file_chunk_size * (i + 1)}))
      chunks.append(GoogleStorageLineInputReader.from_json(
          {cls.FILE_PATH_PARAM: file_path,
           cls.INITIAL_POSITION_PARAM: file_chunk_size * (shards_per_file - 1),
           cls.END_POSITION_PARAM: file_size}))

    return chunks

  def next(self):
    """Returns the next input from as an (offset, line) tuple."""
    self._has_iterated = True

    if not self._filestream:
      self._filestream = files.BufferedFile(self._file_path)
      if self._start_position:
        self._filestream.seek(self._start_position)
        self._filestream.readline()

    start_position = self._filestream.tell()

    if start_position > self._end_position:
      raise StopIteration()

    line = self._filestream.readline()

    if not line:
      raise StopIteration()

    return start_position, line.rstrip("\n")

  def _next_offset(self):
    """Return the offset of the next line to read."""
    if self._filestream:
      offset = self._filestream.tell()
      if offset:
        offset -= 1
    else:
      offset = self._start_position

    return offset

  def to_json(self):
    """Returns an json-compatible input shard spec for remaining inputs."""
    return {self.FILE_PATH_PARAM: self._file_path,
            self.INITIAL_POSITION_PARAM: self._next_offset(),
            self.END_POSITION_PARAM: self._end_position}

  def __str__(self):
    """Returns the string representation of this GoogleStorageLineInputReader."""
    return "FilePath(%r):[%d, %d]" % (
        self._file_path, self._next_offset(), self._end_position)

  @classmethod
  def from_json(cls, json):
    """Instantiates an instance of this InputReader for the given shard spec."""
    return cls(json[cls.FILE_PATH_PARAM],
               json[cls.INITIAL_POSITION_PARAM],
               json[cls.END_POSITION_PARAM])


def _get_params(mapper_spec, allowed_keys=None):
  """Obtain input reader parameters.

  Utility function for input readers implementation. Fetches parameters
  from mapreduce specification giving appropriate usage warnings.

  Args:
    mapper_spec: The MapperSpec for the job
    allowed_keys: set of all allowed keys in parameters as strings. If it is not
      None, then parameters are expected to be in a separate "input_reader"
      subdictionary of mapper_spec parameters.

  Returns:
    mapper parameters as dict

  Raises:
    BadReaderParamsError: if parameters are invalid/missing or not allowed.
  """
  if "input_reader" not in mapper_spec.params:
    message = ("Input reader's parameters should be specified in "
               "input_reader subdictionary.")
    if allowed_keys:
      raise errors.BadReaderParamsError(message)
    else:
      logging.warning(message)
    params = mapper_spec.params
    params = dict((str(n), v) for n, v in params.iteritems())
  else:
    if not isinstance(mapper_spec.params.get("input_reader"), dict):
      raise BadReaderParamsError(
          "Input reader parameters should be a dictionary")
    params = mapper_spec.params.get("input_reader")
    params = dict((str(n), v) for n, v in params.iteritems())
    if allowed_keys:
      params_diff = set(params.keys()) - allowed_keys
      if params_diff:
        raise errors.BadReaderParamsError(
            "Invalid input_reader parameters: %s" % ",".join(params_diff))

  return params
