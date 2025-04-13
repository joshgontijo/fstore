- Refactor TLog to use only events (remove current data format) - In Progress
- Move metadata to a TLog so it cannot cause data problems
  (careful logs can be deleted, segment states would have to be carried over to the next tlog segment)
- Internal events (flush, metadata, etc) should be just an internal stream

