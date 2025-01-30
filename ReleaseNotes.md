# Release Notes

## 0.2.4

- Fixed Storage cleanup, before workflow's individual records were cleared and now any workflow storage infrastructure is cleared as well. Workflows leave no trace.

## 0.2.3

- *resource stream* feature
- `aws`, `sql` blob storage options; `quest-py` extras
- fixed historian storage memory leak, storages clearing properly
- added SIGINT handler

## 0.2.2

- `alias` feature
- fixed exception replay