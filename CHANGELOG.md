## 0.1.1-jm
Features:
  - Added support for sending notification emails when a single topic fails.
  - Moved notification email address to `to` configuration item.
  - Notification email is no longer used a key.
  - Added `--version` command line argument.

## X.Y.Z (TBD)

Features:
  - Added request info to HTTP responses (#64 and #45)

Bugfixes:
  - Fix an issue where maxlag partition is selected badly

## 0.1.1 (2016-05-01)

Features:
  - ZK Offset checking
  - Storm offset checking

Bugfixes:
  - Fixed an issue with not closing HTTP requests and responses properly
  - Change internal hostname structures for properly support IPv6

## 0.1.0 (2015-10-07)

Initial version release
