# aws-s3-lock

A experimental Go package to create distributed locks using S3 without using Databases.

The General flow is 
1. Attempt to Acquire lock
2. If succesfull do the main operation with in the configured timeout.
3. Release the lock

This go package uses fencing tokens as suggested in the [blog](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) to make the lock safe against race conditions. 
This package is heavily influenced and most of the code is adapted from [aws-s3-lock](https://github.com/jfstephe/aws-s3-lock) 
