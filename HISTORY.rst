**********
History
**********

07-27-2014
-----------
   * Added wait() to efficiently wait for a batch of jobs to finish, and retrieve their results.
   * multyvac.get() now batches calls (bug fix: limit on number of jobs passed to get)
   * multyvac.get() now returns jobs in order of request jids.
   * Added _ignore_module_dependencies keyword for submit().

07-26-2014
-----------
Added kill_all() jobs feature.

07-21-2014
----------
Fixed bug with dependency transfer when pyc files are present.

06-12-2014
----------
Added method to update a cluster's max duration.

05-21-2014
----------
Added beta support for clusters.

05-10-2014
-----------
Proper Windows support.

02-23-2014
-----------
First release.

10-18-2013
------------
Ground breaks.
