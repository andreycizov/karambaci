Roadmap
-------
+ Implement the docker configuration routines
+ Implement setup.py
+ Create an image that would be startable
+ Separating queues by their prefixes
+ Separating workers by their workable prefixes
+ Forgotten threads that no longer have any workable workers (?)
+ How do we ensure, on the plugin level - that the services are run for any plugin that requires them?
   ? also, certain plugins may need their own code to be installed - how do we do it ?
   - e.g. a docker plugin, to support pulls - requires it's own daemon running
   - e.g. actually, to support "watches".




- exception handling and exception handler setup
- deployment versioning (workers filter for the thread IDs which are based on the preselected prefix in their context)
   + what should happen to threads which stall ? e.g. threads which have outlived their deployment ?
- code caching and code storage
- transform the HTTP worker to using the FrozenThreadContext instead.
- implement cli for stack creation
- implement cli for thread creation