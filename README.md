file_copy
=========

Project to copy large directories from one location to another.

I started developing this application while trying to transfer a very large data set from one mounted drive to another.  The computer I was using would constantly crash, hang up, or otherwise terminate the transfer.  Most of the file transfer applications have no error recovery, and the ones that claim to, I found to be insufficient or unreliable.  So... I set out with the goal to create a transfer application that records its every movement in a database, and here we are.

# Limitations

Right now, walking the directory structure to get the list of files takes an INCREDIBLY long time.  Longer than actually copying the files, in my experience.  The problem is, in attempting to save the state of listing the files, there's not a convenient way to use the OS's implementation of walking a directory structure.  As such, I need to explicitly stat each directory, add all objects to the file queue table, then remove the directory from the file queue table.  There has to be a better way, but for these purposes, I ended up just doing a `find . -type f > blah.txt`, then JSON-ifying it with sed to create a file for mongo_import, and skipping the listing_files phase all together.
