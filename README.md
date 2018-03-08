## El Mem
A Modified Memcached 1.4.31 that supports autoscaling 
## Dependencies
It requires libketama (https://github.com/RJ/ketama) to be installed for c/c++ in addition to all the dependencies for memcahed to run which can be found at : https://github.com/memcached/memcached/wiki/Install
## How to Use it ?
You can just go inside the memc_autoscale folder and run "./memcached" and provide any command line options you want. Otherwise, if you want to make some more changes, just add your changes, do a makeclean and make and you are good to go. 
## Newly Implemented Stats Commands (Can be used via telnet interface)
We added a bunch of new stats commands to support autoscaling. Following is a list fo those commands.
### stats warmup
### stats dump
### stats hash
### stats migrate
### stats time_dump
### stats merge


