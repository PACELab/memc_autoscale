## El Mem
A Modified Memcached 1.4.31 that supports autoscaling 
## Dependencies
It requires libketama (https://github.com/RJ/ketama) to be installed for c/c++ in addition to all the dependencies for memcahed to run which can be found at : https://github.com/memcached/memcached/wiki/Install
## How to Use it ?
You can just go inside the memcached_autoscale folder and run "./memcached" and provide any command line options you want. Otherwise, if you want to make some more changes, just add your changes, do a makeclean and make and you are good to go. 
## Newly Implemented Stats Commands (Can be used via telnet interface)
We added a bunch of new stats commands to support autoscaling:
### stats dump
Dumps all the contents of a memcached server along with last access timestamps into a file named 'dump' in the home directory.  
Usage:  
```
$ stats dump
```
### stats warmup
Loads the data from a file named 'dump' located in the home directory and does a trick to preserve the difference in last access timestamps as recorded in the 'dump'.  
Usage:  
```
$ stats warmup
```
### lru_crawler time_dump
Hashes all items of all the slabs in the memcached according to the given ring of servers and dumps them in spearate file i.e. one file for each server per slab. To run this command, make sure LRU crawler is enabled.  
Usage:  
```
$ lru_crawler timedump <slab_no> <curr_server_id> <#_of_servers_in ring_n> <id_1> <id_2> ... <id_n> 0
```
### stats merge
Once, you have decided to evict some of the servers, this command can tell optimal set of keys which you should keep given a set of servers that are going to be evicted (assumes some  specific data has been transferred to the current memcached server from all the evicting memcached servers).  
Usage:  
```
$ stats merge <curr_server_id> <#_of_evicting_servers_n> <id_1> <id_2> ... <id_n>
```
### stats migrate
Once, you have a list of optimal set of keys that you want to keep in a server that is being retained after a scaling action, this command actually inserts all of the key value pairs that are not already there (It assumes that the files containing the keys to be moved are present on the local disk of the node running memchaced server and are named in a specific format).  
Usage:  
```
$ stats migrate <curr_server_id> <#_of_evicting_servers_n> <id_1> <id_2> ... <id_n>
```

### stats scores
Returns the r/n^th timestamp for each slab along with the percentage of pages allocated to each slab where **n** is the initial number of servers and **r** is the number of retaining servers after a down-scaling event. This is useful for deciding which server to evict.  
Usage:  
```
$ stats score <n> <r>
```

