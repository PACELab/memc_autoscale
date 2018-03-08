/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include "slabs.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>
#include <poll.h>
#include <ketama.h>
#include <time.h>

/* Forward Declarations */
static void item_link_q(item *it);
static void item_unlink_q(item *it);

static unsigned int lru_type_map[4] = {HOT_LRU, WARM_LRU, COLD_LRU, NOEXP_LRU};

#define LARGEST_ID POWER_LARGEST
typedef struct {
    uint64_t evicted;
    uint64_t evicted_nonzero;
    uint64_t reclaimed;
    uint64_t outofmemory;
    uint64_t tailrepairs;
    uint64_t expired_unfetched; /* items reclaimed but never touched */
    uint64_t evicted_unfetched; /* items evicted but never touched */
    uint64_t crawler_reclaimed;
    uint64_t crawler_items_checked;
    uint64_t lrutail_reflocked;
    uint64_t moves_to_cold;
    uint64_t moves_to_warm;
    uint64_t moves_within_lru;
    uint64_t direct_reclaims;
    rel_time_t evicted_time;
} itemstats_t;

static item *heads[LARGEST_ID];
static item *tails[LARGEST_ID];
static itemstats_t itemstats[LARGEST_ID];
static unsigned int sizes[LARGEST_ID];
static uint64_t sizes_bytes[LARGEST_ID];
static unsigned int *stats_sizes_hist = NULL;
static uint64_t stats_sizes_cas_min = 0;
static int stats_sizes_buckets = 0;

static volatile int do_run_lru_maintainer_thread = 0;
static int lru_maintainer_initialized = 0;
static int lru_maintainer_check_clsid = 0;
static pthread_mutex_t lru_maintainer_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t cas_id_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t stats_sizes_lock = PTHREAD_MUTEX_INITIALIZER;

void item_stats_reset(void) {
    int i;
    for (i = 0; i < LARGEST_ID; i++) {
        pthread_mutex_lock(&lru_locks[i]);
        memset(&itemstats[i], 0, sizeof(itemstats_t));
        pthread_mutex_unlock(&lru_locks[i]);
    }
}

/* called with class lru lock held */
void do_item_stats_add_crawl(const int i, const uint64_t reclaimed,
        const uint64_t unfetched, const uint64_t checked) {
    itemstats[i].crawler_reclaimed += reclaimed;
    itemstats[i].expired_unfetched += unfetched;
    itemstats[i].crawler_items_checked += checked;
}

#define LRU_PULL_EVICT 1
#define LRU_PULL_CRAWL_BLOCKS 2

static int lru_pull_tail(const int orig_id, const int cur_lru,
        const uint64_t total_bytes, uint8_t flags);

/* Get the next CAS id for a new item. */
/* TODO: refactor some atomics for this. */
uint64_t get_cas_id(void) {
    static uint64_t cas_id = 0;
    pthread_mutex_lock(&cas_id_lock);
    uint64_t next_id = ++cas_id;
    pthread_mutex_unlock(&cas_id_lock);
    return next_id;
}

int item_is_flushed(item *it) {
    rel_time_t oldest_live = settings.oldest_live;
    uint64_t cas = ITEM_get_cas(it);
    uint64_t oldest_cas = settings.oldest_cas;
    if (oldest_live == 0 || oldest_live > current_time)
        return 0;
    if ((it->time <= oldest_live)
            || (oldest_cas != 0 && cas != 0 && cas < oldest_cas)) {
        return 1;
    }
    return 0;
}

static unsigned int noexp_lru_size(int slabs_clsid) {
    int id = CLEAR_LRU(slabs_clsid);
    id |= NOEXP_LRU;
    unsigned int ret;
    pthread_mutex_lock(&lru_locks[id]);
    ret = sizes_bytes[id];
    pthread_mutex_unlock(&lru_locks[id]);
    return ret;
}

/* Enable this for reference-count debugging. */
#if 0
# define DEBUG_REFCNT(it,op) \
                fprintf(stderr, "item %x refcnt(%c) %d %c%c%c\n", \
                        it, op, it->refcount, \
                        (it->it_flags & ITEM_LINKED) ? 'L' : ' ', \
                        (it->it_flags & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const unsigned int flags, const int nbytes,
                     char *suffix, uint8_t *nsuffix) {
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t) snprintf(suffix, 40, " %u %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

item *do_item_alloc(char *key, const size_t nkey, const unsigned int flags,
                    const rel_time_t exptime, const int nbytes) {
    int i;
    uint8_t nsuffix;
    item *it = NULL;
    char suffix[40];
    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);
    if (settings.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    unsigned int id = slabs_clsid(ntotal);
    if (id == 0)
        return 0;

    /* If no memory is available, attempt a direct LRU juggle/eviction */
    /* This is a race in order to simplify lru_pull_tail; in cases where
     * locked items are on the tail, you want them to fall out and cause
     * occasional OOM's, rather than internally work around them.
     * This also gives one fewer code path for slab alloc/free
     */
    /* TODO: if power_largest, try a lot more times? or a number of times
     * based on how many chunks the new object should take up?
     * or based on the size of an object lru_pull_tail() says it evicted?
     * This is a classical GC problem if "large items" are of too varying of
     * sizes. This is actually okay here since the larger the data, the more
     * bandwidth it takes, the more time we can loop in comparison to serving
     * and replacing small items.
     */
    for (i = 0; i < 10; i++) {
        uint64_t total_bytes;
        /* Try to reclaim memory first */
        if (!settings.lru_maintainer_thread) {
            lru_pull_tail(id, COLD_LRU, 0, 0);
        }
        it = slabs_alloc(ntotal, id, &total_bytes, 0);

        if (settings.expirezero_does_not_evict)
            total_bytes -= noexp_lru_size(id);

        if (it == NULL) {
            if (settings.lru_maintainer_thread) {
                lru_pull_tail(id, HOT_LRU, total_bytes, 0);
                lru_pull_tail(id, WARM_LRU, total_bytes, 0);
                if (lru_pull_tail(id, COLD_LRU, total_bytes, LRU_PULL_EVICT) <= 0)
                    break;
            } else {
                if (lru_pull_tail(id, COLD_LRU, 0, LRU_PULL_EVICT) <= 0)
                    break;
            }
        } else {
            break;
        }
    }

    if (i > 0) {
        pthread_mutex_lock(&lru_locks[id]);
        itemstats[id].direct_reclaims += i;
        pthread_mutex_unlock(&lru_locks[id]);
    }

    if (it == NULL) {
        pthread_mutex_lock(&lru_locks[id]);
        itemstats[id].outofmemory++;
        pthread_mutex_unlock(&lru_locks[id]);
        return NULL;
    }

    assert(it->slabs_clsid == 0);
    //assert(it != heads[id]);

    /* Refcount is seeded to 1 by slabs_alloc() */
    it->next = it->prev = 0;

    /* Items are initially loaded into the HOT_LRU. This is '0' but I want at
     * least a note here. Compiler (hopefully?) optimizes this out.
     */
    if (settings.lru_maintainer_thread) {
        if (exptime == 0 && settings.expirezero_does_not_evict) {
            id |= NOEXP_LRU;
        } else {
            id |= HOT_LRU;
        }
    } else {
        /* There is only COLD in compat-mode */
        id |= COLD_LRU;
    }
    it->slabs_clsid = id;

    DEBUG_REFCNT(it, '*');
    it->it_flags |= settings.use_cas ? ITEM_CAS : 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    memcpy(ITEM_key(it), key, nkey);
    it->exptime = exptime;
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    it->nsuffix = nsuffix;

    /* Need to shuffle the pointer stored in h_next into it->data. */
    if (it->it_flags & ITEM_CHUNKED) {
        item_chunk *chunk = (item_chunk *) ITEM_data(it);

        chunk->next = (item_chunk *) it->h_next;
        chunk->prev = 0;
        chunk->head = it;
        /* Need to chain back into the head's chunk */
        chunk->next->prev = chunk;
        chunk->size = chunk->next->size - ((char *)chunk - (char *)it);
        chunk->used = 0;
        assert(chunk->size > 0);
    }
    it->h_next = 0;

    return it;
}

void item_free(item *it) {
    size_t ntotal = ITEM_ntotal(it);
    unsigned int clsid;
    assert((it->it_flags & ITEM_LINKED) == 0);
    assert(it != heads[it->slabs_clsid]);
    assert(it != tails[it->slabs_clsid]);
    assert(it->refcount == 0);

    /* so slab size changer can tell later if item is already free or not */
    clsid = ITEM_clsid(it);
    DEBUG_REFCNT(it, 'F');
    slabs_free(it, ntotal, clsid);
}

/**
 * Returns true if an item will fit in the cache (its size does not exceed
 * the maximum for a cache entry.)
 */
bool item_size_ok(const size_t nkey, const int flags, const int nbytes) {
    char prefix[40];
    uint8_t nsuffix;

    size_t ntotal = item_make_header(nkey + 1, flags, nbytes,
                                     prefix, &nsuffix);
    if (settings.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    return slabs_clsid(ntotal) != 0;
}

static void do_item_link_q(item *it) { /* item is the new head */
    item **head, **tail;
    assert((it->it_flags & ITEM_SLABBED) == 0);

    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];
    assert(it != *head);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = 0;
    it->next = *head;
    if (it->next) it->next->prev = it;
    *head = it;
    if (*tail == 0) *tail = it;
    sizes[it->slabs_clsid]++;
    sizes_bytes[it->slabs_clsid] += it->nbytes;
    return;
}

static void item_link_q(item *it) {
    pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
    do_item_link_q(it);
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
}

static void do_item_unlink_q(item *it) {
    item **head, **tail;
    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    sizes[it->slabs_clsid]--;
    sizes_bytes[it->slabs_clsid] -= it->nbytes;
    return;
}

static void item_unlink_q(item *it) {
    pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
    do_item_unlink_q(it);
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
}

int do_item_link(item *it, const uint32_t hv) {
    MEMCACHED_ITEM_LINK(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & (ITEM_LINKED|ITEM_SLABBED)) == 0);
    it->it_flags |= ITEM_LINKED;
    it->time = current_time;

    STATS_LOCK();
    stats_state.curr_bytes += ITEM_ntotal(it);
    stats_state.curr_items += 1;
    stats.total_items += 1;
    STATS_UNLOCK();

    /* Allocate a new CAS ID on link. */
    ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
    assoc_insert(it, hv);
    item_link_q(it);
    refcount_incr(&it->refcount);
    item_stats_sizes_add(it);

    return 1;
}

void do_item_unlink(item *it, const uint32_t hv) {
    MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
    if ((it->it_flags & ITEM_LINKED) != 0) {
        it->it_flags &= ~ITEM_LINKED;
        STATS_LOCK();
        stats_state.curr_bytes -= ITEM_ntotal(it);
        stats_state.curr_items -= 1;
        STATS_UNLOCK();
        item_stats_sizes_remove(it);
        assoc_delete(ITEM_key(it), it->nkey, hv);
        item_unlink_q(it);
        do_item_remove(it);
    }
}

/* FIXME: Is it necessary to keep this copy/pasted code? */
void do_item_unlink_nolock(item *it, const uint32_t hv) {
    MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
    if ((it->it_flags & ITEM_LINKED) != 0) {
        it->it_flags &= ~ITEM_LINKED;
        STATS_LOCK();
        stats_state.curr_bytes -= ITEM_ntotal(it);
        stats_state.curr_items -= 1;
        STATS_UNLOCK();
        item_stats_sizes_remove(it);
        assoc_delete(ITEM_key(it), it->nkey, hv);
        do_item_unlink_q(it);
        do_item_remove(it);
    }
}

void do_item_remove(item *it) {
    MEMCACHED_ITEM_REMOVE(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & ITEM_SLABBED) == 0);
    assert(it->refcount > 0);

    if (refcount_decr(&it->refcount) == 0) {
        item_free(it);
    }
}

/* Copy/paste to avoid adding two extra branches for all common calls, since
 * _nolock is only used in an uncommon case where we want to relink. */
void do_item_update_nolock(item *it) {
    MEMCACHED_ITEM_UPDATE(ITEM_key(it), it->nkey, it->nbytes);
    if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
        assert((it->it_flags & ITEM_SLABBED) == 0);

        if ((it->it_flags & ITEM_LINKED) != 0) {
            do_item_unlink_q(it);
            it->time = current_time;
            do_item_link_q(it);
        }
    }
}

/* Bump the last accessed time, or relink if we're in compat mode */
void do_item_update(item *it) {
    MEMCACHED_ITEM_UPDATE(ITEM_key(it), it->nkey, it->nbytes);
    if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
        assert((it->it_flags & ITEM_SLABBED) == 0);

        if ((it->it_flags & ITEM_LINKED) != 0) {
            it->time = current_time;
            if (!settings.lru_maintainer_thread) {
                item_unlink_q(it);
                item_link_q(it);
            }
        }
    }
}

int do_item_replace(item *it, item *new_it, const uint32_t hv) {
    MEMCACHED_ITEM_REPLACE(ITEM_key(it), it->nkey, it->nbytes,
                           ITEM_key(new_it), new_it->nkey, new_it->nbytes);
    assert((it->it_flags & ITEM_SLABBED) == 0);

    do_item_unlink(it, hv);
    return do_item_link(new_it, hv);
}

char* dump(int * bytes_ret) {
    unsigned int memlimit = 1024 * 1024;   /* 2MB max response size */
    char *buffer_ret;
    char temp_ret[512];
    int bufcurr = 0;
    int len;
    int i;
    item *it;
    int count = 0;
    char val[10010];
    char key_temp[12];
    //size_t nkey = 11;
    char temp[11512];
    int tocpy;
    FILE *fp;
    FILE *fp_ts;

    snprintf(temp, sizeof(temp), "/home/ubuntu/dump");
    fp = fopen(temp, "w");
    snprintf(temp, sizeof(temp), "/home/ubuntu/dump_ts");
    fp_ts = fopen(temp, "w");
    buffer_ret = malloc((size_t)memlimit);

    for (i = 0; i < LARGEST_ID; i++)
    {
        pthread_mutex_lock(&lru_locks[i]);
        printf("locked %d \n", i);
        if (heads[i] != NULL)
        {
            len = snprintf(temp_ret, sizeof(temp_ret), "%d\r\n",i);
            memcpy(buffer_ret + bufcurr, temp_ret, len);
            bufcurr += len;
            printf("found a slab\n");
            it = heads[i];
            while (it)
            {
                count++;
                tocpy = it->nbytes-2;
                strncpy(val, ITEM_data(it), tocpy);
                val[tocpy] = 0x00;
                strncpy(key_temp, ITEM_key(it), it->nkey);
                key_temp[it->nkey] = 0x00; /* terminate */
                snprintf(temp, sizeof(temp), "%s %lu %s\n", key_temp, strlen(val), val);
                fputs(temp, fp);
		snprintf(temp, sizeof(temp), "%lu\n", (unsigned long)(it->time + process_started));
                fputs(temp, fp_ts);
                it = it->next;
            }
            printf("slab done\n");
            // pthread_mutex_unlock(&lru_locks[i]);
        }
	pthread_mutex_unlock(&lru_locks[i]);
    }
    fclose(fp);
    fclose(fp_ts);
    len = snprintf(temp, sizeof(temp), "total dumped : %d\n", count);
    memcpy(buffer_ret + bufcurr, temp, len);
    bufcurr += len;
    memcpy(buffer_ret + bufcurr, "END\r\n", 6);
    bufcurr += 5;
    *bytes_ret = bufcurr;
    return buffer_ret;
}

char * get_slabs(int * bytes) {
    unsigned int memlimit = 1024 * 1024;   /* 2MB max response size */
    char *buffer_ret;
    char temp_ret[512];
    int bufcurr = 0;
    int len;

    int * slabs;
    int count, i;
    buffer_ret = malloc((size_t)memlimit);
    slabs = do_get_active_slabs(&count);
    printf("count is : %d\n", count);
    for (i = 0; i < count; i++) {
	len = snprintf(temp_ret, sizeof(temp_ret), "%d\r\n",slabs[i]);
        memcpy(buffer_ret + bufcurr, temp_ret, len);
        bufcurr += len;
    }

    //buffer_ret = malloc((size_t)memlimit);
    memcpy(buffer_ret + bufcurr, "END\r\n", 6);
    bufcurr += 5;
    *bytes = bufcurr;
    free(slabs);
    return buffer_ret;
}

int * do_get_active_slabs(int * count) {
    printf("inside slabs\n");
    int cnt = 0;
    int i;
    for (i = 0; i < LARGEST_ID; i++)
    {
	printf("curr slab : %d\n", i);
	pthread_mutex_lock(&lru_locks[i]);
	printf("locked : %d\n", i);
        if (heads[i] != NULL)
        {
	    cnt += 1;
	}
	//pthread_mutex_unlock(&lru_locks[i]);
	printf("unlocked : %d\n", i);
    }
    printf("outside loop\n");
    *count = cnt;
    printf("count updated\n");
    int * slabs = malloc(cnt * sizeof(int));
    printf("mallocing done \n");
    cnt  = 0;
    for (i = 0; i < LARGEST_ID; i++)
    {
	printf("+++++++ trying lock : ++++++ : %d\n", i);
	//pthread_mutex_lock(&lru_locks[i]);
	printf("+++++++locked2++++++ : %d\n", i);
	if (heads[i] != NULL)
	{
	    slabs[cnt] = i;
	    cnt += 1;
	}
	pthread_mutex_unlock(&lru_locks[i]);
	printf("*******unlocked2****** : %d\n", i);
    }
//    pthread_mutex_unlock(&lru_locks[i]);
    return slabs;
}



#define ELEM_SWAP(a,b) { register unsigned long t=(a);(a)=(b);(b)=t; }


/*---------------------------------------------------------------------------
   Function :   kth_smallest()
   In       :   array of elements, # of elements in the array, rank k
   Out      :   one element
   Job      :   find the kth smallest element in the array
   Notice   :   use the median() macro defined below to get the median.

                Reference:

                  Author: Wirth, Niklaus
                   Title: Algorithms + data structures = programs
               Publisher: Englewood Cliffs: Prentice-Hall, 1976
    Physical description: 366 p.
                  Series: Prentice-Hall Series in Automatic Computation

 ---------------------------------------------------------------------------*/


unsigned long kth_smallest(unsigned long a[], unsigned int n, unsigned int k)
{
    register int i,j,l,m ;
    register unsigned long x ;

    l=0 ; m=n-1 ;
    while (l<m) {
        x=a[k] ;
        i=l ;
        j=m ;
        do {
            while (a[i]<x) i++ ;
            while (x<a[j]) j-- ;
            if (i<=j) {
                ELEM_SWAP(a[i],a[j]) ;
                i++ ; j-- ;
            }
        } while (i<=j) ;
        if (j<k) l=i ;
        if (k<i) m=j ;
    }
    return a[k] ;
}


#define median(a,n) kth_smallest(a,n,(((n)&1)?((n)/2):(((n)/2)-1)))

const void * last_comp1;
const void * last_comp2;
const char * server_id;
int is_exp;

int cmpfunc(const void * a, const void * b) {
	last_comp1 = a;
	last_comp2 = b;
	return ( *(unsigned long*)a - *(unsigned long*)b );
}

void store_id(char * buf) {
    int len = strlen(buf);
    printf("len of server id :%d\n", len);
    char * tmp_server_id = malloc(sizeof(char)*(len+1));
    memcpy(tmp_server_id, buf, len);
    tmp_server_id[len] = 0x00;
    server_id = tmp_server_id;
}

const char* get_server_id() {
    return server_id;
}

int get_server() {
    return atoi(server_id+2);
}

int get_exp() {
    return is_exp;
}

void preprocess_hashing(char * myid, int ret_count, int * retServers, int do_exp) {
    FILE *ketama_file;
    char temp[100];
    int i;
    ketama_file = fopen("ketama.servers", "w");
    for (i = 0; i < ret_count; i++) {
        snprintf(temp, sizeof(temp), "mc%d 1\n", retServers[i]);
        fputs(temp, ketama_file);
    }
    fclose(ketama_file);
    is_exp = do_exp;
    store_id(myid);
}

unsigned int insertion_pt(unsigned long arr[], unsigned int size, unsigned long num)
{
	unsigned long *item = (unsigned long*) bsearch (&num, arr, size, sizeof(int), cmpfunc);
	unsigned int x;
	// if (!item)
		// printf("item is null\n");
	if (last_comp1 == &num)
		item = (unsigned long*) last_comp2;
	else
		item = (unsigned long*) last_comp1;
	// if (!item)
		// printf("item is still null!!\n");
	x = (unsigned int) (((unsigned long*)item - (unsigned long*)arr));
	return x;
}

unsigned int * find_top_k(unsigned int m, unsigned int n, unsigned int * sizes, unsigned long **arrs, unsigned int k) {
    unsigned long * meds = malloc(m * sizeof(unsigned long));
    unsigned long * meds_index = malloc(m * sizeof(unsigned long));
	int * start_pt = malloc(m * sizeof(unsigned int));
    int * end_pt = malloc(m * sizeof(unsigned int));
	unsigned int * break_pts = malloc(m * sizeof(unsigned int));
	// int * active = malloc(m * sizeof(unsigned int));
	unsigned int * insertion_pts = malloc(m * sizeof(unsigned int));
	unsigned int i, j, ii;
	unsigned int med;
    unsigned int med_count = 0;
	unsigned long med_of_med;
	unsigned int larger_count;
	//unsigned int total_elem  = k;

	for (i = 0; i < m; i++) {
		start_pt[i] = 0;
        end_pt[i] = sizes[i]-1;
        // active[i] = 1;
	}

	while (k > 4294967295) {
		// int xyz;
		// scanf("%d", &xyz);
		// for (i = 0; i < m; i++)
		// {
		// 	// printf("%d\n", i);
		// 	// printf("start : %d\n", start_pt[i]);
		// 	// printf("end : %d\n", end_pt[i]);
		// }

        med_count = 0;
		for (i = 0; i < m; i++) {
            if (start_pt[i] > end_pt[i]) {
                continue;
            }

			med = (end_pt[i]+start_pt[i])/2;
			meds[med_count] = arrs[i][med];
            meds_index[med_count] = i;
            med_count++;
		}
		// printf("med count : %u\n", med_count);
		med_of_med = median(meds, med_count);

		// printf("median of medians : %lu\n", med_of_med);

		larger_count = 0;
		for (ii = 0; ii < med_count; ii++) {
            i = meds_index[ii];
            // if (start_pt[i] <= end_pt[i]) {
    		insertion_pts[i] = insertion_pt(&arrs[i][start_pt[i]], end_pt[i]-start_pt[i]+1, med_of_med);
    			// printf("first element : %d\n", arrs[i][0]);
    			// printf("second element : %d\n", arrs[i][1]);
    			// printf("second element : %d\n", *(&arrs[i][start_pt[i]]+1));
    			// if (insertion_pts[i] < end_pt[i]-start_pt[i])
    		larger_count += (end_pt[i] - start_pt[i] + 1) - (insertion_pts[i] + 1);
            // }
		}

		if (larger_count > k) {
			for (ii = 0; ii < med_count; ii++) {
                // if (start_pt[i] <= end_pt[i])
                i = meds_index[ii];
                start_pt[i] = start_pt[i] + insertion_pts[i]+1;
            }
		}
		else if (larger_count <= k) {
			for (ii = 0; ii < med_count; ii++) {
                // if (start_pt[i] <= end_pt[i])
                i = meds_index[ii];
                end_pt[i] = start_pt[i]+insertion_pts[i];
            }
			k = k - larger_count;
		}
		// printf(" value of k : %d\n", k);
		// printf(" value of larger_count : %d\n", larger_count);
	}

	// nth_elem = med_of_med;

	for (i = 0; i < k; i++)
	{
		int list = 0;
		int max = 0;
		for (j = 0; j < m; j++)
		{
			// j = meds_index[ii];
			// printf("end pt :  %u\n", end_pt[j]);
			// printf("max size : %u\n", sizes[j]);
			if (end_pt[j] >= start_pt[j] && arrs[j][end_pt[j]] > max)
			{
				max = arrs[j][end_pt[j]];
				list = j;
			}
		}
		//if (end_pt[list] == 0)
		//	end_pt[list] = 4294967295;
		//else
		if (end_pt[list] == 0)
			start_pt[list] = 1;
		else
			end_pt[list]--;
		// nth_elem = max;
	}
	unsigned int tot_count = 0;
	for (i = 0; i < m; i++)
	{
		// printf("elements to pick : %u\n", n-end_pt[i]-1);
        tot_count += sizes[i]-end_pt[i]-1;
		break_pts[i] = sizes[i]-end_pt[i]-1;
	}
	printf("value of k : %u, total count :  %u\n", k, tot_count);

    free(meds);
    free(meds_index);
    free(start_pt);
    free(end_pt);
    free(insertion_pts);

	return break_pts;
}

unsigned int * get_merging_points(int slabno, unsigned int count, unsigned long * my_lru, int myid, int * retiring_servers, int rts_count)
{
	int i;
	unsigned long **arrs = (unsigned long **)malloc((rts_count+1) * sizeof(unsigned long *));
    for (i=0; i<(rts_count+1); i++)
        arrs[i] = (unsigned long *)calloc(count, sizeof(unsigned long));
    unsigned int * end_pt = malloc((rts_count+1) * sizeof(unsigned int));
	char temp[100];
	char buf[200];
	unsigned long ul;
	int j;
	int temp_count;
	//int is_ended;
	unsigned int * to_ret;
	FILE *fp;

	printf("initialized\n");

	for (i = 0; i < rts_count; i++)
	{
		// printf("curr retiring server %d\n", retiring_servers[i]);
		//is_ended = 0;
		snprintf(temp, sizeof(temp), "/home/ubuntu/keys/mc%d_mc%d/mc%d_mc%d_slbn=%d.ss", myid, retiring_servers[i], retiring_servers[i], myid,  slabno);
		printf("file opening: %s\n", temp);
		fp = fopen(temp, "r");
		if (!fp)
		    printf("file not opened\n");
		// printf("file opeened : %d %d, number of files : %d \n", count, i, rts_count);
		j = count-1;
		temp_count = 0;

		for (j = 0; j < count; j++)
		{
			// printf("%s inside forloop %d\n", temp, j);
			if (fgets(buf, 150, fp))
			{
				// printf("getting item  : %s, buf len : %u\n", buf, (unsigned int)strlen(buf));
				ul = strtoul(buf, NULL, 0);
				// printf("converted to unisgned int, putting at i : %d, j : %d \n", i, j);
				// printf("i limit : %d, j limit : %u", rts_count, count-1);
				arrs[i][j] = ul;
				// printf("getting item done  : %s\n", buf);
				temp_count++;
			}
            else
            {
                break;
            }
		}
        end_pt[i] = temp_count;
		if (fp == NULL)
			printf("something bad happened\n");
		// printf("closing file \n");
		fclose(fp);
	}
	printf("appending my own list\n");

	for (j = 0; j < count; j++)
	{
		arrs[rts_count][j] = my_lru[j];
	}
    end_pt[rts_count] = count;
	printf("going on to find the top k \n");

	to_ret = find_top_k(rts_count+1, count, end_pt, arrs, count);
	for (i = 0; i < (rts_count+1); i++) {
  		free(arrs[i]);
	}
	free(arrs);
    free(end_pt);
	return to_ret;
}


char *do_merge_all(int myid, int * bytes_ret, int * retiring_servers, int rts_count) {
    unsigned int memlimit = 4 * 1024 * 1024;   /* 4MB max response size */
    char *buffer_ret;
    char temp_ret[1024];
    int bufcurr = 0;
    int len;
    unsigned int * merging_pts;
    unsigned int kv_count;
    int tmp_tot;
    item * it;
    item * cnt;
    int i, j, locked;
    buffer_ret = malloc((size_t)memlimit);

    for (i = 0; i < LARGEST_ID; i++) {
        //printf("slab id : %d\n", i);
        locked = 1;
        pthread_mutex_lock(&lru_locks[i]);
        if (heads[i] != NULL) {
            kv_count = 0;
            it = heads[i];
            cnt = it;
           // printf("slab is here \n");
            while (cnt) {
                cnt = cnt->next;
                kv_count++;
            }
            printf("done counting\n");
            unsigned long * my_lru = malloc(kv_count * sizeof(unsigned long));
            for (j = 0; j < kv_count; j++) {
                my_lru[kv_count-1-j] = (unsigned long) (it->time + process_started);
                it = it->next;
            }
            pthread_mutex_unlock(&lru_locks[i]);
            locked = 0;
            printf("Merging Points for Slab # %d, total items :%u :\n", i, kv_count);
            merging_pts = get_merging_points(i, kv_count, my_lru, myid, retiring_servers, rts_count);
            free(my_lru);
            tmp_tot = 0;
            for (j = 0; j < rts_count; j++) {
                len = snprintf(temp_ret, sizeof(temp_ret), "%d %d %u\r\n", i, retiring_servers[j], merging_pts[j]);
                tmp_tot += merging_pts[j];
                memcpy(buffer_ret + bufcurr, temp_ret, len);
                bufcurr += len;
                printf("%d %d %u\n", i, retiring_servers[j], merging_pts[j]);
            }
	    printf("items to import : %d\n", tmp_tot);
            tmp_tot += merging_pts[rts_count];
            printf("sum of merging points: %d\n", tmp_tot);
            printf("\n");
            free(merging_pts);
        }
        if (locked)
            pthread_mutex_unlock(&lru_locks[i]);
    }
    memcpy(buffer_ret + bufcurr, "END\r\n", 6);
    bufcurr += 5;
    *bytes_ret = bufcurr;
    return buffer_ret;
}

/*
char * update_ts(char * key, int key_len, int * bytes) {
    uint32_t hv;
    item * it;
    hv = hash(key, key_len);
    item_lock(hv);
    it = do_item_get(key, it->nkey, hv, c);



}
*/

void dump_slabs_pages(void) {
    int i;
    unsigned int id;
    char fname[200];
    FILE *fp;
    for (i = 0; i < LARGEST_ID; i++) {
	//pthread_mutex_lock(&lru_locks[i]);
	if (heads[i] == NULL) {
	 //   pthread_mutex_unlock(&lru_locks[i]);
	    continue;
	}
	//pthread_mutex_unlock(&lru_locks[i]);
	snprintf(fname, sizeof(fname), "/home/ubuntu/ts/%d", i);
	fp = fopen(fname, "w");
	if (!settings.lru_maintainer_thread)
            id = i-128;
	else
	    id = i;
	printf("Doing slab id : %u\n", id);
	printf("Slab i : %d\n", i);
	get_items(id, fp);
	fclose(fp);
    }
}

void dump_slabs(void) {
    item *it;
    char temp[11512];
    FILE *fp;
    char fname[200];
    int i,tocpy,lcount;
    char val[11000];
    char key_temp[15];
    for (i = 0; i < LARGEST_ID; i++) {
	lcount = 0;
	pthread_mutex_lock(&lru_locks[i]);
	if (heads[i] == NULL) {
	    pthread_mutex_unlock(&lru_locks[i]);
	    continue;
	}
	snprintf(fname, sizeof(fname), "/home/ubuntu/ts/%d", i);
	fp = fopen(fname, "w");
	it = heads[i];
	while (it) {
	    tocpy = it->nbytes-2;
	    strncpy(val, ITEM_data(it), tocpy);
	    val[tocpy] = 0x00;
	    strncpy(key_temp, ITEM_key(it), it->nkey);
	    key_temp[it->nkey] = 0x00;
	    snprintf(temp, sizeof(temp), "%lu %s %s\n", (unsigned long)(it->time + process_started), key_temp, val);
	    fputs(temp, fp);
	    //fprintf(fp, "%d %s %s\r\n", tocpy, key_temp, val);
	    //if (x < 0) {
	    //printf("failed : %s\n", temp);
	    //}
	    it = it->next;
	    lcount++;
	}
	printf("****%d*** line count : %d\n", i, lcount);
	pthread_mutex_unlock(&lru_locks[i]);
	fclose(fp);
    }
}

char* do_hash_all(char* buf, int * bytes_ret, int exp, int ret_count, int * retServers) {
    unsigned int memlimit = 1024 * 1024;   /* 1MB max response size */
    char *buffer_ret;
    char temp_ret[512];
    int bufcurr;
    int len;
    char *buffer;
    item *it;
    char key_temp[KEY_MAX_LENGTH + 1];
    char temp[11512];
    char val[11000];
    int tocpy;
    int files = 0;
    FILE *ketama_file;
    FILE *fp[10];
    FILE *fp_data[10];
    char fnames[10][20];
    char fname[200];
    char dirname[200];
    int i = 0;
    int j = 0;
    int count = 0;
    int file_index = 0;

    buffer_ret = malloc((size_t)memlimit);
    ketama_continuum con;
    ketama_file = fopen("ketama1.servers", "r");

    while (fgets(temp, 100, ketama_file)) {
        if (strlen(temp) < 3)
            break;
        strcpy(fnames[files], strtok(temp, " "));
        files++;
    }
    fclose(ketama_file);

    printf("ketama created\n");
    ketama_file = fopen("ketama.servers", "w");
    for (i = 0; i < ret_count; i++) {
	snprintf(temp, sizeof(temp), "mc%d 1\n", retServers[i]);
        fputs(temp, ketama_file);
    }
    fclose(ketama_file);
    ketama_roll(&con, "ketama.servers");
    printf("ketama rolled\n");

    bufcurr = 0;
    for (i = 0; i < LARGEST_ID; i++)
    {
        pthread_mutex_lock(&lru_locks[i]);
        printf("locked %d \n", i);
        if (heads[i] != NULL)
        {
            len = snprintf(temp_ret, sizeof(temp_ret), "%d\r\n",i);
            memcpy(buffer_ret + bufcurr, temp_ret, len);
            bufcurr += len;
            printf("found a slab\n");
            for (j = 0; j < files; j++)
            {
		snprintf(dirname, sizeof(dirname), "/home/ubuntu/keys/%s_%s", fnames[j], buf);
		struct stat st = {0};

		if (stat(dirname, &st) == -1) {
    		    mkdir(dirname, 0700);
		}
                snprintf(fname, sizeof(fname), "/home/ubuntu/keys/%s_%s/%s_%s_slbn=%d.ss", fnames[j], buf, buf, fnames[j], i);
                fp[j] = fopen(fname, "w");
                printf("file created : %s\n", fname);
                snprintf(fname, sizeof(fname), "/home/ubuntu/data/%s_%s_slbn=%d.dt", buf, fnames[j], i);
                fp_data[j] = fopen(fname, "w");
            }
            it = heads[i];
            while (it)
            {
		//if (exp == 1)
		 //   it->exptime = current_time;
		// printf("inside loop\n");
                count++;
                tocpy = it->nbytes-2;
                strncpy(val, ITEM_data(it), tocpy);
                val[tocpy] = 0x00;
                strncpy(key_temp, ITEM_key(it), it->nkey);
                key_temp[it->nkey] = 0x00; /* terminate */
                mcs *m = ketama_get_server(key_temp, con);
		//if (exp == 1)
		//    buffer = buf+2;
                //else
		    buffer = (m->ip)+2;
		if (exp == 1 && atoi(buffer) != atoi(buf+2))
			it->exptime = 1;
                file_index = atoi(buffer);
		// printf("writing to file\n");
                snprintf(temp, sizeof(temp), "%s %lu %s\n", key_temp, strlen(val), val);
                fputs(temp, fp_data[file_index-1]);

                snprintf(temp, sizeof(temp), "%lu\n", (unsigned long)it->time + process_started);
                fputs(temp, fp[file_index-1]);

                it = it->next;
            }
            // printf("slab done\n");

            for (j = 0; j < files; j++)
            {
                if (fp[j])
                {
                    fflush(fp[j]);
                    fclose(fp[j]);
                }

                if (fp_data[j])
                {
                    fflush(fp_data[j]);
                    fclose(fp_data[j]);
                }
            }
            //pthread_mutex_unlock(&lru_locks[i]);
            //printf("done flush\n");
        }
	pthread_mutex_unlock(&lru_locks[i]);
        printf("done flush\n");
    }
    memcpy(buffer_ret + bufcurr, "END\r\n", 6);
    bufcurr += 5;
    *bytes_ret = bufcurr;
    return buffer_ret;
}


char * scores_all(int * bytes, int init, int ret) {
    unsigned memlimit = 2 * 1024 * 1024;
    char *buffer_ret;
    double * weights;
    int * active_slabs;
    unsigned int * items_count;
    double rt = ((double)ret)/((double)init);

    char temp_ret[512];
    int bufcurr = 0;
    int slabs_count = 256;
    int i, j, len;
    unsigned int id;
    item * it;
    unsigned int med_ts;


    buffer_ret = malloc((size_t)memlimit);

    weights = malloc(slabs_count * sizeof(double));
    active_slabs = malloc(slabs_count * sizeof(int));
    items_count = malloc(slabs_count * sizeof(unsigned int));

    slabs_count = get_scores(weights, active_slabs, items_count);

    for (i = 0; i < slabs_count; i++)
    {
	id  = active_slabs[i];
	if (!settings.lru_maintainer_thread)
        	id |= COLD_LRU;
        printf("Active slab id : %d\n", id);
        printf("Active slab weight : %f\n", weights[i]);
        printf("Active slab item_count : %u\n", items_count[i]);
	pthread_mutex_lock(&lru_locks[id]);
	it = heads[id];
	printf("total  : %d\n", init);
	printf("remaining  : %d\n", ret);
	printf("ratio  : %f\n", rt);
	printf("items count : %f\n", items_count[i]*rt);
	for (j = 0; j < items_count[i]*rt; j++) {
	    it = it->next;
	}
	med_ts = process_started + it->time;
	pthread_mutex_unlock(&lru_locks[id]);
	len = snprintf(temp_ret, sizeof(temp_ret), "%d %u %u %.4f\r\n",id, items_count[i], med_ts, weights[i]);
        memcpy(buffer_ret + bufcurr, temp_ret, len);
        bufcurr += len;
    }
    free(weights);
    free(active_slabs);
    free(items_count);
    memcpy(buffer_ret + bufcurr, "END\r\n", 6);
    bufcurr += 5;
    *bytes = bufcurr;
    return buffer_ret;
}


/*@null@*/
/* This is walking the line of violating lock order, but I think it's safe.
 * If the LRU lock is held, an item in the LRU cannot be wiped and freed.
 * The data could possibly be overwritten, but this is only accessing the
 * headers.
 * It may not be the best idea to leave it like this, but for now it's safe.
 * FIXME: only dumps the hot LRU with the new LRU's.
 */
char *item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes) {
    unsigned int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
    char *buffer;
    unsigned int bufcurr;
    item *it;
    unsigned int len;
    unsigned int shown = 0;
    char key_temp[KEY_MAX_LENGTH + 1];
    char temp[512];
    unsigned int id = slabs_clsid;
    if (!settings.lru_maintainer_thread)
        id |= COLD_LRU;
    pthread_mutex_lock(&lru_locks[id]);
    it = heads[id];

    buffer = malloc((size_t)memlimit);
    if (buffer == 0) {
        return NULL;
    }
    bufcurr = 0;

    while (it != NULL && (limit == 0 || shown < limit)) {
        assert(it->nkey <= KEY_MAX_LENGTH);
        if (it->nbytes == 0 && it->nkey == 0) {
            it = it->next;
            continue;
        }
        /* Copy the key since it may not be null-terminated in the struct */
        strncpy(key_temp, ITEM_key(it), it->nkey);
        key_temp[it->nkey] = 0x00; /* terminate */
        len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %lu s]\r\n",
                       key_temp, it->nbytes - 2,
                       it->exptime == 0 ? 0 :
                       (unsigned long)it->exptime + process_started);
        if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */
            break;
        memcpy(buffer + bufcurr, temp, len);
        bufcurr += len;
        shown++;
        it = it->next;
    }

    memcpy(buffer + bufcurr, "END\r\n", 6);
    bufcurr += 5;

    *bytes = bufcurr;
    pthread_mutex_unlock(&lru_locks[id]);
    return buffer;
}
/* Implementation to display the time of each item to decide the hotness */
char *item_cachedump_time(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes) {
    unsigned int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
    char *buffer;
    unsigned int bufcurr;
    item *it;
    unsigned int len;
    unsigned int shown = 0;
    char key_temp[KEY_MAX_LENGTH + 1];
    char temp[512];
    unsigned int id = slabs_clsid;
    if (!settings.lru_maintainer_thread)
        id |= COLD_LRU;

    pthread_mutex_lock(&lru_locks[id]);
    it = heads[id];

    buffer = malloc((size_t)memlimit);
    if (buffer == 0) {
        return NULL;
    }
    bufcurr = 0;

    while (it != NULL && (limit == 0 || shown < limit)) {
        assert(it->nkey <= KEY_MAX_LENGTH);
        if (it->nbytes == 0 && it->nkey == 0) {
            it = it->next;
            continue;
        }
        /* Copy the key since it may not be null-terminated in the struct */
        strncpy(key_temp, ITEM_key(it), it->nkey);
        key_temp[it->nkey] = 0x00; /* terminate */
        
	len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %lu s]\r\n",
                       key_temp, it->nbytes - 2,
                       it->time == 0 ? 0 :
                       (unsigned long)it->time);
        

	/* len = snprintf(temp, sizeof(temp), "%s\r\n",
                       key_temp);
        */
	if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */
            break;
        memcpy(buffer + bufcurr, temp, len);
        bufcurr += len;
        shown++;
        it = it->next;
    }

    memcpy(buffer + bufcurr, "END\r\n", 6);
    bufcurr += 5;

    *bytes = bufcurr;
    pthread_mutex_unlock(&lru_locks[id]);
    return buffer;
}


void item_stats_totals(ADD_STAT add_stats, void *c) {
    itemstats_t totals;
    memset(&totals, 0, sizeof(itemstats_t));
    int n;
    for (n = 0; n < MAX_NUMBER_OF_SLAB_CLASSES; n++) {
        int x;
        int i;
        for (x = 0; x < 4; x++) {
            i = n | lru_type_map[x];
            pthread_mutex_lock(&lru_locks[i]);
            totals.expired_unfetched += itemstats[i].expired_unfetched;
            totals.evicted_unfetched += itemstats[i].evicted_unfetched;
            totals.evicted += itemstats[i].evicted;
            totals.reclaimed += itemstats[i].reclaimed;
            totals.crawler_reclaimed += itemstats[i].crawler_reclaimed;
            totals.crawler_items_checked += itemstats[i].crawler_items_checked;
            totals.lrutail_reflocked += itemstats[i].lrutail_reflocked;
            totals.moves_to_cold += itemstats[i].moves_to_cold;
            totals.moves_to_warm += itemstats[i].moves_to_warm;
            totals.moves_within_lru += itemstats[i].moves_within_lru;
            totals.direct_reclaims += itemstats[i].direct_reclaims;
            pthread_mutex_unlock(&lru_locks[i]);
        }
    }
    APPEND_STAT("expired_unfetched", "%llu",
                (unsigned long long)totals.expired_unfetched);
    APPEND_STAT("evicted_unfetched", "%llu",
                (unsigned long long)totals.evicted_unfetched);
    APPEND_STAT("evictions", "%llu",
                (unsigned long long)totals.evicted);
    APPEND_STAT("reclaimed", "%llu",
                (unsigned long long)totals.reclaimed);
    APPEND_STAT("crawler_reclaimed", "%llu",
                (unsigned long long)totals.crawler_reclaimed);
    APPEND_STAT("crawler_items_checked", "%llu",
                (unsigned long long)totals.crawler_items_checked);
    APPEND_STAT("lrutail_reflocked", "%llu",
                (unsigned long long)totals.lrutail_reflocked);
    if (settings.lru_maintainer_thread) {
        APPEND_STAT("moves_to_cold", "%llu",
                    (unsigned long long)totals.moves_to_cold);
        APPEND_STAT("moves_to_warm", "%llu",
                    (unsigned long long)totals.moves_to_warm);
        APPEND_STAT("moves_within_lru", "%llu",
                    (unsigned long long)totals.moves_within_lru);
        APPEND_STAT("direct_reclaims", "%llu",
                    (unsigned long long)totals.direct_reclaims);
    }
}

void item_stats(ADD_STAT add_stats, void *c) {
    itemstats_t totals;
    int n;
    for (n = 0; n < MAX_NUMBER_OF_SLAB_CLASSES; n++) {
        memset(&totals, 0, sizeof(itemstats_t));
        int x;
        int i;
        unsigned int size = 0;
        unsigned int age  = 0;
        unsigned int lru_size_map[4];
        const char *fmt = "items:%d:%s";
        char key_str[STAT_KEY_LEN];
        char val_str[STAT_VAL_LEN];
        int klen = 0, vlen = 0;
        for (x = 0; x < 4; x++) {
            i = n | lru_type_map[x];
            pthread_mutex_lock(&lru_locks[i]);
            totals.evicted += itemstats[i].evicted;
            totals.evicted_nonzero += itemstats[i].evicted_nonzero;
            totals.outofmemory += itemstats[i].outofmemory;
            totals.tailrepairs += itemstats[i].tailrepairs;
            totals.reclaimed += itemstats[i].reclaimed;
            totals.expired_unfetched += itemstats[i].expired_unfetched;
            totals.evicted_unfetched += itemstats[i].evicted_unfetched;
            totals.crawler_reclaimed += itemstats[i].crawler_reclaimed;
            totals.crawler_items_checked += itemstats[i].crawler_items_checked;
            totals.lrutail_reflocked += itemstats[i].lrutail_reflocked;
            totals.moves_to_cold += itemstats[i].moves_to_cold;
            totals.moves_to_warm += itemstats[i].moves_to_warm;
            totals.moves_within_lru += itemstats[i].moves_within_lru;
            totals.direct_reclaims += itemstats[i].direct_reclaims;
            size += sizes[i];
            lru_size_map[x] = sizes[i];
            if (lru_type_map[x] == COLD_LRU && tails[i] != NULL)
                age = current_time - tails[i]->time;
            pthread_mutex_unlock(&lru_locks[i]);
        }
        if (size == 0)
            continue;
        APPEND_NUM_FMT_STAT(fmt, n, "number", "%u", size);
        if (settings.lru_maintainer_thread) {
            APPEND_NUM_FMT_STAT(fmt, n, "number_hot", "%u", lru_size_map[0]);
            APPEND_NUM_FMT_STAT(fmt, n, "number_warm", "%u", lru_size_map[1]);
            APPEND_NUM_FMT_STAT(fmt, n, "number_cold", "%u", lru_size_map[2]);
            if (settings.expirezero_does_not_evict) {
                APPEND_NUM_FMT_STAT(fmt, n, "number_noexp", "%u", lru_size_map[3]);
            }
        }
        APPEND_NUM_FMT_STAT(fmt, n, "age", "%u", age);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted",
                            "%llu", (unsigned long long)totals.evicted);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted_nonzero",
                            "%llu", (unsigned long long)totals.evicted_nonzero);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted_time",
                            "%u", totals.evicted_time);
        APPEND_NUM_FMT_STAT(fmt, n, "outofmemory",
                            "%llu", (unsigned long long)totals.outofmemory);
        APPEND_NUM_FMT_STAT(fmt, n, "tailrepairs",
                            "%llu", (unsigned long long)totals.tailrepairs);
        APPEND_NUM_FMT_STAT(fmt, n, "reclaimed",
                            "%llu", (unsigned long long)totals.reclaimed);
        APPEND_NUM_FMT_STAT(fmt, n, "expired_unfetched",
                            "%llu", (unsigned long long)totals.expired_unfetched);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted_unfetched",
                            "%llu", (unsigned long long)totals.evicted_unfetched);
        APPEND_NUM_FMT_STAT(fmt, n, "crawler_reclaimed",
                            "%llu", (unsigned long long)totals.crawler_reclaimed);
        APPEND_NUM_FMT_STAT(fmt, n, "crawler_items_checked",
                            "%llu", (unsigned long long)totals.crawler_items_checked);
        APPEND_NUM_FMT_STAT(fmt, n, "lrutail_reflocked",
                            "%llu", (unsigned long long)totals.lrutail_reflocked);
        if (settings.lru_maintainer_thread) {
            APPEND_NUM_FMT_STAT(fmt, n, "moves_to_cold",
                                "%llu", (unsigned long long)totals.moves_to_cold);
            APPEND_NUM_FMT_STAT(fmt, n, "moves_to_warm",
                                "%llu", (unsigned long long)totals.moves_to_warm);
            APPEND_NUM_FMT_STAT(fmt, n, "moves_within_lru",
                                "%llu", (unsigned long long)totals.moves_within_lru);
            APPEND_NUM_FMT_STAT(fmt, n, "direct_reclaims",
                                "%llu", (unsigned long long)totals.direct_reclaims);
        }
    }

    /* getting here means both ascii and binary terminators fit */
    add_stats(NULL, 0, NULL, 0, c);
}

bool item_stats_sizes_status(void) {
    bool ret = false;
    mutex_lock(&stats_sizes_lock);
    if (stats_sizes_hist != NULL)
        ret = true;
    mutex_unlock(&stats_sizes_lock);
    return ret;
}

void item_stats_sizes_init(void) {
    if (stats_sizes_hist != NULL)
        return;
    stats_sizes_buckets = settings.item_size_max / 32 + 1;
    stats_sizes_hist = calloc(stats_sizes_buckets, sizeof(int));
    stats_sizes_cas_min = (settings.use_cas) ? get_cas_id() : 0;
}

void item_stats_sizes_enable(ADD_STAT add_stats, void *c) {
    mutex_lock(&stats_sizes_lock);
    if (!settings.use_cas) {
        APPEND_STAT("sizes_status", "error", "");
        APPEND_STAT("sizes_error", "cas_support_disabled", "");
    } else if (stats_sizes_hist == NULL) {
        item_stats_sizes_init();
        if (stats_sizes_hist != NULL) {
            APPEND_STAT("sizes_status", "enabled", "");
        } else {
            APPEND_STAT("sizes_status", "error", "");
            APPEND_STAT("sizes_error", "no_memory", "");
        }
    } else {
        APPEND_STAT("sizes_status", "enabled", "");
    }
    mutex_unlock(&stats_sizes_lock);
}

void item_stats_sizes_disable(ADD_STAT add_stats, void *c) {
    mutex_lock(&stats_sizes_lock);
    if (stats_sizes_hist != NULL) {
        free(stats_sizes_hist);
        stats_sizes_hist = NULL;
    }
    APPEND_STAT("sizes_status", "disabled", "");
    mutex_unlock(&stats_sizes_lock);
}

void item_stats_sizes_add(item *it) {
    if (stats_sizes_hist == NULL || stats_sizes_cas_min > ITEM_get_cas(it))
        return;
    int ntotal = ITEM_ntotal(it);
    int bucket = ntotal / 32;
    if ((ntotal % 32) != 0) bucket++;
    if (bucket < stats_sizes_buckets) stats_sizes_hist[bucket]++;
}

/* I think there's no way for this to be accurate without using the CAS value.
 * Since items getting their time value bumped will pass this validation.
 */
void item_stats_sizes_remove(item *it) {
    if (stats_sizes_hist == NULL || stats_sizes_cas_min > ITEM_get_cas(it))
        return;
    int ntotal = ITEM_ntotal(it);
    int bucket = ntotal / 32;
    if ((ntotal % 32) != 0) bucket++;
    if (bucket < stats_sizes_buckets) stats_sizes_hist[bucket]--;
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
/* Locks are correct based on a technicality. Holds LRU lock while doing the
 * work, so items can't go invalid, and it's only looking at header sizes
 * which don't change.
 */
void item_stats_sizes(ADD_STAT add_stats, void *c) {
    mutex_lock(&stats_sizes_lock);

    if (stats_sizes_hist != NULL) {
        int i;
        for (i = 0; i < stats_sizes_buckets; i++) {
            if (stats_sizes_hist[i] != 0) {
                char key[8];
                snprintf(key, sizeof(key), "%d", i * 32);
                APPEND_STAT(key, "%u", stats_sizes_hist[i]);
            }
        }
    } else {
        APPEND_STAT("sizes_status", "disabled", "");
    }

    add_stats(NULL, 0, NULL, 0, c);
    mutex_unlock(&stats_sizes_lock);
}

/** wrapper around assoc_find which does the lazy expiration logic */
item *do_item_get(const char *key, const size_t nkey, const uint32_t hv, conn *c) {
    item *it = assoc_find(key, nkey, hv);
    if (it != NULL) {
        refcount_incr(&it->refcount);
        /* Optimization for slab reassignment. prevents popular items from
         * jamming in busy wait. Can only do this here to satisfy lock order
         * of item_lock, slabs_lock. */
        /* This was made unsafe by removal of the cache_lock:
         * slab_rebalance_signal and slab_rebal.* are modified in a separate
         * thread under slabs_lock. If slab_rebalance_signal = 1, slab_start =
         * NULL (0), but slab_end is still equal to some value, this would end
         * up unlinking every item fetched.
         * This is either an acceptable loss, or if slab_rebalance_signal is
         * true, slab_start/slab_end should be put behind the slabs_lock.
         * Which would cause a huge potential slowdown.
         * Could also use a specific lock for slab_rebal.* and
         * slab_rebalance_signal (shorter lock?)
         */
        /*if (slab_rebalance_signal &&
            ((void *)it >= slab_rebal.slab_start && (void *)it < slab_rebal.slab_end)) {
            do_item_unlink(it, hv);
            do_item_remove(it);
            it = NULL;
        }*/
    }
    int was_found = 0;

    if (settings.verbose > 2) {
        int ii;
        if (it == NULL) {
            fprintf(stderr, "> NOT FOUND ");
        } else {
            fprintf(stderr, "> FOUND KEY ");
        }
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
    }

    if (it != NULL) {
        was_found = 1;
        if (item_is_flushed(it)) {
            do_item_unlink(it, hv);
            do_item_remove(it);
            it = NULL;
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.get_flushed++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            if (settings.verbose > 2) {
                fprintf(stderr, " -nuked by flush");
            }
            was_found = 2;
        } else if (it->exptime != 0 && it->exptime <= current_time) {
            do_item_unlink(it, hv);
            do_item_remove(it);
            it = NULL;
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.get_expired++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            if (settings.verbose > 2) {
                fprintf(stderr, " -nuked by expire");
            }
            was_found = 3;
        } else {
            it->it_flags |= ITEM_FETCHED|ITEM_ACTIVE;
            DEBUG_REFCNT(it, '+');
        }
    }

    if (settings.verbose > 2)
        fprintf(stderr, "\n");
    /* For now this is in addition to the above verbose logging. */
    LOGGER_LOG(c->thread->l, LOG_FETCHERS, LOGGER_ITEM_GET, NULL, was_found, key, nkey);

    return it;
}

item *do_item_touch(const char *key, size_t nkey, uint32_t exptime,
                    const uint32_t hv, conn *c) {
    item *it = do_item_get(key, nkey, hv, c);
    if (it != NULL) {
        it->exptime = exptime;
    }
    return it;
}

/*** LRU MAINTENANCE THREAD ***/

/* Returns number of items remove, expired, or evicted.
 * Callable from worker threads or the LRU maintainer thread */
static int lru_pull_tail(const int orig_id, const int cur_lru,
        const uint64_t total_bytes, uint8_t flags) {
    item *it = NULL;
    int id = orig_id;
    int removed = 0;
    if (id == 0)
        return 0;

    int tries = 5;
    item *search;
    item *next_it;
    void *hold_lock = NULL;
    unsigned int move_to_lru = 0;
    uint64_t limit = 0;

    id |= cur_lru;
    pthread_mutex_lock(&lru_locks[id]);
    search = tails[id];
    /* We walk up *only* for locked items, and if bottom is expired. */
    for (; tries > 0 && search != NULL; tries--, search=next_it) {
        /* we might relink search mid-loop, so search->prev isn't reliable */
        next_it = search->prev;
        if (search->nbytes == 0 && search->nkey == 0 && search->it_flags == 1) {
            /* We are a crawler, ignore it. */
            if (flags & LRU_PULL_CRAWL_BLOCKS) {
                pthread_mutex_unlock(&lru_locks[id]);
                return 0;
            }
            tries++;
            continue;
        }
        uint32_t hv = hash(ITEM_key(search), search->nkey);
        /* Attempt to hash item lock the "search" item. If locked, no
         * other callers can incr the refcount. Also skip ourselves. */
        if ((hold_lock = item_trylock(hv)) == NULL)
            continue;
        /* Now see if the item is refcount locked */
        if (refcount_incr(&search->refcount) != 2) {
            /* Note pathological case with ref'ed items in tail.
             * Can still unlink the item, but it won't be reusable yet */
            itemstats[id].lrutail_reflocked++;
            /* In case of refcount leaks, enable for quick workaround. */
            /* WARNING: This can cause terrible corruption */
            if (settings.tail_repair_time &&
                    search->time + settings.tail_repair_time < current_time) {
                itemstats[id].tailrepairs++;
                search->refcount = 1;
                /* This will call item_remove -> item_free since refcnt is 1 */
                do_item_unlink_nolock(search, hv);
                item_trylock_unlock(hold_lock);
                continue;
            }
        }

        /* Expired or flushed */
        if ((search->exptime != 0 && search->exptime < current_time)
            || item_is_flushed(search)) {
            itemstats[id].reclaimed++;
            if ((search->it_flags & ITEM_FETCHED) == 0) {
                itemstats[id].expired_unfetched++;
            }
            /* refcnt 2 -> 1 */
            do_item_unlink_nolock(search, hv);
            /* refcnt 1 -> 0 -> item_free */
            do_item_remove(search);
            item_trylock_unlock(hold_lock);
            removed++;

            /* If all we're finding are expired, can keep going */
            continue;
        }

        /* If we're HOT_LRU or WARM_LRU and over size limit, send to COLD_LRU.
         * If we're COLD_LRU, send to WARM_LRU unless we need to evict
         */
        switch (cur_lru) {
            case HOT_LRU:
                limit = total_bytes * settings.hot_lru_pct / 100;
            case WARM_LRU:
                if (limit == 0)
                    limit = total_bytes * settings.warm_lru_pct / 100;
                if (sizes_bytes[id] > limit) {
                    itemstats[id].moves_to_cold++;
                    move_to_lru = COLD_LRU;
                    do_item_unlink_q(search);
                    it = search;
                    removed++;
                    break;
                } else if ((search->it_flags & ITEM_ACTIVE) != 0) {
                    /* Only allow ACTIVE relinking if we're not too large. */
                    itemstats[id].moves_within_lru++;
                    search->it_flags &= ~ITEM_ACTIVE;
                    do_item_update_nolock(search);
                    do_item_remove(search);
                    item_trylock_unlock(hold_lock);
                } else {
                    /* Don't want to move to COLD, not active, bail out */
                    it = search;
                }
                break;
            case COLD_LRU:
                it = search; /* No matter what, we're stopping */
                if (flags & LRU_PULL_EVICT) {
                    if (settings.evict_to_free == 0) {
                        /* Don't think we need a counter for this. It'll OOM.  */
                        break;
                    }
                    itemstats[id].evicted++;
                    itemstats[id].evicted_time = current_time - search->time;
                    if (search->exptime != 0)
                        itemstats[id].evicted_nonzero++;
                    if ((search->it_flags & ITEM_FETCHED) == 0) {
                        itemstats[id].evicted_unfetched++;
                    }
                    LOGGER_LOG(NULL, LOG_EVICTIONS, LOGGER_EVICTION, search);
                    do_item_unlink_nolock(search, hv);
                    removed++;
                    if (settings.slab_automove == 2) {
                        slabs_reassign(-1, orig_id);
                    }
                } else if ((search->it_flags & ITEM_ACTIVE) != 0
                        && settings.lru_maintainer_thread) {
                    itemstats[id].moves_to_warm++;
                    search->it_flags &= ~ITEM_ACTIVE;
                    move_to_lru = WARM_LRU;
                    do_item_unlink_q(search);
                    removed++;
                }
                break;
        }
        if (it != NULL)
            break;
    }

    pthread_mutex_unlock(&lru_locks[id]);

    if (it != NULL) {
        if (move_to_lru) {
            it->slabs_clsid = ITEM_clsid(it);
            it->slabs_clsid |= move_to_lru;
            item_link_q(it);
        }
        do_item_remove(it);
        item_trylock_unlock(hold_lock);
    }

    return removed;
}

/* Loop up to N times:
 * If too many items are in HOT_LRU, push to COLD_LRU
 * If too many items are in WARM_LRU, push to COLD_LRU
 * If too many items are in COLD_LRU, poke COLD_LRU tail
 * 1000 loops with 1ms min sleep gives us under 1m items shifted/sec. The
 * locks can't handle much more than that. Leaving a TODO for how to
 * autoadjust in the future.
 */
static int lru_maintainer_juggle(const int slabs_clsid) {
    int i;
    int did_moves = 0;
    bool mem_limit_reached = false;
    uint64_t total_bytes = 0;
    unsigned int chunks_perslab = 0;
    unsigned int chunks_free = 0;
    /* TODO: if free_chunks below high watermark, increase aggressiveness */
    chunks_free = slabs_available_chunks(slabs_clsid, &mem_limit_reached,
            &total_bytes, &chunks_perslab);
    if (settings.expirezero_does_not_evict)
        total_bytes -= noexp_lru_size(slabs_clsid);

    /* If slab automove is enabled on any level, and we have more than 2 pages
     * worth of chunks free in this class, ask (gently) to reassign a page
     * from this class back into the global pool (0)
     */
    if (settings.slab_automove > 0 && chunks_free > (chunks_perslab * 2.5)) {
        slabs_reassign(slabs_clsid, SLAB_GLOBAL_PAGE_POOL);
    }

    /* Juggle HOT/WARM up to N times */
    for (i = 0; i < 1000; i++) {
        int do_more = 0;
        if (lru_pull_tail(slabs_clsid, HOT_LRU, total_bytes, LRU_PULL_CRAWL_BLOCKS) ||
            lru_pull_tail(slabs_clsid, WARM_LRU, total_bytes, LRU_PULL_CRAWL_BLOCKS)) {
            do_more++;
        }
        do_more += lru_pull_tail(slabs_clsid, COLD_LRU, total_bytes, LRU_PULL_CRAWL_BLOCKS);
        if (do_more == 0)
            break;
        did_moves++;
    }
    return did_moves;
}

/* Will crawl all slab classes a minimum of once per hour */
#define MAX_MAINTCRAWL_WAIT 60 * 60

/* Hoping user input will improve this function. This is all a wild guess.
 * Operation: Kicks crawler for each slab id. Crawlers take some statistics as
 * to items with nonzero expirations. It then buckets how many items will
 * expire per minute for the next hour.
 * This function checks the results of a run, and if it things more than 1% of
 * expirable objects are ready to go, kick the crawler again to reap.
 * It will also kick the crawler once per minute regardless, waiting a minute
 * longer for each time it has no work to do, up to an hour wait time.
 * The latter is to avoid newly started daemons from waiting too long before
 * retrying a crawl.
 */
static void lru_maintainer_crawler_check(struct crawler_expired_data *cdata) {
    int i;
    static rel_time_t next_crawls[MAX_NUMBER_OF_SLAB_CLASSES];
    static rel_time_t next_crawl_wait[MAX_NUMBER_OF_SLAB_CLASSES];
    uint8_t todo[MAX_NUMBER_OF_SLAB_CLASSES];
    memset(todo, 0, sizeof(uint8_t) * MAX_NUMBER_OF_SLAB_CLASSES);
    bool do_run = false;
    if (!cdata->crawl_complete) {
        return;
    }

    for (i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        crawlerstats_t *s = &cdata->crawlerstats[i];
        /* We've not successfully kicked off a crawl yet. */
        if (s->run_complete) {
            pthread_mutex_lock(&cdata->lock);
            int x;
            /* Should we crawl again? */
            uint64_t possible_reclaims = s->seen - s->noexp;
            uint64_t available_reclaims = 0;
            /* Need to think we can free at least 1% of the items before
             * crawling. */
            /* FIXME: Configurable? */
            uint64_t low_watermark = (s->seen / 100) + 1;
            rel_time_t since_run = current_time - s->end_time;
            /* Don't bother if the payoff is too low. */
            if (settings.verbose > 1)
                fprintf(stderr, "maint crawler[%d]: low_watermark: %llu, possible_reclaims: %llu, since_run: %u\n",
                        i, (unsigned long long)low_watermark, (unsigned long long)possible_reclaims,
                        (unsigned int)since_run);
            for (x = 0; x < 60; x++) {
                available_reclaims += s->histo[x];
                if (available_reclaims > low_watermark) {
                    if (next_crawl_wait[i] < (x * 60)) {
                        next_crawl_wait[i] += 60;
                    } else if (next_crawl_wait[i] >= 60) {
                        next_crawl_wait[i] -= 60;
                    }
                    break;
                }
            }

            if (available_reclaims == 0) {
                next_crawl_wait[i] += 60;
            }

            if (next_crawl_wait[i] > MAX_MAINTCRAWL_WAIT) {
                next_crawl_wait[i] = MAX_MAINTCRAWL_WAIT;
            }

            next_crawls[i] = current_time + next_crawl_wait[i] + 5;
            if (settings.verbose > 1)
                fprintf(stderr, "maint crawler[%d]: next_crawl: %u, [%d] now: [%d]\n",
                        i, next_crawl_wait[i], next_crawls[i], current_time);
            // Got our calculation, avoid running until next actual run.
            s->run_complete = false;
            pthread_mutex_unlock(&cdata->lock);
        }
        if (current_time > next_crawls[i]) {
            todo[i] = 1;
            do_run = true;
            next_crawls[i] = current_time + 5; // minimum retry wait.
        }
    }
    if (do_run) {
        lru_crawler_start(todo, 0, CRAWLER_EXPIRED, cdata, NULL, 0);
    }
}

static pthread_t lru_maintainer_tid;

#define MAX_LRU_MAINTAINER_SLEEP 1000000
#define MIN_LRU_MAINTAINER_SLEEP 1000

static void *lru_maintainer_thread(void *arg) {
    int i;
    useconds_t to_sleep = MIN_LRU_MAINTAINER_SLEEP;
    rel_time_t last_crawler_check = 0;
    struct crawler_expired_data cdata;
    memset(&cdata, 0, sizeof(struct crawler_expired_data));
    pthread_mutex_init(&cdata.lock, NULL);
    cdata.crawl_complete = true; // kick off the crawler.

    pthread_mutex_lock(&lru_maintainer_lock);
    if (settings.verbose > 2)
        fprintf(stderr, "Starting LRU maintainer background thread\n");
    while (do_run_lru_maintainer_thread) {
        int did_moves = 0;
        pthread_mutex_unlock(&lru_maintainer_lock);
        usleep(to_sleep);
        pthread_mutex_lock(&lru_maintainer_lock);

        STATS_LOCK();
        stats.lru_maintainer_juggles++;
        STATS_UNLOCK();
        /* We were asked to immediately wake up and poke a particular slab
         * class due to a low watermark being hit */
        if (lru_maintainer_check_clsid != 0) {
            did_moves = lru_maintainer_juggle(lru_maintainer_check_clsid);
            lru_maintainer_check_clsid = 0;
        } else {
            for (i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
                did_moves += lru_maintainer_juggle(i);
            }
        }
        if (did_moves == 0) {
            if (to_sleep < MAX_LRU_MAINTAINER_SLEEP)
                to_sleep += 1000;
        } else {
            to_sleep /= 2;
            if (to_sleep < MIN_LRU_MAINTAINER_SLEEP)
                to_sleep = MIN_LRU_MAINTAINER_SLEEP;
        }
        /* Once per second at most */
        if (settings.lru_crawler && last_crawler_check != current_time) {
            lru_maintainer_crawler_check(&cdata);
            last_crawler_check = current_time;
        }
    }
    pthread_mutex_unlock(&lru_maintainer_lock);
    if (settings.verbose > 2)
        fprintf(stderr, "LRU maintainer thread stopping\n");

    return NULL;
}
int stop_lru_maintainer_thread(void) {
    int ret;
    pthread_mutex_lock(&lru_maintainer_lock);
    /* LRU thread is a sleep loop, will die on its own */
    do_run_lru_maintainer_thread = 0;
    pthread_mutex_unlock(&lru_maintainer_lock);
    if ((ret = pthread_join(lru_maintainer_tid, NULL)) != 0) {
        fprintf(stderr, "Failed to stop LRU maintainer thread: %s\n", strerror(ret));
        return -1;
    }
    settings.lru_maintainer_thread = false;
    return 0;
}

int start_lru_maintainer_thread(void) {
    int ret;

    pthread_mutex_lock(&lru_maintainer_lock);
    do_run_lru_maintainer_thread = 1;
    settings.lru_maintainer_thread = true;
    if ((ret = pthread_create(&lru_maintainer_tid, NULL,
        lru_maintainer_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create LRU maintainer thread: %s\n",
            strerror(ret));
        pthread_mutex_unlock(&lru_maintainer_lock);
        return -1;
    }
    pthread_mutex_unlock(&lru_maintainer_lock);

    return 0;
}

/* If we hold this lock, crawler can't wake up or move */
void lru_maintainer_pause(void) {
    pthread_mutex_lock(&lru_maintainer_lock);
}

void lru_maintainer_resume(void) {
    pthread_mutex_unlock(&lru_maintainer_lock);
}

int init_lru_maintainer(void) {
    if (lru_maintainer_initialized == 0) {
        pthread_mutex_init(&lru_maintainer_lock, NULL);
        lru_maintainer_initialized = 1;
    }
    return 0;
}

/* Tail linkers and crawler for the LRU crawler. */
void do_item_linktail_q(item *it) { /* item is the new tail */
    item **head, **tail;
    assert(it->it_flags == 1);
    assert(it->nbytes == 0);

    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];
    //assert(*tail != 0);
    assert(it != *tail);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = *tail;
    it->next = 0;
    if (it->prev) {
        assert(it->prev->next == 0);
        it->prev->next = it;
    }
    *tail = it;
    if (*head == 0) *head = it;
    return;
}

void do_item_unlinktail_q(item *it) {
    item **head, **tail;
    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    return;
}

/* This is too convoluted, but it's a difficult shuffle. Try to rewrite it
 * more clearly. */
item *do_item_crawl_q(item *it) {
    item **head, **tail;
    assert(it->it_flags == 1);
    assert(it->nbytes == 0);
    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    /* We've hit the head, pop off */
    if (it->prev == 0) {
        assert(*head == it);
        if (it->next) {
            *head = it->next;
            assert(it->next->prev == it);
            it->next->prev = 0;
        }
        return NULL; /* Done */
    }

    /* Swing ourselves in front of the next item */
    /* NB: If there is a prev, we can't be the head */
    assert(it->prev != it);
    if (it->prev) {
        if (*head == it->prev) {
            /* Prev was the head, now we're the head */
            *head = it;
        }
        if (*tail == it) {
            /* We are the tail, now they are the tail */
            *tail = it->prev;
        }
        assert(it->next != it);
        if (it->next) {
            assert(it->prev->next == it);
            it->prev->next = it->next;
            it->next->prev = it->prev;
        } else {
            /* Tail. Move this above? */
            it->prev->next = 0;
        }
        /* prev->prev's next is it->prev */
        it->next = it->prev;
        it->prev = it->next->prev;
        it->next->prev = it;
        /* New it->prev now, if we're not at the head. */
        if (it->prev) {
            it->prev->next = it;
        }
    }
    assert(it->next != it);
    assert(it->prev != it);

    return it->next; /* success */
}
