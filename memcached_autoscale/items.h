
#ifndef ITEMS_H
#define ITEMS_H

#define HOT_LRU 0
#define WARM_LRU 64
#define COLD_LRU 128
#define NOEXP_LRU 192

#define CLEAR_LRU(id) (id & ~(3<<6))

/* See items.c */
uint64_t get_cas_id(void);

/*@null@*/
item *do_item_alloc(char *key, const size_t nkey, const unsigned int flags, const rel_time_t exptime, const int nbytes);
void item_free(item *it);
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);

int  do_item_link(item *it, const uint32_t hv);     /** may fail if transgresses limits */
void do_item_unlink(item *it, const uint32_t hv);
void do_item_unlink_nolock(item *it, const uint32_t hv);
void do_item_remove(item *it);
void do_item_update(item *it);   /** update LRU time to current and reposition */
void do_item_update_nolock(item *it);
int  do_item_replace(item *it, item *new_it, const uint32_t hv);
char* dump(int * bytes_ret);
char * get_slabs(int * bytes);
int * do_get_active_slabs(int * count);
const char * get_server_id(void);
int get_exp(void);
int get_server(void);
void store_id(char * buf);
void preprocess_hashing(char * myid, int ret_count, int * retServers, int do_exp);

int item_is_flushed(item *it);

void do_item_linktail_q(item *it);
void do_item_unlinktail_q(item *it);
item *do_item_crawl_q(item *it);

/*@null@*/
void dump_slabs_pages(void);
void dump_slabs(void);
int cmpfunc(const void * a, const void * b);
unsigned int insertion_pt(unsigned long arr[], unsigned int size, unsigned long num);
unsigned int * find_top_k(unsigned int m, unsigned int n, unsigned int * sizes, unsigned long **arrs, unsigned int k);
unsigned long kth_smallest(unsigned long a[], unsigned int n, unsigned int k);
unsigned int * get_merging_points(int slabno, unsigned int count, unsigned long * my_lru, int myid, int * retiring_servers, int rts_count);
char *do_merge_all(int myid, int * bytes_ret, int * retiring_servers, int rts_count);
char *do_hash_all(char* buf, int * bytes, int self, int ret_count, int * retServers);
char *item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
char *item_cachedump_time(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
char * scores_all(int * bytes, int init, int ret);

void item_stats(ADD_STAT add_stats, void *c);
void do_item_stats_add_crawl(const int i, const uint64_t reclaimed,
        const uint64_t unfetched, const uint64_t checked);
void item_stats_totals(ADD_STAT add_stats, void *c);
/*@null@*/
void item_stats_sizes(ADD_STAT add_stats, void *c);
void item_stats_sizes_init(void);
void item_stats_sizes_enable(ADD_STAT add_stats, void *c);
void item_stats_sizes_disable(ADD_STAT add_stats, void *c);
void item_stats_sizes_add(item *it);
void item_stats_sizes_remove(item *it);
bool item_stats_sizes_status(void);

item *do_item_get(const char *key, const size_t nkey, const uint32_t hv, conn *c);
item *do_item_touch(const char *key, const size_t nkey, uint32_t exptime, const uint32_t hv, conn *c);
void item_stats_reset(void);
extern pthread_mutex_t lru_locks[POWER_LARGEST];

int start_lru_maintainer_thread(void);
int stop_lru_maintainer_thread(void);
int init_lru_maintainer(void);
void lru_maintainer_pause(void);
void lru_maintainer_resume(void);

#endif
