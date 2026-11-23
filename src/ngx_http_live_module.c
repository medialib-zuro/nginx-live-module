
/*
 * Copyright (C) Roman Arutyunyan
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>
#include "ngx_ts_hls.h"
#include "ngx_http_live_module.h"

#define GOP_COUNT_LIMIT         8

typedef struct {
    ngx_atomic_t                n_active;
    ngx_rbtree_t                ptree;
    ngx_rbtree_node_t           sentinel;
} ngx_http_live_sh_t;


typedef struct {
    ngx_rbtree_t                btree;
    ngx_rbtree_node_t           bsentinel;
    ngx_rbtree_t                gtree;
    ngx_rbtree_node_t           sentinel;
    ngx_http_live_sh_t         *sh;
    ngx_slab_pool_t            *shpool;
    ngx_uint_t                  max_conns;
    unsigned                    persistent:1;
    unsigned                    flush:1;
} ngx_http_live_t;


typedef struct {
    ngx_rbtree_node_t           node;
    ngx_pid_t                   pid;
    u_char                      md5[16];
} ngx_http_live_post_node_t;

typedef struct {
    ngx_rbtree_node_t           node;
    u_char                     *md5;
    ngx_http_request_t         *request;
} ngx_http_live_get_node_t;

typedef struct {
    ngx_rbtree_node_t           node;
    u_char                      md5[16];
    ngx_uint_t                  size;
    ngx_uint_t                  count;
    ngx_pool_t                 *pool;
    ngx_chain_t               **pbusy;
    ngx_chain_t               **pfree;
    ngx_chain_t                *busy;
    ngx_chain_t                *free;
    ngx_chain_t                *key;
    ngx_uint_t                  latest_counter;
    ngx_buf_t                  *ts_index_tables;
} ngx_http_live_gop_node_t;

typedef struct {
    size_t                      size;
    size_t                      kpos;
    ngx_http_live_t            *live;
    ngx_uint_t                  counter;
    ngx_uint_t                  flag;
    unsigned                    last;
    unsigned                    expired:1;
    u_char                      md5[16];
    u_char                      data[1];
} ngx_http_live_msg_t;


typedef struct {
    ngx_str_t                   key;
    u_char                      md5[16];
    ngx_buf_t                  *buf;
    ngx_chain_t                *in;
    ngx_chain_t                *out;
    ngx_chain_t                *free;
    ngx_chain_t                *busy;
    ngx_http_live_t            *live;
    ngx_http_live_post_node_t  *post;
    ngx_http_live_get_node_t   *get;
    ngx_http_request_t         *request;
    ngx_uint_t                  size; // for GET, partial read size of a msg
    ngx_uint_t                  counter; // for POST/GET, msg counter
    ngx_uint_t                  allocated;
    unsigned                    done:1;
    ngx_ts_stream_t            *ts;
    ngx_flag_t                  do_hls_live;
    ngx_flag_t                  do_hls_playback;
} ngx_http_live_ctx_t;


typedef struct {
    ngx_shm_zone_t             *shm_zone;
    ngx_uint_t                  methods;
    ngx_http_complex_value_t   *key;
    ngx_ts_hls_conf_t          *live_hls;
    ngx_ts_hls_conf_t          *playback_hls;
} ngx_http_live_loc_conf_t;

typedef struct {
    size_t                      relay_buffer_size;
    size_t                      buffer_size;
    size_t                      buffer_limit;
    ngx_bufs_t                  bufs;
    ngx_socket_t               *write_fd;
    ngx_socket_t               *read_fd;
    ngx_uint_t                  nfd;
    ngx_buf_t                  *buf;
    ngx_connection_t          **conn;
    ngx_http_live_t             *live;
    ngx_msec_t                  check_period;
    ngx_event_t                 check_event;
    ngx_flag_t                  gop_cache;  /* enabled:1; */
    ngx_flag_t                  live_hls_enabled;  /* enabled:1; */
    ngx_flag_t                  enabled;  /* enabled:1; */
} ngx_http_live_main_conf_t;

static ngx_int_t ngx_http_live_handler(ngx_http_request_t *r);

static ngx_int_t ngx_http_live_get(ngx_http_request_t *r);
static void ngx_http_live_get_cleanup(void *data);
static ngx_http_live_get_node_t *ngx_http_live_get_rbtree_lookup(
    ngx_rbtree_t *rbtree, u_char *md5);
static void ngx_http_live_get_rbtree_insert_value(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static void ngx_http_live_get_write_handler(ngx_http_request_t *r);

static ngx_http_live_gop_node_t *ngx_http_live_gop_node_alloc(ngx_pool_t *pool, ngx_log_t *log);
static void ngx_http_live_gop_node_cleanup(ngx_pool_t *pool, ngx_http_live_gop_node_t *gop);
static void ngx_http_live_gop_buf_handler(ngx_http_request_t *r,
        ngx_http_live_gop_node_t *gop);

static void ngx_http_live_gop_mark_expired(ngx_http_live_gop_node_t *gop, size_t limit);
static void ngx_http_live_gop_free_expired(ngx_http_live_gop_node_t *gop);
static ngx_int_t ngx_http_live_gop_append_buf(ngx_http_live_gop_node_t *gop,
        ngx_http_live_msg_t *msg, ngx_buf_t *b, void *tag);

static ngx_int_t ngx_http_live_post(ngx_http_request_t *r);
static void ngx_http_live_post_cleanup(void *data);
static ngx_http_live_post_node_t *ngx_http_live_post_rbtree_lookup(
    ngx_rbtree_t *rbtree, u_char *md5);
static void ngx_http_live_post_rbtree_insert_value(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static ngx_http_live_gop_node_t *ngx_http_live_gop_rbtree_lookup(
    ngx_rbtree_t *rbtree, u_char *md5);
static void ngx_http_live_gop_rbtree_insert_value(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static void ngx_http_live_post_read_handler(ngx_http_request_t *r);

static char *ngx_http_live_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_live_init_zone(ngx_shm_zone_t *shm_zone, void *data);
static char *ngx_http_live(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_live_active_check_handler(ngx_http_request_t *r);
static char *ngx_http_live_active_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_live_idle_check_handler(ngx_http_request_t *r);
static char *ngx_http_live_idle_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void *ngx_http_live_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_live_merge_loc_conf(ngx_conf_t *cf, void *parent,
        void *child);
static void *ngx_http_live_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_live_init_main_conf(ngx_conf_t *cf, void *conf);

static ngx_int_t ngx_http_live_init_module(ngx_cycle_t *cycle);
static void ngx_http_live_notify_cleanup(void *data);
static ngx_int_t ngx_http_live_init_process(ngx_cycle_t *cycle);
static void ngx_http_live_notify_read_handler(ngx_event_t *rev);
static void ngx_http_live_notify_process_gop(ngx_http_live_msg_t *msg, ngx_buf_t *b, size_t limit);
static void ngx_http_live_notify_process_msg(ngx_http_live_msg_t *msg);
static void ngx_http_live_notify_write_handler(ngx_event_t *wev);
static ngx_int_t ngx_http_live_notify(ngx_http_request_t *r, ngx_buf_t *b);

static ngx_conf_bitmask_t  ngx_http_live_methods_mask[] = {
    { ngx_string("get"), NGX_HTTP_GET },
    { ngx_string("post"), NGX_HTTP_POST },
    { ngx_null_string, 0 }
};


static ngx_command_t  ngx_http_live_commands[] = {

    {   ngx_string("live_zone"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_ANY,
        ngx_http_live_zone,
        NGX_HTTP_MAIN_CONF_OFFSET,
        0,
        NULL
    },

    {   ngx_string("live_relay_buffer_size"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_size_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_live_main_conf_t, relay_buffer_size),
        NULL
    },

    {   ngx_string("live_buffers"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE2,
        ngx_conf_set_bufs_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_live_main_conf_t, bufs),
        NULL
    },

    { ngx_string("live_gop_cache"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_flag_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_live_main_conf_t, gop_cache),
        NULL
    },

    { ngx_string("live_hls_enabled"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_flag_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_live_main_conf_t, live_hls_enabled),
        NULL
    },

    { ngx_string("live_active_check"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_live_active_check,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },

    { ngx_string("live_idle_check"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_live_idle_check,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },

    {   ngx_string("live"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_live,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },

    {   ngx_string("live_methods"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE12,
        ngx_conf_set_bitmask_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_live_loc_conf_t, methods),
        &ngx_http_live_methods_mask
    },

    {   ngx_string("live_key"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_set_complex_value_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_live_loc_conf_t, key),
        NULL
    },

    {   ngx_string("live_hls"),
        NGX_HTTP_LOC_CONF|NGX_CONF_1MORE,
        ngx_ts_hls_set_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_live_loc_conf_t, live_hls),
        NULL
    },

    {   ngx_string("playback_hls"),
        NGX_HTTP_LOC_CONF|NGX_CONF_1MORE,
        ngx_ts_hls_set_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_live_loc_conf_t, playback_hls),
        NULL
    },

    ngx_null_command
};


static ngx_http_module_t  ngx_http_live_module_ctx = {
    NULL,                           /* preconfiguration */
    NULL,                           /* postconfiguration */

    ngx_http_live_create_main_conf, /* create main configuration */
    ngx_http_live_init_main_conf,   /* init main configuration */

    NULL,                           /* create server configuration */
    NULL,                           /* merge server configuration */

    ngx_http_live_create_loc_conf,  /* create location configuration */
    ngx_http_live_merge_loc_conf    /* merge location configuration */
};


ngx_module_t  ngx_http_live_module = {
    NGX_MODULE_V1,
    &ngx_http_live_module_ctx,      /* module context */
    ngx_http_live_commands,         /* module directives */
    NGX_HTTP_MODULE,                /* module type */
    NULL,                           /* init master */
    ngx_http_live_init_module,      /* init module */
    ngx_http_live_init_process,     /* init process */
    NULL,                           /* init thread */
    NULL,                           /* exit thread */
    NULL,                           /* exit process */
    NULL,                           /* exit master */
    NGX_MODULE_V1_PADDING
};

static ngx_buf_t *
ngx_http_live_alloc_buf(ngx_log_t *log, size_t len)
{
    u_char                      *p;
    ngx_buf_t                   *buf;

    p = ngx_calloc(sizeof(ngx_buf_t) + len, log);
    if (p == NULL) {
        return NULL;
    }

    buf = (ngx_buf_t *) p;
    buf->start = p + sizeof(ngx_buf_t);
    buf->pos = buf->start;
    buf->last = buf->start;
    buf->end = buf->start + len;

    return buf;
}

ngx_flag_t ngx_http_live_actived(ngx_http_request_t *r)
{
    ngx_http_live_t            *live;
    ngx_http_live_loc_conf_t   *llcf;

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_live_module);

    if (llcf && llcf->shm_zone) {
        live = llcf->shm_zone->data;
        return live->max_conns ? (live->sh->n_active < live->max_conns) : 1;
    }

    return 1;
}


ngx_flag_t ngx_http_live_existed(ngx_http_request_t *r)
{
    u_char                     md5[16];
    ngx_md5_t                  md5_ctx;
    ngx_str_t                  key;
    ngx_http_live_t           *live;
    ngx_http_live_post_node_t *post;
    ngx_http_live_loc_conf_t  *llcf;

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_live_module);

    if (!llcf || !llcf->shm_zone) {
        return 0;
    }

    live = llcf->shm_zone->data;

    if (llcf->key == NULL) {
        key = r->uri;
    } else {
        if (ngx_http_complex_value(r, llcf->key, &key) != NGX_OK) {
            return 0;
        }
    }

    ngx_md5_init(&md5_ctx);
    ngx_md5_update(&md5_ctx, key.data, key.len);
    ngx_md5_final(md5, &md5_ctx);

    ngx_shmtx_lock(&live->shpool->mutex);
    post = ngx_http_live_post_rbtree_lookup(&live->sh->ptree, md5);
    ngx_shmtx_unlock(&live->shpool->mutex);

    if (post == NULL) {
        return 0;
    }

    return 1;
}

static ngx_int_t
ngx_http_live_handler(ngx_http_request_t *r)
{
    ngx_int_t                  rc;
    ngx_md5_t                  md5;
    ngx_http_live_ctx_t       *ctx;
    ngx_http_live_loc_conf_t  *llcf;

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_live_module);

    if (!(r->method & llcf->methods)) {
        return NGX_DECLINED;
    }

    if (llcf->shm_zone == NULL) {
        return NGX_DECLINED;
    }

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_live_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ngx_http_set_ctx(r, ctx, ngx_http_live_module);

    ctx->request = r;
    ctx->live = llcf->shm_zone->data;

    if (llcf->key == NULL) {
        ctx->key = r->uri;
    } else {
        if (ngx_http_complex_value(r, llcf->key, &ctx->key) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "live key: \"%V\"", &ctx->key);

    ngx_md5_init(&md5);
    ngx_md5_update(&md5, ctx->key.data, ctx->key.len);
    ngx_md5_final(ctx->md5, &md5);

#if (NGX_DEBUG)
    {
        u_char  hex[32];
        ngx_hex_dump(hex, ctx->md5, 16);
        ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "live md5: %*s", 32, hex);
    }
#endif

    switch (r->method) {
    case NGX_HTTP_GET:
        rc = ngx_http_live_get(r);
        break;
    default: /* NGX_HTTP_POST */
        rc = ngx_http_live_post(r);
        break;
    }

    return rc;
}

static ngx_int_t
ngx_http_live_get(ngx_http_request_t *r)
{
    ngx_int_t                  rc;
    ngx_http_live_t           *live;
    ngx_pool_cleanup_t        *cln;
    ngx_http_live_ctx_t       *ctx;
    ngx_http_live_post_node_t *post = NULL;
    ngx_http_live_get_node_t  *get;

    ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0, "live get");

    rc = ngx_http_discard_request_body(r);
    if (rc != NGX_OK) {
        return rc;
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);

    live = ctx->live;

    if (!live->persistent) {
        ngx_shmtx_lock(&live->shpool->mutex);
        post = ngx_http_live_post_rbtree_lookup(&live->sh->ptree, ctx->md5);
        ngx_shmtx_unlock(&live->shpool->mutex);

        if (post == NULL) {
            ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
                          "live stream not found");
            return NGX_HTTP_NOT_FOUND;
        }
    }

    cln = ngx_pool_cleanup_add(r->pool, 0);
    if (cln == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    cln->handler = ngx_http_live_get_cleanup;
    cln->data = ctx;

    get = ngx_pcalloc(r->pool, sizeof(ngx_http_live_get_node_t));
    if (get == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    get->md5 = ctx->md5;
    get->request = r;
#if (NGX_DEBUG)
    {
        u_char  hex[32];
        ngx_hex_dump(hex, get->md5, 16);
        ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0, "insert get %*s", 32, hex);
    }
#endif
    ngx_rbtree_insert(&live->gtree, &get->node);

    ctx->get = get;

    r->headers_out.status = 200;
    r->headers_out.content_length_n = -1;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    r->write_event_handler = ngx_http_live_get_write_handler;

    r->main->count++;
    live->sh->n_active++;

    return NGX_DONE;
}


static void
ngx_http_live_get_cleanup(void *data)
{
    ngx_http_live_ctx_t *ctx = data;

    if (ctx->get) {
#if (NGX_DEBUG)
        {
            u_char  hex[32];
            ngx_hex_dump(hex, ctx->get->md5, 16);
            ngx_log_error(NGX_LOG_NOTICE, ctx->request->connection->log, 0, "delete get %*s", 32, hex);
        }
#endif
        ctx->live->sh->n_active--;
        ngx_rbtree_delete(&ctx->live->gtree, &ctx->get->node);
    }
}

static ngx_http_live_get_node_t *
ngx_http_live_get_rbtree_lookup(ngx_rbtree_t *rbtree, u_char *md5)
{
    ngx_int_t                  rc;
    ngx_rbtree_node_t         *node, *sentinel;
    ngx_http_live_get_node_t  *get, *ret;

    /* this function returns the leftmost node having the given key */

    node = rbtree->root;
    sentinel = rbtree->sentinel;
    ret = NULL;

    while (node != sentinel) {
        get = (ngx_http_live_get_node_t *) node;

        rc = ngx_memcmp(md5, get->md5, 16);

        if (rc < 0) {
            node = node->left;
            continue;
        }

        if (rc > 0) {
            node = node->right;
            continue;
        }

        ret = get;
        node = node->left;
    }

    return ret;
}

static void
ngx_http_live_get_rbtree_insert_value(ngx_rbtree_node_t *temp,
                                      ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_rbtree_node_t         **p;
    ngx_http_live_get_node_t   *get, *gett;

    get = (ngx_http_live_get_node_t *) node;

    for ( ;; ) {
        gett = (ngx_http_live_get_node_t *) temp;

        p = ngx_memcmp(get->md5, gett->md5, 16) < 0
            ? &temp->left : &temp->right;

        if (*p == sentinel) {
            break;
        }

        temp = *p;
    }

    *p = node;
    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}


static void
ngx_http_live_get_write_handler(ngx_http_request_t *r)
{
    ngx_int_t                  rc;
    ngx_event_t               *wev;
    ngx_connection_t          *c;
    ngx_http_live_ctx_t       *ctx;
    ngx_http_core_loc_conf_t  *clcf;

    ngx_log_debug1(NGX_LOG_DEBUG, r->connection->log, 0,
                  "%s", __func__);

    ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);

    c = r->connection;
    wev = c->write;

    clcf = ngx_http_get_module_loc_conf(r->main, ngx_http_core_module);

    if (wev->timedout) {
        ngx_log_error(NGX_LOG_NOTICE, c->log, NGX_ETIMEDOUT, "client timed out");
        c->timedout = 1;

        ngx_http_finalize_request(r, NGX_HTTP_REQUEST_TIME_OUT);
        return;
    }

    if (wev->delayed) {
        ngx_log_error(NGX_LOG_INFO, c->log, 0, "live writer delayed");

        if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
            ngx_http_finalize_request(r, NGX_ERROR);
        }

        return;
    }

    rc = ngx_http_output_filter(r, ctx->out);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "live output filter rc:%i", rc);

    if (rc == NGX_ERROR) {
        ngx_http_finalize_request(r, rc);
        return;
    }

    ngx_chain_update_chains(r->pool, &ctx->free, &ctx->busy,
                            &ctx->out, &ngx_http_live_module);

    if (ctx->done) {
        ngx_log_error(NGX_LOG_NOTICE, c->log, 0,
                      "live close stream");
        ngx_http_finalize_request(r, NGX_DONE);
        return;
    }

    if (!wev->delayed) {
        if (r->buffered || r->postponed || (r == r->main && c->buffered)) {
            ngx_add_timer(wev, clcf->send_timeout);
        } else if (wev->timer_set) {
            ngx_del_timer(wev);
        }
    }

    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
        ngx_http_finalize_request(r, NGX_ERROR);
    }
}

static ngx_int_t
ngx_http_live_post(ngx_http_request_t *r)
{
    size_t                      len;
    ngx_pid_t                   gid;
    ngx_int_t                   rc;
    ngx_http_live_t            *live;
    ngx_pool_cleanup_t         *cln;
    ngx_http_live_ctx_t        *ctx;
    ngx_http_live_post_node_t  *post;
    ngx_http_live_main_conf_t  *lmcf;
    ngx_http_live_loc_conf_t   *llcf;
    ngx_uint_t                  n;
    ngx_str_t                   do_hls;
    ngx_str_t                   stream_name;
    ngx_http_live_msg_t         *msg;

    ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);
    ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0, "live post");

    lmcf = ngx_http_get_module_main_conf(r, ngx_http_live_module);

    cln = ngx_pool_cleanup_add(r->pool, 0);
    if (cln == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    cln->handler = ngx_http_live_post_cleanup;
    cln->data = ctx;

    len = lmcf->buffer_size;
    ctx->buf = ngx_http_live_alloc_buf(r->connection->log, len);
    if (ctx->buf == NULL) {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "failed alloc live post buf");
        return NGX_ERROR;
    }

    ctx->do_hls_live = lmcf->live_hls_enabled;
    ctx->buf->tag = (ngx_buf_tag_t *) r;
    ctx->buf->last = ctx->buf->pos + offsetof(ngx_http_live_msg_t, data);
    msg = (ngx_http_live_msg_t *) ctx->buf->pos;
    msg->live = ctx->live;
    ngx_memcpy(msg->md5, ctx->md5, 16);

    live = ctx->live;
    ngx_shmtx_lock(&live->shpool->mutex);

    post = ngx_http_live_post_rbtree_lookup(&live->sh->ptree, ctx->md5);
    if (post) {
        gid = getpgid(post->pid);
        if (gid == -1) {
            ngx_rbtree_delete(&live->sh->ptree, &post->node);
            ngx_slab_free_locked(live->shpool, post);
        } else {
            ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                        "live stream \"%V\" already exists on worker:%P", &ctx->key, post->pid);
            ngx_shmtx_unlock(&live->shpool->mutex);
            return NGX_HTTP_CONFLICT;
        }
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                      "drop live stream \"%V\" on exited worker:%P", &ctx->key, post->pid);
    }

    post = ngx_slab_calloc_locked(live->shpool, sizeof(ngx_http_live_post_node_t));
    if (post == NULL) {
        ngx_shmtx_unlock(&live->shpool->mutex);
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_memcpy(post->md5, ctx->md5, 16);
    post->pid = ngx_log_pid;

#if (NGX_DEBUG)
    {
        u_char  hex[32];
        ngx_hex_dump(hex, post->md5, 16);
        ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0, "insert post %*s", 32, hex);
    }
#endif
    ngx_rbtree_insert(&live->sh->ptree, &post->node);

    ngx_shmtx_unlock(&live->shpool->mutex);

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_live_module);

    if (lmcf->gop_cache || llcf->live_hls || llcf->playback_hls) {
        ctx->ts = ngx_pcalloc(r->pool, sizeof(ngx_ts_stream_t));
        if (ctx->ts == NULL) {
            return NGX_ERROR;
        }

        ctx->ts->pool = r->pool;
        ctx->ts->log = r->connection->log;
    }

    if (ngx_http_arg(r, (u_char *) "playback_hls", sizeof("playback_hls") - 1, &do_hls) == NGX_OK) {
        if (ngx_strncmp(do_hls.data, "true", ngx_max(do_hls.len, 4)) == 0) {
            ctx->do_hls_playback = 1;
        }
    }
    if (ngx_http_arg(r, (u_char *) "live_hls", sizeof("live_hls") - 1, &do_hls) == NGX_OK) {
        if (ngx_strncmp(do_hls.data, "true", ngx_max(do_hls.len, 4)) == 0) {
            ctx->do_hls_live = 1;
        }
    }

    if (llcf->live_hls || llcf->playback_hls) {
        for (n = 0; n < r->uri.len; n++) {
            if (r->uri.data[r->uri.len - 1 - n] == '/') {
                break;
            }
        }

        stream_name.data = &r->uri.data[r->uri.len - n];
        stream_name.len = n;
        if (ctx->do_hls_live)
            ngx_ts_hls_create(llcf->live_hls, ctx->ts, &stream_name);
        if (ctx->do_hls_playback)
            ngx_ts_hls_create(llcf->playback_hls, ctx->ts, &stream_name);
    }

    ctx->post = post;

    r->request_body_no_buffering = 1;

    rc = ngx_http_read_client_request_body(r, ngx_http_live_post_read_handler);

    if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
        return rc;
    }

    return NGX_DONE;
}


static void
ngx_http_live_post_cleanup(void *data)
{
    ngx_http_live_ctx_t *ctx = data;

    size_t                n;
    ngx_buf_t            *b;
    ngx_http_live_t      *live;
    ngx_http_live_msg_t  *msg;
    ngx_http_live_main_conf_t   *lmcf;

    live = ctx->live;
    if (ctx->post) {

        n = offsetof(ngx_http_live_msg_t, data);
        b = ngx_http_live_alloc_buf(ngx_cycle->log, n);

        if (b) {
            b->last = b->pos + n;
            msg = (ngx_http_live_msg_t *) b->pos;
            msg->size = n;
            msg->live = live;
            msg->counter = ctx->counter++;
            msg->last = 1;
            ngx_memcpy(msg->md5, ctx->md5, 16);

            lmcf = ngx_http_get_module_main_conf(ctx->request, ngx_http_live_module);
            if (lmcf->gop_cache) {
                ngx_http_live_notify_process_gop(msg, b, lmcf->buffer_limit);
            } else {
                ngx_http_live_notify_process_msg(msg);
            }
            if (ngx_http_live_notify(ctx->request, b) != NGX_OK) {
                ngx_log_error(NGX_LOG_NOTICE, ctx->request->connection->log, 0, "last live notify error");
            }
        }

        ngx_shmtx_lock(&live->shpool->mutex);
#if (NGX_DEBUG)
        {
            u_char  hex[32];
            ngx_hex_dump(hex, ctx->post->md5, 16);
            ngx_log_error(NGX_LOG_NOTICE, ctx->request->connection->log, 0, "delete post %*s", 32, hex);
        }
#endif
        ngx_rbtree_delete(&live->sh->ptree, &ctx->post->node);
        ngx_slab_free_locked(live->shpool, ctx->post);
        ngx_shmtx_unlock(&live->shpool->mutex);
    }

    if (ctx->buf) {
        if (ctx->buf->file_pos) {
            ctx->buf->tag = NULL;
        }
    }
}


static ngx_http_live_post_node_t *
ngx_http_live_post_rbtree_lookup(ngx_rbtree_t *rbtree, u_char *md5)
{
    ngx_int_t              	     rc;
    ngx_rbtree_node_t     	    *node, *sentinel;
    ngx_http_live_post_node_t   *n;

    node = rbtree->root;
    sentinel = rbtree->sentinel;

    while (node != sentinel) {
        n = (ngx_http_live_post_node_t *) node;

        rc = ngx_memcmp(md5, n->md5, 16);

        if (rc < 0) {
            node = node->left;
            continue;
        }

        if (rc > 0) {
            node = node->right;
            continue;
        }

        return n;
    }

    return NULL;
}


static void
ngx_http_live_post_rbtree_insert_value(ngx_rbtree_node_t *temp,
                                       ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_rbtree_node_t          **p;
    ngx_http_live_post_node_t   *n, *nt;

    n = (ngx_http_live_post_node_t *) node;

    for ( ;; ) {
        nt = (ngx_http_live_post_node_t *) temp;

        p = ngx_memcmp(n->md5, nt->md5, 16) < 0 ? &temp->left : &temp->right;

        if (*p == sentinel) {
            break;
        }

        temp = *p;
    }

    *p = node;
    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}

static ngx_http_live_gop_node_t *
ngx_http_live_gop_rbtree_lookup(ngx_rbtree_t *rbtree, u_char *md5)
{
    ngx_int_t              	     rc;
    ngx_rbtree_node_t     	    *node, *sentinel;
    ngx_http_live_gop_node_t    *n;

    node = rbtree->root;
    sentinel = rbtree->sentinel;

    while (node != sentinel) {
        n = (ngx_http_live_gop_node_t *) node;

        rc = ngx_memcmp(md5, n->md5, 16);

        if (rc < 0) {
            node = node->left;
            continue;
        }

        if (rc > 0) {
            node = node->right;
            continue;
        }

        return n;
    }

    return NULL;
}


static void
ngx_http_live_gop_rbtree_insert_value(ngx_rbtree_node_t *temp,
                                       ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_rbtree_node_t          **p;
    ngx_http_live_gop_node_t    *n, *nt;

    n = (ngx_http_live_gop_node_t *) node;

    for ( ;; ) {
        nt = (ngx_http_live_gop_node_t *) temp;

        p = ngx_memcmp(n->md5, nt->md5, 16) < 0 ? &temp->left : &temp->right;

        if (*p == sentinel) {
            break;
        }

        temp = *p;
    }

    *p = node;
    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}

static ngx_int_t
ngx_http_live_post_ts_index_tables(ngx_http_request_t *r)
{
    size_t                      len;
    ngx_buf_t                   *buf;
    ngx_chain_t                 *cl;
    ngx_http_live_ctx_t         *ctx;
    ngx_http_live_msg_t         *msg;
    ngx_http_live_main_conf_t   *lmcf;

    len = 0;
    ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);
    lmcf = ngx_http_get_module_main_conf(r, ngx_http_live_module);

    if (ngx_ts_build_index_tables(ctx->ts) != NGX_OK) {
        return NGX_ERROR;
    }

    len = offsetof(ngx_http_live_msg_t, data);
    for (cl = ctx->ts->index_tables; cl; cl = cl->next) {
        len += cl->buf->last - cl->buf->pos;
    }

    buf = ngx_http_live_alloc_buf(r->connection->log, len);
    if (buf == NULL) {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "failed alloc live ts index buf:%ui", len);
        return NGX_ERROR;
    }

    buf->last = buf->pos + offsetof(ngx_http_live_msg_t, data);
    msg = (ngx_http_live_msg_t *) buf->pos;
    msg->live = ctx->live;
    msg->flag = PKT_TS_INDEX;
    ngx_memcpy(msg->md5, ctx->md5, 16);

    for (cl = ctx->ts->index_tables; cl; cl = cl->next) {
        ngx_memmove(buf->last, cl->buf->pos, cl->buf->last - cl->buf->pos);
        buf->last += cl->buf->last - cl->buf->pos;
    }
    msg->size = buf->last - buf->pos;

    ngx_http_live_notify_process_gop(msg, buf, lmcf->buffer_limit);

    return ngx_http_live_notify(r, buf);
}

static void
ngx_http_live_post_read_handler(ngx_http_request_t *r)
{
    size_t                       n, left, kpos;
    ngx_int_t                    rc;
    ngx_uint_t                   flag;
    ngx_buf_t                   *b;
    ngx_chain_t                 *cl;
    ngx_event_t                 *rev;
    ngx_http_live_msg_t         *msg;
    ngx_http_live_ctx_t         *ctx;
    ngx_http_request_body_t     *rb;
    ngx_http_live_main_conf_t   *lmcf;

    ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);
    lmcf = ngx_http_get_module_main_conf(r, ngx_http_live_module);

    if (ngx_exiting || ngx_terminate) {
        ngx_http_finalize_request(r, NGX_HTTP_CLOSE);
        return;
    }

    rev = r->connection->read;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                  "live post read handler d:%d, r:%O",
                  rev->delayed, ctx->buf->file_pos);

    if (rev->delayed || ctx->buf->file_pos) {
        if (ngx_handle_read_event(rev, 0) != NGX_OK) {
            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        }
        return;
    }

    rb = r->request_body;
    if (rb == NULL) {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    flag = 0;
    b = ctx->buf;
    msg = (ngx_http_live_msg_t *) b->pos;

    if (b->last == b->end) {
        b->pos = b->start;
        b->last = b->pos + offsetof(ngx_http_live_msg_t, data);
        msg->flag = 0;
    }

    for ( ;r->reading_body; ) {
        if (rb->bufs == NULL) {
            rc = ngx_http_read_unbuffered_request_body(r);

            if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
                ngx_http_finalize_request(r, rc);
                return;
            }
        }

        if (rb->bufs == NULL) {
            break;
        }

        cl = rb->bufs;
        n = cl->buf->last - cl->buf->pos;
        left = b->end - b->last;

        if (n > left) {
            n = left;
        }

        b->last = ngx_cpymem(b->last, cl->buf->pos, n);
        cl->buf->pos += n;

        if (b->last == b->end) {
            break;
        }

        rb->bufs = cl->next;
        ngx_free_chain(r->pool, cl);
    }

    if (b->last == b->end) {
        kpos = 0;
        if (ctx->ts) {
            ngx_ts_read_buf(ctx->ts, &msg->data[0], b->end - b->pos - offsetof(ngx_http_live_msg_t, data), &flag, &kpos);
        }

        if (!ctx->ts->index_tables && lmcf->gop_cache && flag == PKT_KEY_FLAG) {
            // send pat pmt first
            ngx_http_live_post_ts_index_tables(r);
        }

        msg->size = b->last - b->pos;
        msg->kpos = kpos;
        msg->counter = ctx->counter++;
        msg->expired = 0;
        msg->last = (rb->bufs || r->reading_body) ? 0 : 1;
        msg->flag = flag;

        if (lmcf->gop_cache) {
            ngx_http_live_notify_process_gop(msg, b, lmcf->buffer_limit);
        } else {
            ngx_http_live_notify_process_msg(msg);
        }
        rc = ngx_http_live_notify(r, b);
        if (rc != NGX_OK) {
            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }
    }


    if (r->reading_body) {
        if (rb->bufs) {
            r->read_event_handler = ngx_http_live_post_read_handler;
            ngx_add_timer(rev, 1);
            rev->delayed = 1;
            return;
        }
        r->read_event_handler = ngx_http_live_post_read_handler;
        return;
    }

    ngx_http_finalize_request(r, NGX_HTTP_CLOSE);
}


static char *
ngx_http_live_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_live_main_conf_t *lmcf = conf;

    size_t            size;
    u_char           *p;
    ngx_str_t         s, name, *value;
    ngx_uint_t        i, max_conns;
    ngx_shm_zone_t   *shm_zone;
    ngx_http_live_t  *live;

    live = ngx_pcalloc(cf->pool, sizeof(ngx_http_live_t));
    if (live == NULL) {
        return NGX_CONF_ERROR;
    }

    ngx_rbtree_init(&live->gtree, &live->sentinel,
                    ngx_http_live_get_rbtree_insert_value);
    ngx_rbtree_init(&live->btree, &live->bsentinel,
                    ngx_http_live_gop_rbtree_insert_value);

    name.len = 0;
    size = 0;
    max_conns = 0;

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "zone=", 5) == 0) {

            name.data = value[i].data + 5;

            p = (u_char *) ngx_strchr(name.data, ':');

            if (p) {
                name.len = p - name.data;

                p++;

                s.len = value[i].data + value[i].len - p;
                s.data = p;

                size = ngx_parse_size(&s);
                if (size > 8191) {
                    continue;
                }
            }

            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "invalid zone size \"%V\"", &value[i]);
            return NGX_CONF_ERROR;
        }

        if (ngx_strncmp(value[i].data, "max_conns=", 10) == 0) {
            s.len = value[i].len - 10;
            s.data = value[i].data + 10;

            max_conns = ngx_atoi(s.data, s.len);
            if (max_conns != (ngx_uint_t) NGX_ERROR) {
                live->max_conns = max_conns;
                continue;
            }
        }

        if (ngx_strcmp(value[i].data, "persistent") == 0) {
            live->persistent = 1;
            continue;
        }

        if (ngx_strcmp(value[i].data, "flush") == 0) {
            live->flush = 1;
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    if (name.len == 0 || size == 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "live_path must have \"zone\" parameter");
        return NGX_CONF_ERROR;
    }

    shm_zone = ngx_shared_memory_add(cf, &name, size, &ngx_http_live_module);
    if (shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    if (shm_zone->data) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "duplicate zone \"%V\"", &name);
        return NGX_CONF_ERROR;
    }

    shm_zone->init = ngx_http_live_init_zone;
    shm_zone->data = live;

    shm_zone->noreuse = 1;

    lmcf->enabled = 1;
    lmcf->live = live;

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_live_init_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    size_t            len;
    ngx_http_live_t  *live;

    live = shm_zone->data;

    live->shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

    if (shm_zone->shm.exists) {
        live->sh = live->shpool->data;
        return NGX_OK;
    }

    live->sh = ngx_slab_calloc(live->shpool, sizeof(ngx_http_live_sh_t));
    if (live->sh == NULL) {
        return NGX_ERROR;
    }

    live->shpool->data = live->sh;

    ngx_rbtree_init(&live->sh->ptree, &live->sh->sentinel,
                    ngx_http_live_post_rbtree_insert_value);

    len = sizeof(" in live zone \"\"") + shm_zone->shm.name.len;

    live->shpool->log_ctx = ngx_slab_calloc(live->shpool, len);
    if (live->shpool->log_ctx == NULL) {
        return NGX_ERROR;
    }

    ngx_sprintf(live->shpool->log_ctx, " in live zone \"%V\"%Z",
                &shm_zone->shm.name);

    return NGX_OK;
}


static ngx_int_t
ngx_http_set_header_helper(ngx_http_request_t *r,
    ngx_str_t *key, ngx_str_t *value,
    ngx_table_elt_t **output_header, ngx_flag_t no_create)
{
    ngx_table_elt_t             *h;
    ngx_list_part_t             *part;
    ngx_uint_t                   i, hash;
    ngx_flag_t                   matched = 0;

    hash = ngx_hash_key(value->data, value->len);
    part = &r->headers_out.headers.part;
    h = part->elts;

    for (i = 0; /* void */; i++) {

        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            h = part->elts;
            i = 0;
        }

        if (h[i].hash == 0) {
            continue;
        }

        if (h[i].key.len == key->len
            && ngx_strncasecmp(h[i].key.data, key->data,
                               h[i].key.len) == 0)
        {
            goto matched;
        }

        if (h[i].key.len >= key->len - 1
            && ngx_strncasecmp(h[i].key.data, key->data,
                               key->len - 1) == 0)
        {
            goto matched;
        }

        /* not matched */
        continue;

matched:

        if (value->len == 0 || matched) {

            h[i].value.len = 0;
            h[i].hash = 0;

        } else {
            h[i].value = *value;
            h[i].hash = hash;
        }

        if (output_header) {
            *output_header = &h[i];
        }

        matched = 1;
    }

    if (matched){
        return NGX_OK;
    }

    if (no_create && value->len == 0) {
        return NGX_OK;
    }

    /* XXX we still need to create header slot even if the value
     * is empty because some builtin headers like Last-Modified
     * relies on this to get cleared */

    h = ngx_list_push(&r->headers_out.headers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    if (value->len == 0) {
        h->hash = 0;

    } else {
        h->hash = hash;
    }

    h->key = *key;
    h->value = *value;

    h->lowcase_key = ngx_pnalloc(r->pool, h->key.len);
    if (h->lowcase_key == NULL) {
        return NGX_ERROR;
    }

    ngx_strlow(h->lowcase_key, h->key.data, h->key.len);

    if (output_header) {
        *output_header = h;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_live_active_check_handler(ngx_http_request_t *r)
{
    u_char                             *pos;
    ngx_str_t                           key, value;
    ngx_int_t                           rc;
    ngx_buf_t                          *b;
    ngx_uint_t                          num;
    ngx_chain_t                         out;
    ngx_http_live_t                    *live;
    ngx_http_live_loc_conf_t           *llcf;

    if (r->method != NGX_HTTP_GET && r->method != NGX_HTTP_HEAD) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    rc = ngx_http_discard_request_body(r);

    if (rc != NGX_OK) {
        return rc;
    }

    r->headers_out.status = NGX_HTTP_OK;

    num = 0;
    llcf = ngx_http_get_module_loc_conf(r, ngx_http_live_module);

    if (llcf && llcf->shm_zone) {
        live = llcf->shm_zone->data;
        num = live->sh->n_active;
    }

    ngx_str_set(&key, "Conns");
    value.data = ngx_pnalloc(r->pool, sizeof(ngx_uint_t) + 1);
    if (value.data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    pos = ngx_sprintf(value.data, "%ui", num);
    value.len = pos - value.data;
    ngx_http_set_header_helper(r, &key, &value, NULL, 0);

    if (r->method == NGX_HTTP_HEAD) {
        rc = ngx_http_send_header(r);
        return rc;
    }

    b = ngx_create_temp_buf(r->pool, sizeof(ngx_uint_t) + 1);
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    b->last = ngx_sprintf(b->last, "%ui", num);

    out.buf = b;
    out.next = NULL;

    r->headers_out.content_length_n = b->last - b->pos;

    b->last_buf = 1;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    return ngx_http_output_filter(r, &out);
}

static char *
ngx_http_live_active_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                 *value;
    ngx_http_core_loc_conf_t  *clcf;
    ngx_http_live_loc_conf_t  *llcf = conf;

    value = cf->args->elts;

    llcf->shm_zone = ngx_shared_memory_add(cf, &value[1], 0,
                                           &ngx_http_live_module);
    if (llcf->shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_live_active_check_handler;

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_live_idle_check_handler(ngx_http_request_t *r)
{
    u_char                             *pos;
    ngx_str_t                           key, value;
    ngx_int_t                           rc;
    ngx_buf_t                          *b;
    ngx_uint_t                          num, idle, max_conns;
    ngx_chain_t                         out;
    ngx_http_live_t                    *live;
    ngx_http_live_loc_conf_t           *llcf;

    if (r->method != NGX_HTTP_GET && r->method != NGX_HTTP_HEAD) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    rc = ngx_http_discard_request_body(r);

    if (rc != NGX_OK) {
        return rc;
    }

    num = 0;
    idle = 100;
    max_conns = 0;
    llcf = ngx_http_get_module_loc_conf(r, ngx_http_live_module);

    if (llcf && llcf->shm_zone) {
        live = llcf->shm_zone->data;
        num = live->sh->n_active;
        max_conns = live->max_conns;
        idle = (max_conns && num > max_conns) ? 0 : (100 - num * 100 / max_conns);
    }

    r->headers_out.status = (max_conns && num > max_conns) ? NGX_HTTP_TOO_MANY_REQUESTS : NGX_HTTP_OK;

    ngx_str_set(&key, "Idle");
    value.data = ngx_pnalloc(r->pool, sizeof(ngx_uint_t) + 1);
    if (value.data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    pos = ngx_sprintf(value.data, "%ui", idle);
    value.len = pos - value.data;
    ngx_http_set_header_helper(r, &key, &value, NULL, 0);

    if (r->method == NGX_HTTP_HEAD) {
        rc = ngx_http_send_header(r);
        return rc;
    }

    b = ngx_create_temp_buf(r->pool, sizeof(ngx_uint_t) + 1);
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    b->last = ngx_sprintf(b->last, "%ui", idle);

    out.buf = b;
    out.next = NULL;

    r->headers_out.content_length_n = b->last - b->pos;

    b->last_buf = 1;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    return ngx_http_output_filter(r, &out);
}

static char *
ngx_http_live_idle_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                 *value;
    ngx_http_core_loc_conf_t  *clcf;
    ngx_http_live_loc_conf_t  *llcf = conf;

    value = cf->args->elts;

    llcf->shm_zone = ngx_shared_memory_add(cf, &value[1], 0,
                                           &ngx_http_live_module);
    if (llcf->shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_live_idle_check_handler;

    return NGX_CONF_OK;
}

static char *
ngx_http_live(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_live_loc_conf_t *llcf = conf;

    ngx_str_t                 *value;
    ngx_http_core_loc_conf_t  *clcf;

    value = cf->args->elts;

    llcf->shm_zone = ngx_shared_memory_add(cf, &value[1], 0,
                                           &ngx_http_live_module);
    if (llcf->shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

    clcf->handler = ngx_http_live_handler;

    return NGX_CONF_OK;
}

static void *
ngx_http_live_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_live_loc_conf_t  *llcf;

    llcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_live_loc_conf_t));
    if (llcf == NULL) {
        return NULL;
    }

    llcf->live_hls = NGX_CONF_UNSET_PTR;
    llcf->playback_hls = NGX_CONF_UNSET_PTR;

    /*
     * set by ngx_pcalloc():
     *
     *     llcf->shm_zone = NULL;
     *     llcf->methods = 0;
     *     llcf->key = NULL;
     */

    return llcf;
}


static char *
ngx_http_live_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_live_loc_conf_t *prev = parent;
    ngx_http_live_loc_conf_t *conf = child;

    ngx_conf_merge_bitmask_value(conf->methods, prev->methods,
                                 (NGX_CONF_BITMASK_SET|NGX_HTTP_GET));

    if (conf->key == NULL) {
        conf->key = prev->key;
    }

    ngx_conf_merge_ptr_value(conf->live_hls, prev->live_hls, NULL);
    ngx_conf_merge_ptr_value(conf->playback_hls, prev->playback_hls, NULL);

    return NGX_CONF_OK;
}


static void *
ngx_http_live_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_live_main_conf_t  *lmcf;

    lmcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_live_main_conf_t));
    if (lmcf == NULL) {
        return NULL;
    }

    lmcf->relay_buffer_size = NGX_CONF_UNSET_SIZE;
    lmcf->buffer_size = NGX_CONF_UNSET_SIZE;
    lmcf->gop_cache = NGX_CONF_UNSET;
    lmcf->live_hls_enabled = NGX_CONF_UNSET;
    lmcf->check_period = NGX_CONF_UNSET_MSEC;

    /*
     * set by ngx_pcalloc()
     *
     *     lmcf->write_fd = NULL;
     *     lmcf->read_fd = NULL;
     *     lmcf->nfd = 0;
     *     lmcf->buf = NULL;
     *     lmcf->enabled = 0;
     *
     */

    return lmcf;
}


static char *
ngx_http_live_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_live_main_conf_t *lmcf = conf;

    ngx_conf_init_value(lmcf->gop_cache, 1);
    ngx_conf_init_value(lmcf->live_hls_enabled, 0);
    ngx_conf_init_value(lmcf->bufs.num, 128);
    ngx_conf_init_size_value(lmcf->bufs.size, 1316 * 3);
    ngx_conf_init_size_value(lmcf->relay_buffer_size, 102400);
    ngx_conf_init_size_value(lmcf->buffer_size, 1316 * 3);
    ngx_conf_init_size_value(lmcf->buffer_limit, 1316 * 3 * 384);
    ngx_conf_init_msec_value(lmcf->check_period, 10000);

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_live_init_module(ngx_cycle_t *cycle)
{
    int                         value;
    size_t                      size;
    socklen_t                   olen;
    ngx_uint_t                  n, nfd;
    ngx_socket_t               *fd, fds[2];
    ngx_core_conf_t            *ccf;
    ngx_pool_cleanup_t         *cln;
    ngx_http_live_main_conf_t  *lmcf;

    lmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_live_module);

    if (lmcf == NULL || !lmcf->enabled) {
        return NGX_OK;
    }

    ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);

    cln = ngx_pool_cleanup_add(cycle->pool, 0);
    if (cln == NULL) {
        return NGX_ERROR;
    }

    cln->handler = ngx_http_live_notify_cleanup;
    cln->data = lmcf;

    size = 0;
    nfd = ccf->master ? ccf->worker_processes : 0;

    if (nfd > 0) {
        fd = ngx_pcalloc(cycle->pool, nfd * 2 * sizeof(ngx_socket_t));
        if (fd == NULL) {
            return NGX_ERROR;
        }

        lmcf->read_fd = fd;
        lmcf->write_fd = fd + nfd;
    }

    for (n = 0; n < nfd; n++) {
        if (socketpair(AF_UNIX,
#if !defined(SOCK_SEQPACKET) || (NGX_DARWIN)
                       SOCK_DGRAM,
#else
                       SOCK_SEQPACKET,
#endif
                       0, fds))
        {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_socket_errno,
                          "socketpair() failed");
            return NGX_ERROR;
        }

        ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                      "live socketpair p:%d, r:%d <= w:%d",
                      n, (int) fds[0], (int) fds[1]);

        lmcf->read_fd[n] = fds[0];
        lmcf->write_fd[n] = fds[1];

        lmcf->nfd++;

        if (ngx_nonblocking(fds[0]) == -1
                || ngx_nonblocking(fds[1]) == -1)
        {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_socket_errno,
                          ngx_nonblocking_n " failed");
            return NGX_ERROR;
        }

        if (lmcf->relay_buffer_size) {
            value = lmcf->relay_buffer_size;

            if (setsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &value, sizeof(int))
                    == -1)
            {
                ngx_log_error(NGX_LOG_CRIT, cycle->log, ngx_socket_errno,
                              "setsockopt(SO_SNDBUF) live notify failed");
                return NGX_ERROR;
            }
        }

        if (size == 0) {
            olen = sizeof(int);

            if (getsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &value, &olen) == -1) {
                ngx_log_error(NGX_LOG_CRIT, cycle->log, ngx_socket_errno,
                              "getsockopt(SO_SNDBUF) live notify failed");
                return NGX_ERROR;
            }

            if (value <= 64) {
                ngx_log_error(NGX_LOG_CRIT, cycle->log, 0,
                              "too small live notify buffer");
                return NGX_ERROR;
            }

            /*
             * On BSD/Macos buffer_size is the full socket queue length.
             * It should not exceed (net.local.dgram.recvspace - 16).
             */

            size = value;

#if (NGX_LINUX)
            /*
             * On Linux buffer_size is max length of a single packet.
             * It is 32 bytes less than SO_SNDBUF returned by setsockopt().
             */

            size -= 32;
#endif

            if (size < sizeof(ngx_http_live_msg_t)) {
                ngx_log_error(NGX_LOG_CRIT, cycle->log, 0,
                              "too small live notify buffer");
                return NGX_ERROR;
            }

            ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                          "live notify buffer size:%uz", size);
        }
    }

    lmcf->bufs.size = (int) (lmcf->bufs.size / 188 + 1) * 188;
    lmcf->buffer_size = lmcf->bufs.size + offsetof(ngx_http_live_msg_t, data);
    lmcf->buffer_limit = 3 * lmcf->bufs.num * lmcf->buffer_size;
    ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                    "live read buffer size:%uz limit:%uz",
                    lmcf->buffer_size - offsetof(ngx_http_live_msg_t, data),
                    lmcf->buffer_limit);

    lmcf->buf = ngx_create_temp_buf(cycle->pool, lmcf->buffer_size);
    if (lmcf->buf == NULL) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


static void
ngx_http_live_notify_cleanup(void *data)
{
    ngx_http_live_main_conf_t *lmcf = data;

    ngx_uint_t  n;

    if (lmcf->read_fd && lmcf->write_fd) {
        for (n = 0; n < lmcf->nfd; n++) {
            (void) ngx_close_socket(lmcf->read_fd[n]);
            (void) ngx_close_socket(lmcf->write_fd[n]);
        }
    }
}

static ngx_flag_t
ngx_http_live_rbtree_empty(ngx_rbtree_t *rbtree)
{
    if (rbtree->root == rbtree->sentinel)
        return 1;
    return 0;
}

static void
ngx_http_live_timer_check_handler(ngx_event_t *ev)
{
    ngx_http_live_main_conf_t    *lmcf;

    lmcf = (ngx_http_live_main_conf_t *) ev->data;

    if (!ngx_exiting || !ngx_http_live_rbtree_empty(&lmcf->live->sh->ptree)) {
        ngx_add_timer(ev, lmcf->check_period);
    }
}

static ngx_int_t
ngx_http_live_init_process(ngx_cycle_t *cycle)
{
    ngx_int_t                   event;
    ngx_uint_t                  n;
    ngx_event_t                *rev, *wev;
    ngx_socket_t                fd;
    ngx_connection_t           *c;
    ngx_http_live_main_conf_t  *lmcf;

    if (ngx_process != NGX_PROCESS_WORKER && ngx_process != NGX_PROCESS_SINGLE) {
        return NGX_OK;
    }

    lmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_live_module);

    if (lmcf == NULL || lmcf->nfd == 0) {
        return NGX_OK;
    }

    fd = NGX_INVALID_FILE;

    for (n = 0; n < lmcf->nfd; n++) {
        // ngx_worker id: 0 - ($num_workers - 1)
        if (n == ngx_worker) {
            fd = lmcf->read_fd[n];
            continue;
        }

        if (ngx_close_socket(lmcf->read_fd[n]) == NGX_FILE_ERROR) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_socket_errno,
                          ngx_close_socket_n " live_notify_socket[%ui] failed",
                          n);
        }
    }

    if (fd == NGX_INVALID_FILE) {
        ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
                      "worker has no live notify socket");
        return NGX_ERROR;
    }

    event = (ngx_event_flags & NGX_USE_CLEAR_EVENT) ? NGX_CLEAR_EVENT
            : NGX_LEVEL_EVENT;

    c = ngx_get_connection(fd, cycle->log);
    if (c == NULL) {
        return NGX_ERROR;
    }

    c->recv = ngx_udp_recv;
    c->data = lmcf;

    rev = c->read;
    rev->channel = 1;
    rev->log = cycle->log;
    rev->handler = ngx_http_live_notify_read_handler;

    if (ngx_add_event(rev, NGX_READ_EVENT, event) != NGX_OK) {
        return NGX_ERROR;
    }

    lmcf->conn = ngx_pcalloc(ngx_cycle->pool,
                             lmcf->nfd * sizeof(ngx_connection_t *));
    if (lmcf->conn == NULL) {
        return NGX_ERROR;
    }

    for (n = 0; n < lmcf->nfd; n++) {
        c = ngx_get_connection(lmcf->write_fd[n], cycle->log);
        if (c == NULL) {
            return NGX_ERROR;
        }

        lmcf->conn[n] = c;

        c->send = ngx_send;
        c->data = NULL;

        rev = c->read;
        rev->channel = 1;

        wev = c->write;
        wev->log = cycle->log;
        wev->handler = ngx_http_live_notify_write_handler;

        if (ngx_add_event(wev, NGX_WRITE_EVENT, event) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    lmcf->read_fd = NULL;
    lmcf->write_fd = NULL;

    lmcf->check_event.handler = ngx_http_live_timer_check_handler;
    lmcf->check_event.log = ngx_cycle->log;
    lmcf->check_event.data = lmcf;
    ngx_add_timer(&lmcf->check_event, lmcf->check_period);

    return NGX_OK;
}


static void
ngx_http_live_notify_read_handler(ngx_event_t *rev)
{
    ssize_t                     n;
    ngx_buf_t                  *b;
    ngx_connection_t           *c;
    ngx_http_live_msg_t        *msg;
    ngx_http_live_main_conf_t  *lmcf;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ngx_cycle->log, 0,
                   "live notify read handler");

    c = rev->data;
    lmcf = c->data;
    b = lmcf->buf;

    while (rev->ready) {
        n = c->recv(c, b->start, b->end - b->start);
        if (n <= 0) {
            break;
        }

        if ((size_t) n < offsetof(ngx_http_live_msg_t, data)) {
            ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                          "live notify message format error");
            continue;
        }

        msg = (ngx_http_live_msg_t *) b->start;

        if ((size_t) n != msg->size) {
            ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                          "live notify message size error: %z %z", n, msg->size);
            continue;
        }

#if (NGX_DEBUG)
        {
            u_char  hex[32];

            ngx_hex_dump(hex, msg->md5, 16);

            ngx_log_debug5(NGX_LOG_DEBUG_HTTP, ngx_cycle->log, 0,
                           "live notify recv n:%z, c:%ui, l:%d, md5:%*s",
                           n, msg->counter, msg->last, 32, hex);
        }
#endif
        b->pos = b->start;
        b->last = b->start + n;

        if (lmcf->gop_cache) {
            ngx_http_live_notify_process_gop(msg, b, lmcf->buffer_limit);
        } else {
            ngx_http_live_notify_process_msg(msg);
        }
    }

    (void) ngx_handle_read_event(rev, 0);
}


static ngx_uint_t
ngx_http_live_get_data_handler(ngx_http_request_t *r, ngx_http_live_ctx_t *ctx, u_char *p, ngx_uint_t size)
{
    size_t                     n;
    ngx_buf_t                 *b;
    ngx_chain_t               *cl, **ll;
    ngx_http_live_t           *live;
    ngx_http_live_main_conf_t *lmcf;

    if (ctx->done || size <= 0) {
        return 0;
    }

    live = ctx->live;

    lmcf = ngx_http_get_module_main_conf(r, ngx_http_live_module);

    for (ll = &ctx->out; *ll; ll = &(*ll)->next);

    while (size) {
        if (ctx->free) {
            cl = ctx->free;
            ctx->free = cl->next;

        } else if (ctx->allocated < (ngx_uint_t) lmcf->bufs.num) {
            cl = ngx_alloc_chain_link(r->pool);
            if (cl == NULL) {
                return size;
            }

            cl->buf = ngx_create_temp_buf(r->pool, lmcf->bufs.size);
            if (cl->buf == NULL) {
                return size;
            }

            ctx->allocated++;
        } else {
            return size;
        }

        *ll = cl;
        ll = &cl->next;
        cl->next = NULL;

        n = size > lmcf->bufs.size ? lmcf->bufs.size : size;

        b = cl->buf;
        b->tag = &ngx_http_live_module;
        b->flush = live->flush;
        b->pos = b->start;
        b->last = ngx_cpymem(b->pos, p, n);

        p += n;
        ctx->size += n;
        size -= n;
    }
    ctx->counter ++;

    return size;
}

static void
ngx_http_live_gop_buf_handler(ngx_http_request_t *r, ngx_http_live_gop_node_t *gop)
{
    ngx_flag_t                   kpos_flag;
    u_char                      *p;
    ngx_uint_t                   len, size;
    ngx_http_live_ctx_t         *ctx;
    ngx_http_live_msg_t         *msg;

    kpos_flag = 0;
    ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);

    if (ctx == NULL || ctx->done) {
        return;
    }

    if (!gop || !gop->key) {
        if (!ctx->live->persistent)
            ctx->done = 1;
        else
            ctx->in = NULL;
        return;
    }

    if (ctx->counter == 0 && gop->ts_index_tables) {
        // first write ts index tables(pat/pmt)
        msg = (ngx_http_live_msg_t *) (gop->ts_index_tables->pos);
        p = &msg->data[0];
        len = msg->size - offsetof(ngx_http_live_msg_t, data);
        size = ngx_http_live_get_data_handler(r, ctx, p, len);
        if (size) {
            ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0, "failed write ts tables:%ui", len);
        }
    }

    size = 0;
    msg = NULL;
    if (!ctx->in) {
        kpos_flag = 1;
        ctx->in = gop->key;
        ctx->size = 0;
        msg = (ngx_http_live_msg_t *) (ctx->in->buf->pos);
        ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0, "using key buffer %ui/%ui/%ui %ui",
                        ctx->counter, msg->counter, gop->latest_counter, msg->flag);
        ctx->counter = msg->counter;
    }

    while (ctx->in) {
        if (ctx->in->buf->start == ctx->in->buf->last) {
            return;
        }
        msg = (ngx_http_live_msg_t *) (ctx->in->buf->pos);
        if (kpos_flag) {
            ctx->size += msg->kpos;
            kpos_flag = 0;
        }
        p = &msg->data[0] + ctx->size;
        size = msg->size - offsetof(ngx_http_live_msg_t, data) - ctx->size;
        size = ngx_http_live_get_data_handler(r, ctx, p, size);
        if (size || !ctx->in->next) {
            break;
        }
        ctx->in = ctx->in->next;
        ctx->size = 0;
    }

    if (msg && msg->expired && size) {
        ctx->in = NULL;
        return;
    }

    return;
}

static void
ngx_http_live_gop_node_cleanup(ngx_pool_t *pool, ngx_http_live_gop_node_t *gop)
{
    if (gop->pool) {
        ngx_destroy_pool(gop->pool);
    }
    if (pool) {
        ngx_pfree(pool, gop);
    }
}

static ngx_http_live_gop_node_t*
ngx_http_live_gop_node_alloc(ngx_pool_t *pool, ngx_log_t *log)
{
    size_t                          size;
    ngx_http_live_gop_node_t        *gop;

    size = ngx_max(sizeof(ngx_http_live_gop_node_t), pool->max) + 1;
    gop = ngx_pcalloc(pool, size);
    if (!gop) {
        return NULL;
    }
    gop->pool = ngx_create_pool(8192, log);
    if (!gop->pool) {
        ngx_pfree(pool, gop);
        return NULL;
    }

    gop->pbusy = &gop->busy;
    gop->pfree = &gop->free;

    return gop;
}

static void
ngx_http_live_gop_mark_expired(ngx_http_live_gop_node_t *gop, size_t limit)
{
    ngx_uint_t                   size, count;
    ngx_chain_t                 *cl;
    ngx_http_live_msg_t         *msg;

    if (gop->size < limit && gop->count < GOP_COUNT_LIMIT) {
        return;
    }

    size = gop->size;
    count = gop->count;
    cl = gop->busy;

    while (cl) {
        if (cl == gop->key) {
            // incase no key flag found in the middle of stream
            if (size < limit * 2) {
                break;
            } else {
                gop->key = NULL;
            }
        }
        if (count < GOP_COUNT_LIMIT && size < limit / 2) {
            break;
        }

        msg = (ngx_http_live_msg_t *) (cl->buf->pos);
        msg->expired = 1;
        size -= msg->size;
        if (msg->flag & PKT_KEY_FLAG) {
            count --;
        }
        cl = cl->next;
    }

}

static void
ngx_http_live_gop_free_expired(ngx_http_live_gop_node_t *gop)
{
    ngx_chain_t                 *cl, **busy, **free;
    ngx_http_live_msg_t         *msg;

    busy = &gop->busy;
    free = gop->pfree;

    while (*busy) {
        cl = *busy;
        msg = (ngx_http_live_msg_t *) (cl->buf->pos);
        if (!msg->expired) {
            break;
        }
        gop->size -= msg->size;
        *busy = cl->next;
        cl->next = NULL;
        *free = cl;
        free = &cl->next;
        cl->buf->last = cl->buf->start;
        if (msg->flag & PKT_KEY_FLAG) {
            gop->count --;
        }
    }
    gop->pfree = free;
}

static ngx_int_t
ngx_http_live_gop_append_buf(ngx_http_live_gop_node_t *gop, ngx_http_live_msg_t *msg, ngx_buf_t *b, void *tag)
{
    ngx_buf_t          *buf;
    ngx_uint_t          size, n;
    ngx_chain_t        *cl, **key, **busy, **free;

    key = &gop->key;
    busy = gop->pbusy;
    free = &gop->free;
    size = b->last - b->pos;

    if (msg->flag == PKT_TS_INDEX) {
        gop->ts_index_tables = ngx_create_temp_buf(gop->pool, b->end - b->start);
        if (gop->ts_index_tables == NULL) {
            return NGX_ERROR;
        }
        ngx_memmove(gop->ts_index_tables->last, b->pos, size);
        gop->ts_index_tables->last += size;
        return NGX_OK;
    }

    if (msg->flag & PKT_KEY_FLAG || gop->size == 0
            || (msg->flag == PKT_NO_VIDEO && msg->counter % 20 == 0)) {
        *key = NULL;
    }

    cl = *busy;
    gop->size += size;
    gop->latest_counter = msg->counter;

    while (size) {
        if (!cl) {
            if (*free) {
                *busy = cl = *free;
                *free = NULL;
                gop->pfree = free;

                buf = cl->buf;
            } else {
                cl = ngx_alloc_chain_link(gop->pool);
                if (cl == NULL) {
                    return NGX_ERROR;
                }
                *busy = cl;
                cl->next = NULL;
                buf = ngx_create_temp_buf(gop->pool, b->end - b->start);
                if (buf == NULL) {
                    return NGX_ERROR;
                }
                buf->tag = tag;
                cl->buf = buf;
            }
        } else {
            buf = cl->buf;
        }
        if (!(*key)) {
            *key = cl;
            gop->count ++;
            if (!(msg->flag & PKT_KEY_FLAG))
                msg->flag = PKT_KEY_TEMP; // just marked key; may not a video keyframe
        }
        n = buf->end - buf->last;
        if (n >= size) {
            n = size;
            ngx_memmove(buf->last, b->pos, n);
            buf->last += n;
            size -= n;
        }
        busy = &cl->next;
        cl = cl->next;
    }
    gop->pbusy = busy;

    return NGX_OK;
}

static void
ngx_http_live_notify_process_gop(ngx_http_live_msg_t *msg, ngx_buf_t *b, size_t limit)
{
    ngx_connection_t          *c;
    ngx_http_request_t        *r;
    ngx_http_live_get_node_t  *get;
    ngx_http_live_gop_node_t  *gop;

    get = ngx_http_live_get_rbtree_lookup(&msg->live->gtree, msg->md5);
    gop = ngx_http_live_gop_rbtree_lookup(&msg->live->btree, msg->md5);

    if (msg->last && gop) {
        ngx_rbtree_delete(&msg->live->btree, &gop->node);
        ngx_http_live_gop_node_cleanup(ngx_cycle->pool, gop);
        gop = NULL;

    } else if (!msg->last && !gop) {
        gop = ngx_http_live_gop_node_alloc(ngx_cycle->pool, ngx_cycle->log);
        if (gop) {
            ngx_memcpy(gop->md5, msg->md5, 16);
            ngx_rbtree_insert(&msg->live->btree, &gop->node);
        }
    }
    if (gop) {
        ngx_http_live_gop_free_expired(gop);
        ngx_http_live_gop_mark_expired(gop, limit);
        ngx_http_live_gop_append_buf(gop, msg, b, &ngx_http_live_module);
    }

    while (get) {
        r = get->request;
        c = r->connection;

        ngx_http_live_gop_buf_handler(r, gop);

        ngx_post_event(c->write, &ngx_posted_events);

        if (r != c->data) {
            ngx_http_post_request(r, NULL);
        }

        get = (ngx_http_live_get_node_t *)
              ngx_rbtree_next(&msg->live->gtree, &get->node);

        if (get && ngx_memcmp(get->md5, msg->md5, 16) != 0) {
            break;
        }
    }
}


static void
ngx_http_live_notify_process_msg(ngx_http_live_msg_t *msg)
{
    u_char                    *p;
    ngx_uint_t                 size;
    ngx_connection_t          *c;
    ngx_http_request_t        *r;
    ngx_http_live_ctx_t       *ctx;
    ngx_http_live_get_node_t  *get;

    p = &msg->data[0];
    size = msg->size - offsetof(ngx_http_live_msg_t, data);

    get = ngx_http_live_get_rbtree_lookup(&msg->live->gtree, msg->md5);

    while (get) {
        r = get->request;
        c = r->connection;

        ctx = ngx_http_get_module_ctx(r, ngx_http_live_module);

        if (ctx != NULL && !ctx->done) {
            if (msg->last && !ctx->live->persistent) {
                ctx->done = 1;
            }
            ngx_http_live_get_data_handler(r, ctx, p, size);
        }

        ngx_post_event(c->write, &ngx_posted_events);

        if (r != c->data) {
            ngx_http_post_request(r, NULL);
        }

        get = (ngx_http_live_get_node_t *)
              ngx_rbtree_next(&msg->live->gtree, &get->node);

        if (get && ngx_memcmp(get->md5, msg->md5, 16) != 0) {
            break;
        }
    }
}

static void
ngx_http_live_notify_write_handler(ngx_event_t *wev)
{
    ssize_t              n;
    ngx_buf_t           *b;
    ngx_chain_t         *cl;
    ngx_connection_t    *c;
    ngx_http_request_t  *r;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ngx_cycle->log, 0,
                   "live notify write handler");

    c = wev->data;

    while (wev->ready && c->data) {
        cl = c->data;
        b = cl->buf;

        n = c->send(c, b->pos, b->last - b->pos);

        if (n == NGX_AGAIN) {
            break;
        }

        if (n == NGX_ERROR) {
            /*
             * This can be a serious socket error, as well as
             * ENOBUFS on BSD systems when SOCK_DGRAM is used.
             * Anyway, there's not much we can do at this point
             * rather than retry the send after a few millisec.
             *
             * This situation is common on MacOS however.
             * Being a BSD system and missing a SOCK_SEQPACKET
             * implementation (despite having a #define), it may
             * return ENOBUFS here.  Typically for BSD, there is
             * no way to wait for ENOBUFS condition to end with
             * kqueue/select.  The only available solution is
             * waiting on a timer.
             */

            wev->cancelable = 1;
            wev->error = 1;

            // ngx_add_timer(wev, 10);

            // break;

            if (!ngx_exiting && !ngx_terminate) {
                ngx_log_error(NGX_LOG_EMERG, ngx_cycle->log, 0,
                            "live notify write error b:%p, n:%z, s:%z",
                            b, n, b->last - b->pos);
            }
        }

        c->data = cl->next;

        ngx_free_chain(ngx_cycle->pool, cl);

        if (--b->file_pos) {
            continue;
        }

        if (b->tag == NULL) {
            ngx_free(b);
            continue;
        }

        r = (ngx_http_request_t *) b->tag;

        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "live notify reclaim buffer b:%p", b);

        if (ngx_exiting || ngx_terminate) {
            ngx_add_timer(r->connection->read, 1);
        } else {
            ngx_post_event(r->connection->read, &ngx_posted_events);
        }

        if (r != r->connection->data) {
            ngx_http_post_request(r, NULL);
        }
    }

    (void) ngx_handle_write_event(wev, 0);
}


static ngx_int_t
ngx_http_live_notify(ngx_http_request_t *r, ngx_buf_t *b)
{
    ngx_uint_t                  n;
    ngx_chain_t                *cl, **ll;
    ngx_connection_t           *c;
    ngx_http_live_main_conf_t  *lmcf;

#if (NGX_DEBUG)
    {
        u_char                hex[32];
        ngx_http_live_msg_t  *msg;

        msg = (ngx_http_live_msg_t *) b->pos;
        ngx_hex_dump(hex, msg->md5, 16);

        ngx_log_error(NGX_LOG_DEBUG_HTTP, ngx_cycle->log, 0,
                      "live notify b:%p, n:%z, c:%ui, l:%d, f:%ui t:%p, md5:%*s",
                      b, msg->size, msg->counter, msg->last, msg->flag, b->tag, 32, hex);
    }
#endif

    lmcf = ngx_http_get_module_main_conf(r, ngx_http_live_module);

    for (n = 0; n < lmcf->nfd; n++) {
        if (n == ngx_worker) {
            continue;
        }
        cl = ngx_alloc_chain_link(ngx_cycle->pool);
        if (cl == NULL) {
            return NGX_ERROR;
        }

        b->file_pos++;

        cl->buf = b;
        cl->next = NULL;

        if (lmcf->conn) {
            c = lmcf->conn[n];

            for (ll = (ngx_chain_t **) &c->data; *ll; ll = &(*ll)->next);
            *ll = cl;

            if (c->data == cl && c->write->ready) {
                ngx_post_event(c->write, &ngx_posted_events);
            }
        }
    }

    if (b->file_pos == 0 && b->tag == NULL) {
        ngx_free(b);
    }

    return NGX_OK;
}
