
/*
 * Copyright (C) Roman Arutyunyan
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <stdio.h>
#include <openssl/aes.h>

#include "ngx_ts_hls.h"

static ngx_int_t ngx_ts_hls_create_keyfile(ngx_ts_hls_t *hls);
static void ngx_ts_hls_cleanup(void *data);
static ngx_int_t ngx_ts_hls_handler(ngx_ts_handler_data_t *hd);
static ngx_int_t ngx_ts_hls_pat_handler(ngx_ts_hls_t *hls);
static ngx_int_t ngx_ts_hls_pes_handler(ngx_ts_hls_t *hls,
                                        ngx_ts_program_t *prog, ngx_ts_es_t *es, ngx_chain_t *bufs);
static void ngx_ts_hls_update_bandwidth(ngx_ts_hls_t *hls,
                                        ngx_ts_hls_variant_t *var, ngx_chain_t *bufs, uint64_t dts);

static void ngx_ts_hls_restore_playlist(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var);
static ngx_int_t ngx_ts_hls_rename_segment(ngx_ts_hls_t *hls,
        ngx_ts_hls_variant_t *var, ngx_ts_hls_segment_t *seg);
static ngx_int_t ngx_ts_hls_close_segment(ngx_ts_hls_t *hls,
        ngx_ts_hls_variant_t *var, ngx_ts_es_t *es);
static ngx_int_t ngx_ts_hls_update_playlist(ngx_ts_hls_t *hls,
        ngx_ts_hls_variant_t *var);
static ngx_int_t ngx_ts_hls_update_master_playlist(ngx_ts_hls_t *hls);
static ngx_int_t ngx_ts_hls_write_file_temp(u_char *path, u_char *tmp_path,
                                       u_char *data, size_t len, ngx_log_t *log);
static ngx_int_t ngx_ts_hls_write_file(u_char *path, u_char *data, size_t len, ngx_log_t *log);
static ngx_int_t ngx_ts_hls_open_segment(ngx_ts_hls_t *hls,
        ngx_ts_hls_variant_t *var);

static ngx_msec_t ngx_ts_hls_file_manager(void *data);
static ngx_int_t ngx_ts_hls_manage_file(ngx_tree_ctx_t *ctx, ngx_str_t *path);
static ngx_int_t ngx_ts_hls_manage_directory(ngx_tree_ctx_t *ctx,
        ngx_str_t *path);
static ngx_int_t ngx_ts_hls_delete_directory(ngx_tree_ctx_t *ctx,
        ngx_str_t *path);
static ngx_int_t ngx_ts_hls_delete_file(ngx_tree_ctx_t *ctx, ngx_str_t *path);

static ngx_string_format_t*
ngx_ts_hls_add_string_format(ngx_array_t *templates, NGX_FORMAT_TYPE type, void *data)
{
    ngx_string_format_t     *sf;

    sf = ngx_array_push(templates);
    if (sf == NULL) {
        return NULL;
    }
    sf->type = type;
    sf->data = data;

    return sf;
}

static ngx_int_t
ngx_ts_hls_add_string_data(ngx_array_t *templates, ngx_pool_t *pool, u_char *p, size_t n)
{
    u_char                  *str;

    str = ngx_pcalloc(pool, n+1);
    if (str == NULL) {
        return NGX_ERROR;
    }
    ngx_snprintf(str, n, "%s", p);
    if (ngx_ts_hls_add_string_format(templates, FORMAT_TYPE_STR, str) == NULL) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_array_t *
ngx_ts_hls_parse_template(ngx_pool_t *pool, ngx_str_t name)
{
    u_char                  *p, *start;
    size_t                  len, gap, n;
    ngx_array_t             *templates;  
    NGX_FORMAT_TYPE         type;

    p = start = name.data;
    len = name.len;

    templates = ngx_array_create(pool, 1, sizeof(ngx_string_format_t));
    if (templates == NULL) {
        return NULL;
    }
    while (len > 0) {
        gap = 0;
        if (len >= 5 && ngx_strncmp(p, "$name", 5) == 0) {
            type = FORMAT_TYPE_NAME;
            gap = 5;
        }
        if (len >= 5 && ngx_strncmp(p, "$time", 5) == 0) {
            type = FORMAT_TYPE_TIME;
            gap = 5;
        }
        if (len >= 4 && ngx_strncmp(p, "$seq", 4) == 0) {
            type = FORMAT_TYPE_SEQ;
            gap = 4;
        }
        if (gap > 0) {
            if (p > start) {
                n = p - start;
                if (ngx_ts_hls_add_string_data(templates, pool, start, n) != NGX_OK) {
                    return NULL;
                }
                start += n;
            }
            if (ngx_ts_hls_add_string_format(templates, type, NULL) == NULL) {
                return NULL;
            }
            start += gap;
            p += gap;
            len -= gap;
        } else {
            p ++;
            len --;
        }
    }
    if (p > start) {
        n = p - start;
        if (ngx_ts_hls_add_string_data(templates, pool, start, n) != NGX_OK) {
            return NULL;
        }
    }

    return templates;
}

ngx_ts_hls_t *
ngx_ts_hls_create(ngx_ts_hls_conf_t *conf, ngx_ts_stream_t *ts, ngx_str_t *name)
{
    u_char              *p;
    ngx_ts_hls_t        *hls;
    ngx_pool_cleanup_t  *cln;
    ngx_int_t            len;

    if (conf == NULL) {
        return NULL;
    }

    hls = ngx_pcalloc(ts->pool, sizeof(ngx_ts_hls_t));
    if (hls == NULL) {
        return NULL;
    }

    ngx_log_error(NGX_LOG_NOTICE, ts->log, 0, "live hls create t:%M", conf->min_seg);

    cln = ngx_pool_cleanup_add(ts->pool, 0);
    if (cln == NULL) {
        return NULL;
    }

    cln->handler = ngx_ts_hls_cleanup;
    cln->data = hls;

    hls->conf = conf;
    hls->ts = ts;

    hls->is_live = conf->is_live;
    hls->name_templates = conf->name_templates;
    hls->prefix_templates = conf->prefix_templates;

    hls->stream_name = *name;
    hls->m3u8_name = conf->m3u8;

    hls->path.data = ngx_pcalloc(ts->pool, conf->path->name.len + name->len + 2);
    if (hls->path.data == NULL) {
        return NULL;
    }
    p = ngx_sprintf(hls->path.data, "%V/%V", &conf->path->name, name);
    hls->path.len = p - hls->path.data;

    /* XXX dir access mode */
    if (ngx_create_dir(hls->path.data, 0775) == NGX_FILE_ERROR) {
        if (ngx_errno != NGX_EEXIST) {
            ngx_log_error(NGX_LOG_CRIT, ts->log, ngx_errno,
                            ngx_create_dir_n " \"%s\" failed",
                            hls->path.data);
            return NULL;
        }
    }

    hls->tmp_filename.data = ngx_pcalloc(ts->pool, hls->path.len + strlen(NGX_HLS_TMP_EXT) + 2);
    if (hls->tmp_filename.data == NULL) {
        return NULL;
    }
    p = ngx_sprintf(hls->tmp_filename.data, "%V/%s", &hls->path, NGX_HLS_TMP_EXT);
    hls->tmp_filename.len = p - hls->tmp_filename.data;

    if (ENCRYPT_NONE != conf->key_flag && !conf->key_url.len)
        conf->key_flag = ENCRYPT_NONE;

    if (conf->key_flag == ENCRYPT_AES) {
        time_t timestamp = ngx_time();
        /* [path]/[stream_name].key */
        len = hls->path.len + hls->stream_name.len + sizeof("/.key") + 1 + 11;
        hls->key_path = ngx_pcalloc(ts->pool, len);
        if (hls->key_path == NULL) {
            return NULL;
        }
        ngx_sprintf(hls->key_path, "%V/%V-%T.key", &hls->path, &hls->stream_name, timestamp);

        /* [key_url]/[stream_name]/[stream_name]-[timestamp].key */
        len = hls->conf->key_url.len + 2 * hls->stream_name.len + sizeof("//.key") + 1 + 11;
        hls->key_url = ngx_pcalloc(ts->pool, len);
        if (hls->key_url == NULL) {
            return NULL;
        }
        ngx_sprintf(hls->key_url, "%V/%V/%V-%T.key", &hls->conf->key_url, &hls->stream_name, &hls->stream_name, timestamp);
        ngx_log_error(NGX_LOG_NOTICE, ts->log, 0, "key url: %s, key path: %s", hls->key_url, hls->key_path);

        if (ngx_ts_hls_create_keyfile(hls) != NGX_OK) {
            return NULL;
        }
    }

    if (ngx_ts_add_handler(ts, ngx_ts_hls_handler, hls) != NGX_OK) {
        return NULL;
    }

    return hls;
}

static void ngx_ts_hls_key_to_hex(u_char *dst, const u_char *src, uint8_t len)
{
    int i;
    static const char hex_table_lc[16] = { '0', '1', '2', '3', '4', '5', '6', '7',
                                           '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    for (i = 0; i < len; i++) {
        dst[i * 2] = hex_table_lc[src[i] >> 4];
        dst[i * 2 + 1] = hex_table_lc[src[i] & 0xF];
    }

}

static ngx_int_t
ngx_ts_hls_check_keyfile(ngx_ts_hls_t *hls)
{
    ssize_t                  n;
    ngx_fd_t                 fd;
    u_char                   buffer[KEY_LENGTH];

    fd = ngx_open_file(hls->key_path,
                       NGX_FILE_RDONLY,
                       NGX_FILE_OPEN,
                       0664);
    if (fd == NGX_INVALID_FILE) {
        return NGX_ERROR;
    }

    ngx_memzero(buffer, sizeof(buffer));
    n = ngx_read_fd(fd, buffer, sizeof(buffer));
    ngx_close_file(fd);

    if (n != KEY_LENGTH) {
        return NGX_ERROR;
    }

    ngx_memzero(hls->key, sizeof(hls->key));
    ngx_memmove(hls->key, buffer, KEY_LENGTH);

    return NGX_OK;
}

static void
ngx_ts_hls_init_rand_key(u_char *key, ngx_uint_t len)
{
    ngx_uint_t                i;

    // srand((unsigned)time(NULL));
    for (i = 0; i < len; i++) {
        key[i] = (rand() % 26) + 97;
    }
}

static ngx_int_t
ngx_ts_hls_create_keyfile(ngx_ts_hls_t *hls)
{
    ngx_log_error(NGX_LOG_NOTICE, hls->ts->log, 0, "live hls create key file");

    if (NGX_OK == ngx_ts_hls_check_keyfile(hls)) {
        return NGX_OK;
    }

    ngx_memzero(hls->key, sizeof(hls->key));
    ngx_ts_hls_init_rand_key(hls->key, KEY_LENGTH);
    ngx_memzero(hls->key_hex, sizeof(hls->key_hex));
    ngx_ts_hls_key_to_hex(hls->key_hex, hls->key, KEY_LENGTH);

    ngx_log_error(NGX_LOG_NOTICE, hls->ts->log, 0, "live hls write key file");
    if (ngx_ts_hls_write_file(hls->key_path, hls->key, KEY_LENGTH, hls->ts->log) != NGX_OK) {
        return NGX_ERROR;
    }

    // random iv
    ngx_memzero(hls->iv, sizeof(hls->iv));
    ngx_ts_hls_init_rand_key(hls->iv, KEY_LENGTH);
    ngx_memzero(hls->iv_hex, sizeof(hls->iv_hex));
    ngx_ts_hls_key_to_hex(hls->iv_hex, hls->iv, KEY_LENGTH);

    return NGX_OK;
}

static void
ngx_ts_hls_cleanup(void *data)
{
    ngx_ts_hls_t *hls = data;

    int64_t                d, maxd;
    ngx_uint_t             n, i;
    ngx_ts_es_t           *es;
    ngx_ts_stream_t       *ts;
    ngx_ts_hls_variant_t  *var;

    hls->done = 1;

    ts = hls->ts;

    ngx_log_error(NGX_LOG_NOTICE, ts->log, 0, "live hls cleanup");

    for (n = 0; n < hls->nvars; n++) {
        es = NULL;
        var = &hls->vars[n];

        maxd = 0;

        for (i = 0; i < var->prog->nes; i++) {
            es = &var->prog->es[i];

            d = es->dts - var->seg_dts;
            if (maxd < d) {
                maxd = d;
            }
        }

        (void) ngx_ts_hls_close_segment(hls, var, es);

        if (var->file.fd != NGX_INVALID_FILE) {
            if (ngx_close_file(var->file.fd) == NGX_FILE_ERROR) {
                ngx_log_error(NGX_LOG_ALERT, ts->log, ngx_errno,
                              ngx_close_file_n " \"%s\" failed",
                              var->file.name.data);
            }

            var->file.fd = NGX_INVALID_FILE;
        }
    }
}

static ngx_int_t
ngx_ts_hls_handler(ngx_ts_handler_data_t *hd)
{
    ngx_ts_hls_t *hls = hd->data;

    switch (hd->event) {

    case NGX_TS_PAT:
        return ngx_ts_hls_pat_handler(hls);

    case NGX_TS_PES:
        return ngx_ts_hls_pes_handler(hls, hd->prog, hd->es, hd->bufs);

    default:
        return NGX_OK;
    }
}

static ngx_int_t
ngx_ts_hls_pat_handler(ngx_ts_hls_t *hls)
{
    size_t                 len;
    u_char                *p;
    ngx_uint_t             n, i;
    ngx_ts_stream_t       *ts;
    ngx_ts_program_t      *prog;
    ngx_ts_hls_variant_t  *var;

    if (hls->vars) {
        return NGX_OK;
    }

    ts = hls->ts;

    ngx_log_debug0(NGX_LOG_DEBUG_CORE, ts->log, 0, "ts hls pat");

    hls->nvars = ts->nprogs;
    hls->vars = ngx_pcalloc(ts->pool,
                            sizeof(ngx_ts_hls_variant_t) * ts->nprogs);
    if (hls->vars == NULL) {
        return NGX_ERROR;
    }

    if (hls->nvars > 1) {
        len = hls->path.len + hls->m3u8_name.len + 2;

        hls->m3u8_path = ngx_pcalloc(ts->pool, len);
        if (hls->m3u8_path == NULL) {
            return NGX_ERROR;
        }

        ngx_sprintf(hls->m3u8_path, "%V/%V", &hls->path, &hls->m3u8_name);
    }

    for (n = 0; n < ts->nprogs; n++) {
        prog = &ts->progs[n];
        var = &hls->vars[n];

        var->prog = prog;
        var->file.fd = NGX_INVALID_FILE;
        var->file.log = ts->log;

        var->nsegs = hls->conf->nsegs;
        var->segs = ngx_pcalloc(ts->pool,
                                sizeof(ngx_ts_hls_segment_t) * hls->conf->nsegs);
        if (var->segs == NULL) {
            return NGX_ERROR;
        }

        /* segment / m3u8 path */
        if (hls->nvars > 1) {
            /* [path]/[prog] */
            len = hls->path.len + NGX_INT32_LEN + 2;
            p = ngx_pcalloc(ts->pool, len + 1);
            if (p == NULL) {
                return NGX_ERROR;
            }
            var->path.data = p;
            p = ngx_snprintf(p, len, "%V/%d", &hls->path, prog->number);
            var->path.len = p - var->path.data;
            *(var->path.data + var->path.len) = '\0';

            /* XXX dir access mode */
            if (ngx_create_dir(var->path.data, 0775) == NGX_FILE_ERROR) {
                if (ngx_errno != NGX_EEXIST) {
                    ngx_log_error(NGX_LOG_CRIT, ts->log, ngx_errno,
                                    ngx_create_dir_n " \"%s\" failed",
                                    var->path.data);
                    return NGX_ERROR;
                }
            }
        } else {
            var->path = hls->path;
        }

        /* segment filename */
        for (i = 0; i < hls->conf->nsegs; i++) {
            var->segs[i].filename = ngx_pcalloc(ts->pool, MAX_FILENAME_LEN);
            if (var->segs[i].filename == NULL) {
                return NGX_ERROR;
            }
            var->segs[i].name = ngx_pcalloc(ts->pool, MAX_FILENAME_LEN);
            if (var->segs[i].name == NULL) {
                return NGX_ERROR;
            }
            var->segs[i].prefix = ngx_pcalloc(ts->pool, MAX_FILENAME_LEN);
            if (var->segs[i].prefix == NULL) {
                return NGX_ERROR;
            }
        }

        /* m3u8 name*/
        var->m3u8_name.data = ngx_pcalloc(ts->pool, NGX_INT32_LEN + hls->m3u8_name.len + 2);
        if (var->m3u8_name.data == NULL) {
            return NGX_ERROR;
        }
        if (hls->nvars > 1) {
            p = ngx_sprintf(var->m3u8_name.data, "%ui/%V", prog->number, &hls->m3u8_name);
        } else {
            p = ngx_sprintf(var->m3u8_name.data, "%V", &hls->m3u8_name);
        }
        var->m3u8_name.len = p - var->m3u8_name.data;

        /* m3u8 filename */
        var->m3u8_filename.data = ngx_pcalloc(ts->pool, hls->path.len + var->m3u8_name.len + 2);
        if (var->m3u8_filename.data == NULL) {
            return NGX_ERROR;
        }
        p = ngx_sprintf(var->m3u8_filename.data, "%V/%V", &hls->path, &var->m3u8_name);
        var->m3u8_filename.len = p - var->m3u8_filename.data;

        /* tmp filename */
        var->tmp_filename.data = ngx_pcalloc(ts->pool, var->path.len + strlen(NGX_HLS_TMP_EXT) + 2);
        if (var->tmp_filename.data == NULL) {
            return NGX_ERROR;
        }
        p = ngx_sprintf(var->tmp_filename.data, "%V/%s", &var->path, NGX_HLS_TMP_EXT);
        var->tmp_filename.len = p - var->tmp_filename.data;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_ts_hls_pes_handler(ngx_ts_hls_t *hls, ngx_ts_program_t *prog,
                       ngx_ts_es_t *es, ngx_chain_t *bufs)
{
    ngx_uint_t             n;
    ngx_chain_t           *out;
    ngx_ts_stream_t       *ts;
    ngx_ts_hls_variant_t  *var;

    if (!es->ptsf) {
        // return NGX_OK;
    }

    ts = hls->ts;

    ngx_log_debug1(NGX_LOG_DEBUG_CORE, ts->log, 0, "ts hls pes pid:%ud",
                   (unsigned) es->pid);

    for (n = 0; n < hls->nvars; n++) {
        var = &hls->vars[n];
        if (prog == var->prog) {
            goto found;
        }
    }

    ngx_log_error(NGX_LOG_ERR, ts->log, 0, "TS program not found");

    return NGX_AGAIN;

found:

    ngx_ts_hls_update_bandwidth(hls, var, bufs, es->pts);

    if (ngx_ts_hls_close_segment(hls, var, es) != NGX_OK) {
        return NGX_ERROR;
    }

    if (ngx_ts_hls_open_segment(hls, var) != NGX_OK) {
        return NGX_ERROR;
    }

    out = ngx_ts_write_pes(ts, prog, es, bufs);
    if (out == NULL) {
        return NGX_ERROR;
    }

    if (ngx_write_chain_to_file(&var->file, out, var->file.offset, ts->pool)
            == NGX_ERROR) {
        ngx_ts_free_chain(ts, &out);
        return NGX_ERROR;
    }

    ngx_ts_free_chain(ts, &out);

    return NGX_OK;
}

static void
ngx_ts_hls_update_bandwidth(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var,
                            ngx_chain_t *bufs, uint64_t dts)
{
    int64_t  d, analyze;

    if (var->bandwidth) {
        return;
    }

    if (var->bandwidth_bytes == 0) {
        var->bandwidth_dts = dts;
    }

    while (bufs) {
        var->bandwidth_bytes += bufs->buf->last - bufs->buf->pos;
        bufs = bufs->next;
    }

    d = dts - var->bandwidth_dts;
    analyze = (int64_t) hls->conf->analyze * 90;

    if (d >= analyze) {
        var->bandwidth = var->bandwidth_bytes * 8 * 90000 / d;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_CORE, hls->ts->log, 0,
                   "ts hls bandwidth:%ui, pid:%ud",
                   var->bandwidth, (unsigned) var->prog->pid);
}

char *data_to_hex(char *buff, const char *src, int s, int lowercase)
{
    int i;
    static const char hex_table_uc[16] = { '0', '1', '2', '3',
                                           '4', '5', '6', '7',
                                           '8', '9', 'A', 'B',
                                           'C', 'D', 'E', 'F' };
    static const char hex_table_lc[16] = { '0', '1', '2', '3',
                                           '4', '5', '6', '7',
                                           '8', '9', 'a', 'b',
                                           'c', 'd', 'e', 'f' };
    const char *hex_table = lowercase ? hex_table_lc : hex_table_uc;

    for (i = 0; i < s; i++) {
        buff[i * 2]     = hex_table[src[i] >> 4];
        buff[i * 2 + 1] = hex_table[src[i] & 0xF];
    }

    return buff;
}

unsigned char
ngx_ts_hls_key_checksum(unsigned char* key, int size)
{
    unsigned long cksum = 0;

    while (size > 1) {
        cksum += *key++;
        size -= sizeof(unsigned char);
    }
    if (size) {
        cksum += *(unsigned char*)key;
    }
    cksum = (cksum >> 8) + (cksum & 0xff);
    cksum += (cksum >> 8);
    return (unsigned char)(~cksum);
}

static ngx_int_t
ngx_ts_hls_encrypt_segment(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var, ngx_ts_hls_segment_t *seg)
{
    u_char        cmd[4096] = {'\0'};
    FILE         *pp = NULL;

    ngx_sprintf(cmd, "openssl aes-128-cbc -e -in %V -out %s -nosalt -iv %s -K %s", &var->file.name, seg->filename, hls->iv_hex, hls->key_hex);

    ngx_log_error(NGX_LOG_DEBUG, hls->ts->log, 0, "run: \"%s\"", cmd);
    pp = popen((const char*)cmd, "r");
    if (!pp) {
        ngx_log_error(NGX_LOG_ERR, hls->ts->log, ngx_errno, "popen \"%s\" failed", cmd);
        return NGX_ERROR;
    }
    pclose(pp);
    unlink((const char*)var->file.name.data);

    return NGX_OK;
}

static ngx_int_t
ngx_ts_hls_rename_segment(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var, ngx_ts_hls_segment_t *seg)
{

    if (ngx_rename_file(var->file.name.data, seg->filename) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_WARN, hls->ts->log, ngx_errno,
                    "ts hls rename segment \"%V\" to \"%s\" failed",
                    &var->file.name, seg->filename);
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_ts_hls_close_segment(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var,
                         ngx_ts_es_t *es)
{
    off_t                  max_size;
    ngx_uint_t             can_split = 0;
    int64_t                d, min_seg;
    ngx_ts_stream_t       *ts;
    ngx_ts_hls_segment_t  *seg;

    ts = hls->ts;

    if (var->file.fd == NGX_INVALID_FILE) {
        var->seg_dts = es->dts;
        return NGX_OK;
    }

    d = es->dts - var->seg_dts;

    min_seg = (int64_t) hls->conf->min_seg * 90;
    max_size = hls->conf->max_size;

    if (var->prog->video && es->video && es->rand) {
        var->video_keyframes++;
        if (d >= min_seg
                || (var->video_keyframes > hls->conf->max_gop)
                || (hls->conf->faststart && var->seg - var->start_seg < hls->conf->nsegs)) {
            can_split = 1;
        }
    }
    else if (!var->prog->video) {
        if (d >= min_seg) {
            can_split = 1;
        }
    }
    if (hls->conf->low_latency) {
        if (d >= min_seg) {
            can_split = 1;
        }
    }
    if (max_size && var->file.offset > max_size) {
        can_split = 1;
        ngx_log_error(NGX_LOG_WARN, ts->log, 0,
                "closing hls segment \"%s\" on size limit: %d",
                var->file.name.data, max_size);
    } else if (var->file.offset < 65536) {
        return NGX_OK;
    }

    if (!can_split && !hls->done) {
        return NGX_OK;
    }

    ngx_log_error(NGX_LOG_DEBUG, ts->log, 0,
                   "ts hls close segment \"%s\"", var->file.name.data);

    if (ngx_close_file(var->file.fd) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, ts->log, ngx_errno,
                      ngx_close_file_n " \"%s\" failed", var->file.name.data);
    }

    var->video_keyframes = 0;
    var->seg_dts = es->dts;
    seg = &var->segs[var->seg % var->nsegs];
    seg->duration = d;
    seg->size = var->file.offset;

    if (hls->key_url) {
        ngx_ts_hls_encrypt_segment(hls, var, seg);
    } else {
        ngx_ts_hls_rename_segment(hls, var, seg);
    }

    ngx_memzero(&var->file, sizeof(ngx_file_t));

    var->file.fd = NGX_INVALID_FILE;
    var->file.log = ts->log;

    if (ngx_ts_hls_update_playlist(hls, var) != NGX_OK) {
        return NGX_ERROR;
    }
    var->seg++;
    if (var->seg >= UINT32_MAX - var->nsegs) {
        var->seg = var->seg % var->nsegs;
    }
    if (var->segs[var->seg % var->nsegs].discont)
        var->discontinuity_seq++;
    var->segs[var->seg % var->nsegs].discont = 0;

    return ngx_ts_hls_update_master_playlist(hls);
}

static ngx_int_t
ngx_ts_hls_update_playlist(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var)
{
    size_t                 len;
    u_char                *p, *data;
    uint64_t               maxd;
    ngx_int_t              rc;
    ngx_uint_t             i, nsegs, first;
    ngx_ts_stream_t       *ts;
    ngx_ts_hls_segment_t  *seg;

    ts = hls->ts;
    nsegs = 0;

    ngx_log_error(NGX_LOG_DEBUG, ts->log, 0,
                   "ts hls update playlist \"%V\"", &var->m3u8_filename);

    len = sizeof("#EXTM3U\n"
                 "#EXT-X-VERSION:3\n"
                 "#EXT-X-MEDIA-SEQUENCE:\n"
                 "#EXT-X-DISCONTINUITY-SEQUENCE:\n"
                 "#EXT-X-TARGETDURATION:\n\n") - 1
            + 3 * NGX_INT_T_LEN;

    if (hls->key_url) {
        len += sizeof(NGX_HLS_KEY "METHOD=AES-128,URI=\"\",IV=0x\n") + ngx_strlen(hls->key_url) + 32 /*hex iv*/;
    }

    maxd = 0;

    first = (var->seg + 1) % var->nsegs;
    for (i = 0; i < var->nsegs; i++) {
        seg = &var->segs[(first + i) % var->nsegs];

        if (seg->duration) {
            nsegs++;
            if (maxd < seg->duration)
                maxd = seg->duration;

            if (seg->discont)
                len += sizeof("#EXT-X-DISCONTINUITY\n");
            len += sizeof("#EXTINF:,\n\n") + NGX_INT_T_LEN + ngx_strlen(seg->prefix) + ngx_strlen(seg->name);

            if (hls->nvars > 1) {
                len += NGX_INT_T_LEN + 1;
            }
        }
    }

    if (!hls->is_live && hls->done) {
        len += sizeof("\n#EXT-X-ENDLIST\n") - 1;
    }

    data = ngx_alloc(len, ts->log);
    if (data == NULL) {
        return NGX_ERROR;
    }

    p = data;

    p = ngx_sprintf(p, "#EXTM3U\n"
                    "#EXT-X-VERSION:3\n"
                    "#EXT-X-MEDIA-SEQUENCE:%ui\n"
                    "#EXT-X-DISCONTINUITY-SEQUENCE:%ui\n"
                    "#EXT-X-TARGETDURATION:%uL\n", var->seg + 1 - nsegs, var->discontinuity_seq,
                    maxd / 90000 + (maxd % 90000 != 0));
    if (hls->key_url) {
        p = ngx_sprintf(p, NGX_HLS_KEY "METHOD=AES-128,URI=\"%s\",IV=0x%s\n", hls->key_url, hls->iv_hex);
    }

    for (i = 0; i < var->nsegs; i++) {
        seg = &var->segs[(first + i) % var->nsegs];

        if (seg->duration) {
            if (seg->discont) {
                p = ngx_cpymem(p, "#EXT-X-DISCONTINUITY\n", sizeof("#EXT-X-DISCONTINUITY\n") - 1);
            }
            p = ngx_sprintf(p, "#EXTINF:%.2f,\n", seg->duration / 90000.0);
            p = ngx_sprintf(p, "%s%s\n", seg->prefix, seg->name);
        }
    }

    if (!hls->is_live && hls->done) {
        p = ngx_cpymem(p, "\n#EXT-X-ENDLIST\n",
                       sizeof("\n#EXT-X-ENDLIST\n") - 1);
    }

    rc = ngx_ts_hls_write_file_temp(var->m3u8_filename.data, var->tmp_filename.data, data,
                               p - data, ts->log);

    ngx_free(data);

    return rc;
}

static ngx_int_t
ngx_ts_hls_update_master_playlist(ngx_ts_hls_t *hls)
{
    size_t                 len;
    u_char                *p, *data;
    ngx_int_t              rc;
    ngx_uint_t             n;
    ngx_ts_stream_t       *ts;
    ngx_ts_hls_variant_t  *var;

    if (hls->nvars == 1) {
        return NGX_OK;
    }

    ts = hls->ts;

    ngx_log_error(NGX_LOG_DEBUG, ts->log, 0,
                   "ts hls update master playlist \"%s\"", hls->m3u8_path);

    len = sizeof("#EXTM3U\n") - 1;

    for (n = 0; n < hls->nvars; n++) {
        var = &hls->vars[n];

        if (var->bandwidth == 0) {
            ngx_log_debug0(NGX_LOG_DEBUG_CORE, ts->log, 0,
                           "ts hls bandwidth not available");
            return NGX_OK;
        }

        len += sizeof("#EXT-X-STREAM-INF:BANDWIDTH=\n") - 1 + NGX_INT_T_LEN
               + var->m3u8_name.len + strlen("\n");
    }

    data = ngx_alloc(len, ts->log);
    if (data == NULL) {
        return NGX_ERROR;
    }

    p = data;

    p = ngx_cpymem(p, "#EXTM3U\n", sizeof("#EXTM3U\n") - 1);

    for (n = 0; n < hls->nvars; n++) {
        var = &hls->vars[n];

        p = ngx_sprintf(p, "#EXT-X-STREAM-INF:BANDWIDTH=%ui\n%V\n",
                        var->bandwidth, &var->m3u8_name);
    }

    rc = ngx_ts_hls_write_file_temp(hls->m3u8_path, hls->tmp_filename.data, data,
                               p - data, ts->log);

    ngx_free(data);

    return rc;
}

static ngx_int_t
ngx_ts_hls_write_file(u_char *path, u_char *data, size_t len, ngx_log_t *log)
{
    ssize_t                  n;
    ngx_fd_t                 fd;

    fd = ngx_open_file(path,
                       NGX_FILE_WRONLY,
                       NGX_FILE_TRUNCATE,
                       0664);

    if (fd == NGX_INVALID_FILE) {
        ngx_log_error(NGX_LOG_EMERG, log, ngx_errno,
                ngx_open_file_n " \"%s\" failed", path);
        return NGX_ERROR;
    }

    n = ngx_write_fd(fd, data, len);

    if (ngx_close_file(fd) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, log, ngx_errno,
                ngx_close_file_n " \"%s\" failed", path);
        return NGX_ERROR;
    }

    if (n < 0) {
        ngx_log_error(NGX_LOG_ALERT, log, ngx_errno,
                      ngx_write_fd_n " to \"%s\" failed", path);
        return NGX_ERROR;
    }

    if ((size_t) n != len) {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "incomplete write to \"%s\"", path);
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_ts_hls_write_file_temp(u_char *path, u_char *tmp_path, u_char *data, size_t len,
                      ngx_log_t *log)
{

    if (ngx_ts_hls_write_file(tmp_path, data, len, log) != NGX_OK) {
        return NGX_ERROR;
    }

    if (ngx_rename_file(tmp_path, path) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, log, ngx_errno,
                      ngx_rename_file_n " \"%s\" to \"%s\" failed",
                      tmp_path, path);
        return NGX_ERROR;
    }

    return NGX_OK;
}

static void
ngx_ts_hls_restore_playlist(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var)
{
    ngx_file_t                      file;
    ssize_t                         ret;
    off_t                           offset;
    u_char                         *p, *last, *end, *next, *pa, *tmp;
    double                          duration;
    ngx_int_t                       discont;
    ngx_uint_t                      i;
    uint64_t                        mag;
    static u_char                   buffer[4096];
    ngx_flag_t                      encrypted;
    ngx_ts_hls_segment_t           *seg;

    ngx_memzero(&file, sizeof(file));

    file.log = hls->ts->log;
    ngx_str_set(&file.name, "m3u8");

    file.fd = ngx_open_file(var->m3u8_filename.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);
    if (file.fd == NGX_INVALID_FILE) {
        return;
    }

    offset = 0;
    duration = 0;
    discont = 0;
    encrypted = 0;

    for ( ;; ) {

        ret = ngx_read_file(&file, buffer, sizeof(buffer), offset);
        if (ret <= 0) {
            goto done;
        }

        p = buffer;
        end = buffer + ret;

        for ( ;; ) {
            last = ngx_strlchr(p, end, '\n');

            if (last == NULL) {
                if (p == buffer) {
                    goto done;
                }
                break;
            }

            next = last + 1;
            offset += (next - p);

            if (p != last && last[-1] == '\r') {
                last--;
            }

            if (ngx_memcmp(p, NGX_HLS_KEY, NGX_HLS_KEY_LEN) == 0) {
                encrypted = 1;
            }

            if (ngx_memcmp(p, NGX_HLS_MSEQ, NGX_HLS_MSEQ_LEN) == 0) {
                var->seg = (ngx_uint_t) strtod((const char *) &p[NGX_HLS_MSEQ_LEN], NULL);
                var->start_seg = var->seg + var->nsegs;
            }

            if (ngx_memcmp(p, NGX_HLS_EXTINF, NGX_HLS_EXTINF_LEN) == 0) {
                duration = strtod((const char *) &p[NGX_HLS_EXTINF_LEN], NULL);
            }

            if (ngx_memcmp(p, NGX_HLS_DISCONT, NGX_HLS_DISCONT_LEN) == 0) {
                discont = 1;
            }

            if (ngx_memcmp(p, NGX_HLS_DISCONT_SEQ, NGX_HLS_DISCONT_SEQ_LEN) == 0) {
                var->discontinuity_seq = (ngx_uint_t) strtod((const char *) &p[NGX_HLS_DISCONT_SEQ_LEN], NULL);
            }

            // find '.ts\r'

            if (p + 4 <= last &&
                    last[-3] == '.' && last[-2] == 't' && last[-1] == 's')
            {
                seg = &var->segs[var->seg % var->nsegs];
                var->seg++;
                seg->duration = duration * 90000;
                seg->discont = discont;
                tmp = ngx_snprintf(seg->name, last - p, "%s", p);
                ngx_snprintf(tmp, 1, "%Z", "");

                discont = 0;

                mag = 1;
                for (pa = last - 4; pa >= p; pa--) {
                    if (*pa < '0' || *pa > '9') {
                        break;
                    }
                    mag *= 10;
                }
            }
            p = next;
        }
    }

done:
    // if ((1 == encrypted) == (NULL == hls->key_url)) {
    if (1 == encrypted) {
        // restore not supported
        for (i = 0; i < var->nsegs; i++) {
            var->segs[i].duration = 0;
        }
        var->start_seg = var->seg;
    } else {
        ngx_log_error(NGX_LOG_NOTICE, hls->ts->log, 0, "ts hls restore segments:%d", var->nsegs);
        for (i = 0; var->seg && i < var->nsegs; i++) {
            if (var->segs[i].duration)
                ngx_log_error(NGX_LOG_DEBUG, hls->ts->log, 0, "ts hls restore segment \"%s\"", var->segs[i].name);
        }
    }
    ngx_close_file(file.fd);
}

static ngx_int_t
ngx_ts_hls_open_segment(ngx_ts_hls_t *hls, ngx_ts_hls_variant_t *var)
{
    u_char                  *p;
    ngx_err_t                err;
    ngx_uint_t               try, i;
    ngx_chain_t             *out, **ll;
    ngx_ts_stream_t         *ts;
    ngx_ts_hls_segment_t    *seg;
    time_t                   timestamp;
    ngx_string_format_t     *sf;

    if (var->file.fd != NGX_INVALID_FILE) {
        return NGX_OK;
    }

    ts = hls->ts;

    if (var->seg == 0 && hls->conf->restore) {
        ngx_ts_hls_restore_playlist(hls, var);
        var->segs[var->seg % var->nsegs].discont = 1;
    }
    seg = &var->segs[var->seg % var->nsegs];
    timestamp = ngx_time();

    // segment name
    p = seg->name;
    sf = hls->name_templates->elts;
    for (i = 0; i < hls->name_templates->nelts; i++) {
        switch (sf[i].type) {
            case FORMAT_TYPE_STR:
                p = ngx_sprintf(p, "%s", sf[i].data);
                break;
            case FORMAT_TYPE_NAME:
                p = ngx_sprintf(p, "%V", &hls->stream_name);
                break;
            case FORMAT_TYPE_TIME:
                p = ngx_sprintf(p, "%T", timestamp);
                break;
            case FORMAT_TYPE_SEQ:
                p = ngx_sprintf(p, "%ui", var->seg);
                break;
            default:
                break;
        }
    }
    *p = '\0';

    // segment filename
    p = ngx_sprintf(seg->filename, "%V/%s", &var->path, seg->name);
    var->file.name = var->tmp_filename;

    ngx_log_error(NGX_LOG_DEBUG, ts->log, 0,
                   "ts hls open segment \"%s\"", seg->filename);

    // segment prefix
    p = seg->prefix;
    sf = hls->prefix_templates->elts;
    for (i = 0; i < hls->prefix_templates->nelts; i++) {
        switch (sf[i].type) {
            case FORMAT_TYPE_STR:
                p = ngx_sprintf(p, "%s", sf[i].data);
                break;
            case FORMAT_TYPE_NAME:
                p = ngx_sprintf(p, "%V", &hls->stream_name);
                break;
            default:
                break;
        }
    }
    *p = '\0';

    for (try = 0; ; try++) {
        var->file.fd = ngx_open_file(var->file.name.data,
                                        NGX_FILE_WRONLY,
                                        NGX_FILE_TRUNCATE,
                                        0664);

        if (var->file.fd != NGX_INVALID_FILE) {
            break;
        }

        err = ngx_errno;

        if (try || (err != NGX_ENOENT && err != NGX_ENOTDIR)) {
            ngx_log_error(NGX_LOG_EMERG, ts->log, err,
                            ngx_open_file_n " \"%s\" failed", seg->filename);
            return NGX_ERROR;
        }

        /* XXX dir access mode */
        if (ngx_create_dir(var->path.data, 0775) == NGX_FILE_ERROR) {
            err = ngx_errno;

            if (err != NGX_EEXIST) {
                ngx_log_error(NGX_LOG_CRIT, ts->log, err,
                                ngx_create_dir_n " \"%s\" failed",
                                var->path.data);
                return NGX_ERROR;
            }
        }
    }

    if (var->prologue == NULL) {
        out = ngx_ts_write_pat(ts, var->prog);
        if (out == NULL) {
            return NGX_ERROR;
        }

        for (ll = &out; *ll; ll = &(*ll)->next);

        *ll = ngx_ts_write_udt(ts);

        for (ll = &out; *ll; ll = &(*ll)->next);

        *ll = ngx_ts_write_pmt(ts, var->prog);
        if (*ll == NULL) {
            return NGX_ERROR;
        }

        var->prologue = out;
    }

    if (ngx_write_chain_to_file(&var->file, var->prologue, 0, ts->pool)
            == NGX_ERROR) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_msec_t
ngx_ts_hls_file_manager(void *data)
{
    ngx_ts_hls_conf_t *hls = data;

    ngx_tree_ctx_t  tree;

    ngx_log_debug0(NGX_LOG_DEBUG_CORE, ngx_cycle->log, 0,
                   "ts hls file manager");

    tree.init_handler = NULL;
    tree.file_handler = ngx_ts_hls_manage_file;
    tree.pre_tree_handler = ngx_ts_hls_manage_directory;
    tree.post_tree_handler = ngx_ts_hls_delete_directory;
    tree.spec_handler = ngx_ts_hls_delete_file;
    tree.data = hls;
    tree.alloc = 0;
    tree.access = 0775;
    tree.log = ngx_cycle->log;

    (void) ngx_walk_tree(&tree, &hls->path->name);

    return hls->max_seg * hls->nsegs;
}

static ngx_int_t
ngx_ts_hls_manage_file(ngx_tree_ctx_t *ctx, ngx_str_t *path)
{
    ngx_ts_hls_conf_t *hls = ctx->data;

    time_t  age, max_age;

    age = ngx_time() - ctx->mtime;

    max_age = 0;

    if (path->len >= 5
            && ngx_memcmp(path->data + path->len - 5, ".m3u8", 5) == 0)
    {
        max_age = hls->max_seg * hls->nsegs / 1000;
    }
    else if (path->len >= 3
            && ngx_memcmp(path->data + path->len - 3, ".ts", 3) == 0)
    {
        max_age = hls->max_seg * hls->nsegs / 500;
    }
    else if (path->len >= 4
            && ngx_memcmp(path->data + path->len - 4, ".tmp", 4) == 0)
    {
        max_age = hls->max_seg / 1000 + 10;
    }
    else {
        return NGX_OK;
    }

    ngx_log_debug3(NGX_LOG_DEBUG_CORE, ctx->log, 0,
                   "ts hls manage file \"%s\", age:%T, max_age:%T",
                   path->data, age, max_age);

    if (max_age && age < max_age) {
        return NGX_OK;
    }

    return ngx_ts_hls_delete_file(ctx, path);
}

static ngx_int_t
ngx_ts_hls_manage_directory(ngx_tree_ctx_t *ctx, ngx_str_t *path)
{
    // off_t           free;

    // free = ngx_fs_available(path->data);
    // ngx_log_error(NGX_LOG_NOTICE, ctx->log, 0,
    //                "ts hls dir free: \"%O\"", free);
    return NGX_OK;
}

static ngx_int_t
ngx_ts_hls_delete_directory(ngx_tree_ctx_t *ctx, ngx_str_t *path)
{
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, ctx->log, 0,
                   "ts hls delete dir (if empty): \"%s\"", path->data);

    /* non-empty directory will not be removed anyway */

    /* TODO count files instead */

    (void) ngx_delete_dir(path->data);

    return NGX_OK;
}

static ngx_int_t
ngx_ts_hls_delete_file(ngx_tree_ctx_t *ctx, ngx_str_t *path)
{
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, ctx->log, 0,
                   "ts hls delete file: \"%s\"", path->data);

    if (ngx_delete_file(path->data) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_CRIT, ctx->log, ngx_errno,
                      ngx_delete_file_n " \"%s\" failed", path->data);
    }

    return NGX_OK;
}

char *
ngx_ts_hls_set_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    ssize_t             max_size;
    ngx_str_t          *value, s, ss, path, m3u8, prefix, template, key_url;
    ngx_int_t           v;
    ngx_uint_t          i, nsegs, clean, key_flag, max_gop;
    ngx_flag_t          is_live, faststart, low_latency, restore;
    ngx_msec_t          min_seg, max_seg, analyze;
    ngx_ts_hls_conf_t  *hls, **field;

    hls = NULL;
    field = (ngx_ts_hls_conf_t **) (p + cmd->offset);

    if (*field != NGX_CONF_UNSET_PTR) {
        return "is duplicate";
    }

    min_seg = 2000;
    max_seg = 12000;
    analyze = 0;
    max_size = 16 * 1024 * 1024;
    nsegs = 4;
    clean = 1;
    is_live = 1;
    key_flag = 0;
    max_gop = 20;
    faststart = 0;
    low_latency = 0;
    restore = 0;

    ngx_str_null(&key_url);
    ngx_str_set(&path, "/var/www/html/hls");
    ngx_str_set(&m3u8, "index.m3u8");
    ngx_str_set(&prefix, "");
    ngx_str_set(&template, "$name-$time-$seq.ts");

    value = cf->args->elts;
    for (i = 1; i < cf->args->nelts; i++) {

#define K_PATH_LEN strlen("path=")
        if (ngx_strncmp(value[i].data, "path=", K_PATH_LEN) == 0) {

            if (value[i].len > K_PATH_LEN) {
                path.len = value[i].len - K_PATH_LEN;
                path.data = value[i].data + K_PATH_LEN;

                if (path.data[path.len - 1] == '/') {
                    path.len--;
                }
            }

            if (ngx_conf_full_name(cf->cycle, &path, 0) != NGX_OK) {
                return NGX_CONF_ERROR;
            }

            continue;
        }
#define K_M3U8_LEN strlen("m3u8=")
        if (ngx_strncmp(value[i].data, "m3u8=", K_M3U8_LEN) == 0) {

            if (value[i].len > K_M3U8_LEN) {
                m3u8.len = value[i].len - K_M3U8_LEN;
                m3u8.data = value[i].data + K_M3U8_LEN;
            }

            continue;
        }
#define K_TEMPLATE_LEN strlen("template=")
        if (ngx_strncmp(value[i].data, "template=", K_TEMPLATE_LEN) == 0) {

            if (value[i].len > K_TEMPLATE_LEN) {
                template.len = value[i].len - K_TEMPLATE_LEN;
                template.data = value[i].data + K_TEMPLATE_LEN;
            }

            continue;
        }
#define K_PREFIX_LEN strlen("prefix=")
        if (ngx_strncmp(value[i].data, "prefix=", K_PREFIX_LEN) == 0) {

            if (value[i].len > K_PREFIX_LEN) {
                prefix.len = value[i].len - K_PREFIX_LEN;
                prefix.data = value[i].data + K_PREFIX_LEN;
            }

            continue;
        }
#define K_TYPE_LEN strlen("type=")
        if (ngx_strncmp(value[i].data, "type=", K_TYPE_LEN) == 0) {

            if (ngx_strncmp(value[i].data + K_TYPE_LEN, "vod", ngx_max(value[i].len - K_TYPE_LEN, strlen("vod"))) == 0) {
                is_live = 0;
            }

            continue;
        }
#define K_SEGMENT_LEN strlen("segment=")
        if (ngx_strncmp(value[i].data, "segment=", K_SEGMENT_LEN) == 0) {

            s.len = value[i].len - K_SEGMENT_LEN;
            s.data = value[i].data + K_SEGMENT_LEN;

            ss.data = (u_char *) ngx_strchr(s.data, ':');

            if (ss.data) {
                ss.len = s.data + s.len - ss.data - 1;
                s.len = ss.data - s.data;
                ss.data++;
            }

            min_seg = ngx_parse_time(&s, 0);
            if (min_seg == (ngx_msec_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid segment duration value \"%V\"",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            if (ss.data) {
                max_seg = ngx_parse_time(&ss, 0);
                if (max_seg == (ngx_msec_t) NGX_ERROR) {
                    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                       "invalid segment duration value \"%V\"",
                                       &value[i]);
                    return NGX_CONF_ERROR;
                }

            } else {
                max_seg = min_seg * 3;
            }

            continue;
        }
#define K_ANALYZE_LEN strlen("analyze=")
        if (ngx_strncmp(value[i].data, "analyze=", K_ANALYZE_LEN) == 0) {

            s.len = value[i].len - K_ANALYZE_LEN;
            s.data = value[i].data + K_ANALYZE_LEN;

            analyze = ngx_parse_time(&s, 0);
            if (analyze == (ngx_msec_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid analyze duration value \"%V\"",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }
#define K_MAX_SIZE_LEN strlen("max_size=")
        if (ngx_strncmp(value[i].data, "max_size=", K_MAX_SIZE_LEN) == 0) {

            s.len = value[i].len - K_MAX_SIZE_LEN;
            s.data = value[i].data + K_MAX_SIZE_LEN;

            max_size = ngx_parse_size(&s);
            if (max_size == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid max segment size value \"%V\"",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }
#define K_SEGMENTS_LEN strlen("segments=")
        if (ngx_strncmp(value[i].data, "segments=", K_SEGMENTS_LEN) == 0) {

            v = ngx_atoi(value[i].data + K_SEGMENTS_LEN, value[i].len - K_SEGMENTS_LEN);
            if (v == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid segments number value \"%V\"",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            nsegs = v;

            continue;
        }
#define K_MAX_GOP_LEN strlen("max_gop=")
        if (ngx_strncmp(value[i].data, "max_gop=", K_MAX_GOP_LEN) == 0) {

            v = ngx_atoi(value[i].data + K_MAX_GOP_LEN, value[i].len - K_MAX_GOP_LEN);
            if (v == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid keyframes number value \"%V\"",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            max_gop = v;

            continue;
        }
#define K_KEY_FLAG_LEN strlen("key_flag=")
        if (ngx_strncmp(value[i].data, "key_flag=", K_KEY_FLAG_LEN) == 0) {

            v = ngx_atoi(value[i].data + K_KEY_FLAG_LEN, value[i].len - K_KEY_FLAG_LEN);
            if (v == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid key flags value \"%V\"",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            key_flag = v;

            continue;
        }
#define K_KEY_URL_LEN strlen("key_url=")
        if (ngx_strncmp(value[i].data, "key_url=", K_KEY_URL_LEN) == 0) {

            if (value[i].len > K_KEY_URL_LEN) {
                key_url.len = value[i].len - K_KEY_URL_LEN;
                key_url.data = value[i].data + K_KEY_URL_LEN;

                if (key_url.data[key_url.len - 1] == '/') {
                    key_url.len--;
                }
            }

            continue;
        }
#define K_FASTSTART_LEN strlen("faststart=")
        if (ngx_strncmp(value[i].data, "faststart", K_FASTSTART_LEN) == 0) {
            faststart = 1;
            continue;
        }
#define K_LOW_LATENCY_LEN strlen("low_latency=")
        if (ngx_strncmp(value[i].data, "low_latency", K_LOW_LATENCY_LEN) == 0) {
            low_latency = 1;
            continue;
        }
#define K_RESTORE_LEN strlen("restore=")
        if (ngx_strncmp(value[i].data, "restore", K_RESTORE_LEN) == 0) {
            restore = 1;
            continue;
        }
#define K_NOCLEAN_LEN strlen("noclean=")
        if (ngx_strncmp(value[i].data, "noclean", K_NOCLEAN_LEN) == 0) {
            clean = 0;
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    hls = ngx_pcalloc(cf->pool, sizeof(ngx_ts_hls_conf_t));
    if (hls == NULL) {
        return NGX_CONF_ERROR;
    }

    hls->path = ngx_pcalloc(cf->pool, sizeof(ngx_path_t));
    if (hls->path == NULL) {
        return NGX_CONF_ERROR;
    }

    hls->path->name = path;
    ngx_create_dir(path.data, 0775);

    hls->min_seg = min_seg;
    hls->max_seg = max_seg;
    hls->analyze = analyze ? analyze : min_seg;
    hls->max_size = max_size;
    hls->max_gop = max_gop;
    hls->nsegs = nsegs;
    hls->is_live = is_live;
    hls->faststart = faststart;
    hls->low_latency = low_latency;
    hls->restore = restore;

    hls->key_flag = key_flag;
    hls->key_url = key_url;
    hls->m3u8 = m3u8;

    hls->prefix_templates = ngx_ts_hls_parse_template(cf->pool, prefix);
    if (hls->prefix_templates == NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid hls prefix \"%V\"", &prefix);
        return NGX_CONF_ERROR;
    }

    hls->name_templates = ngx_ts_hls_parse_template(cf->pool, template);
    if (hls->name_templates == NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid hls template \"%V\"", &template);
        return NGX_CONF_ERROR;
    }

    if (clean) {
        hls->path->manager = ngx_ts_hls_file_manager;
    }

    hls->path->data = hls;
    hls->path->conf_file = cf->conf_file->file.name.data;
    hls->path->line = cf->conf_file->line;

    if (ngx_add_path(cf, &hls->path) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    *field = hls;

    return NGX_CONF_OK;
}
