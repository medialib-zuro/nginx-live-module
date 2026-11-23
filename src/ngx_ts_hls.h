
/*
 * Copyright (C) Roman Arutyunyan
 */


#include <ngx_config.h>
#include <ngx_core.h>

#include "ngx_ts_stream.h"


#ifndef _NGX_TS_HLS_H_INCLUDED_
#define _NGX_TS_HLS_H_INCLUDED_

#define KEY_LENGTH              16
#define MAX_FILENAME_LEN        1024

#define NGX_HLS_TMP_EXT         ".tmp"
#define NGX_HLS_M3U8_EXT        ".m3u8"

#define NGX_HLS_MSEQ           "#EXT-X-MEDIA-SEQUENCE:"
#define NGX_HLS_MSEQ_LEN       (sizeof(NGX_HLS_MSEQ) - 1)
#define NGX_HLS_DISCONT        "#EXT-X-DISCONTINUITY"
#define NGX_HLS_DISCONT_LEN    (sizeof(NGX_HLS_DISCONT) - 1)
#define NGX_HLS_DISCONT_SEQ        "#EXT-X-DISCONTINUITY-SEQUENCE:"
#define NGX_HLS_DISCONT_SEQ_LEN    (sizeof(NGX_HLS_DISCONT_SEQ) - 1)
#define NGX_HLS_KEY            "#EXT-X-KEY:"
#define NGX_HLS_KEY_LEN        (sizeof(NGX_HLS_KEY) - 1)
#define NGX_HLS_EXTINF         "#EXTINF:"
#define NGX_HLS_EXTINF_LEN     (sizeof(NGX_HLS_EXTINF) - 1)
#define NGX_HLS_VERSION        "#EXT-X-VERSION:"
#define NGX_HLS_VERSION_LEN    (sizeof(NGX_HLS_VERSION) - 1)
#define NGX_HLS_DURATION        "#EXT-X-TARGETDURATION:"
#define NGX_HLS_DURATION_LEN    (sizeof(NGX_HLS_DURATION) - 1)
#define NGX_HLS_ENDLIST        "#EXT-X-ENDLIST"
#define NGX_HLS_ENDLIST_LEN    (sizeof(NGX_HLS_ENDLIST_LEN) - 1)


typedef enum
{
    FORMAT_TYPE_STR = 0,
    FORMAT_TYPE_NAME,
    FORMAT_TYPE_TIME,
    FORMAT_TYPE_SEQ,
} NGX_FORMAT_TYPE;

typedef struct
{
    NGX_FORMAT_TYPE type;
    void *data;
} ngx_string_format_t;

typedef enum
{
    ENCRYPT_NONE = 0,
    ENCRYPT_AES,
} key_flag;

typedef struct {
    ngx_path_t              *path;
    ngx_array_t             *prefix_templates;
    ngx_array_t             *name_templates;
    ngx_msec_t              min_seg;
    ngx_msec_t              max_seg;
    ngx_msec_t              analyze;
    size_t                  max_size;
    ngx_uint_t              max_gop;
    ngx_uint_t              nsegs;
    ngx_uint_t              key_flag;
    ngx_str_t               key_url;
    ngx_str_t               m3u8;
    ngx_flag_t              is_live;
    ngx_flag_t              faststart;
    ngx_flag_t              low_latency;
    ngx_flag_t              restore;
} ngx_ts_hls_conf_t;


typedef struct {
    uint64_t                duration;
    off_t                   size;
    ngx_uint_t              discont;
    u_char                  *filename; // with path
    u_char                  *name;
    u_char                  *prefix;
} ngx_ts_hls_segment_t;


typedef struct {
    ngx_file_t              file;
    ngx_chain_t             *prologue;

    ngx_uint_t              bandwidth;
    ngx_uint_t              bandwidth_bytes;
    uint64_t                bandwidth_dts;

    ngx_ts_hls_segment_t    *segs;
    ngx_uint_t              nsegs;
    ngx_uint_t              seg;
    ngx_uint_t              start_seg;
    ngx_uint_t              discontinuity_seq;
    uint64_t                seg_dts;

    ngx_str_t               tmp_filename;
    ngx_str_t               m3u8_name;
    ngx_str_t               m3u8_filename;
    ngx_str_t               path;
    ngx_uint_t              video_keyframes;

    ngx_ts_program_t        *prog;
} ngx_ts_hls_variant_t;


typedef struct {
    ngx_ts_stream_t         *ts;
    ngx_ts_hls_conf_t       *conf;

    ngx_str_t               path;
    ngx_str_t               stream_name;
    ngx_str_t               m3u8_name;
    ngx_str_t               tmp_filename;
    u_char                  *m3u8_path;
    u_char                  *key_url;
    u_char                  *key_path;
    u_char                  key[KEY_LENGTH + 1];
    u_char                  iv[KEY_LENGTH + 1];
    u_char                  key_hex[KEY_LENGTH * 2 + 1];
    u_char                  iv_hex[KEY_LENGTH * 2 + 1];
    ngx_array_t             *prefix_templates;
    ngx_array_t             *name_templates;
    ngx_ts_hls_variant_t    *vars;
    ngx_uint_t              nvars;
    ngx_flag_t              done;  /* unsigned  done:1; */
    ngx_flag_t              is_live;  /* live:1; vod:0;*/
} ngx_ts_hls_t;


ngx_ts_hls_t *ngx_ts_hls_create(ngx_ts_hls_conf_t *conf, ngx_ts_stream_t *ts,
    ngx_str_t *name);
char *ngx_ts_hls_set_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


#endif /* _NGX_TS_HLS_H_INCLUDED_ */
