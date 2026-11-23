
/*
 * Copyright (C) Roman Arutyunyan
 */


#include <ngx_config.h>
#include <ngx_core.h>


#ifndef _NGX_TS_STREAM_H_INCLUDED_
#define _NGX_TS_STREAM_H_INCLUDED_


/*
 * ISO/IEC 13818-1 : 2000 (E)
 * Table 2-29 – Stream type assignments, p. 48
 *
 * ISO/IEC 13818-1:2007/Amd.3:2009 (E)
 * Table 2-34 – Stream type assignments, p. 6
 */

#define NGX_TS_VIDEO_MPEG1     0x01 /* ISO/IEC 11172-2, MPEG-1 Video */
#define NGX_TS_VIDEO_MPEG2     0x02 /* ISO/IEC 13818-2, MPEG-2 Video */
#define NGX_TS_VIDEO_MPEG4     0x10 /* ISO/IEC 14496-2, MPEG-4 Video */
#define NGX_TS_VIDEO_AVC       0x1b /* ISO/IEC 14496-10, AVC */
#define NGX_TS_VIDEO_HEVC      0x24 /* HEVC */

#define NGX_TS_AUDIO_MPEG1     0x03 /* ISO/IEC 11172-3, MPEG-1 Audio */
#define NGX_TS_AUDIO_MPEG2     0x04 /* ISO/IEC 13818-3, MPEG-2 Audio */
#define NGX_TS_AUDIO_AAC       0x0f /* ISO/IEC 13818-7, MPEG-2 AAC ADTS Audio */

#define PKT_KEY_FLAG            0x0001
#define PKT_KEY_TEMP            0x0003
#define PKT_NO_VIDEO            0x0010
#define PKT_NO_GOP              0x0100
#define PKT_TS_INDEX            0x1000

typedef enum {
    NGX_TS_PAT = 0,
    NGX_TS_PMT,
    NGX_TS_PES,
    NGX_TS_UDT = 0x81
} ngx_ts_event_e;

#define PAT_TID   0x00
#define PMT_TID   0x02
#define UDT_TID   0x81

typedef struct {
    u_char                        type;
    u_char                        sid;
    u_char                        scramble;
    u_char                        cont;
    uint16_t                      pid;
    uint64_t                      pts;
    uint64_t                      dts;
    unsigned                      ptsf:1;
    unsigned                      rand:1;
    unsigned                      video:1;
    ngx_uint_t                    num_buf;
    ngx_chain_t                  *bufs;  /* ES */
} ngx_ts_es_t;


typedef struct {
    uint16_t                      number;
    uint16_t                      pid;
    uint16_t                      pcr_pid;
    uint64_t                      pcr;
    unsigned                      video:1;
    ngx_uint_t                    nes;
    ngx_ts_es_t                  *es;
    ngx_chain_t                  *pmt_bufs;  /* PMT */
} ngx_ts_program_t;


typedef struct ngx_ts_handler_s  ngx_ts_handler_t;


typedef struct {
    int                           udt_key_version;
    int                           udt_key_len;
    u_char                       *udt_key;
    unsigned                      video:1;
    ngx_uint_t                    nprogs;
    ngx_ts_program_t             *progs;
    ngx_log_t                    *log;
    ngx_pool_t                   *pool;
    ngx_buf_t                    *buf;
    ngx_chain_t                  *free;
    ngx_chain_t                  *tmp_bufs; /* tmp */
    ngx_chain_t                  *pat_bufs; /* PAT */
    ngx_chain_t                  *index_tables;
    ngx_ts_handler_t             *handlers;
    void                         *data;
} ngx_ts_stream_t;


typedef struct {
    ngx_ts_event_e                event;
    ngx_ts_stream_t              *ts;
    ngx_ts_program_t             *prog;
    ngx_ts_es_t                  *es;
    ngx_chain_t                  *bufs;
    void                         *data;
} ngx_ts_handler_data_t;


typedef ngx_int_t (*ngx_ts_handler_pt)(ngx_ts_handler_data_t *hd);


struct ngx_ts_handler_s {
    ngx_ts_handler_pt             handler;
    void                         *data;
    ngx_ts_handler_t             *next;
};


ngx_int_t ngx_ts_add_handler(ngx_ts_stream_t *ts, ngx_ts_handler_pt handler,
    void *data);
ngx_int_t ngx_ts_read(ngx_ts_stream_t *ts, ngx_chain_t *in);
ngx_int_t ngx_ts_read_buf(ngx_ts_stream_t *ts, u_char *data, size_t size, ngx_uint_t *flag, size_t *kpos);
ngx_chain_t *ngx_ts_write_udt(ngx_ts_stream_t *ts);
ngx_chain_t *ngx_ts_write_pat(ngx_ts_stream_t *ts, ngx_ts_program_t *prog);
ngx_chain_t *ngx_ts_write_pmt(ngx_ts_stream_t *ts, ngx_ts_program_t *prog);
ngx_chain_t *ngx_ts_write_pes(ngx_ts_stream_t *ts, ngx_ts_program_t *prog,
    ngx_ts_es_t *es, ngx_chain_t *bufs);
void ngx_ts_free_chain(ngx_ts_stream_t *ts, ngx_chain_t **ll);
ngx_uint_t ngx_ts_dash_get_oti(u_char type);
ngx_int_t ngx_ts_build_index_tables(ngx_ts_stream_t *ts);


#endif /* _NGX_TS_STREAM_H_INCLUDED_ */
