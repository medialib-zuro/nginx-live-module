#ifndef _NGX_HTTP_LIVE_MODELE_H_INCLUDED_
#define _NGX_HTTP_LIVE_MODELE_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

ngx_flag_t ngx_http_live_actived(ngx_http_request_t *r);
ngx_flag_t ngx_http_live_existed(ngx_http_request_t *r);

#endif //_NGX_HTTP_LIVE_MODELE_H_INCLUDED_

