/*
 * Copyright (c) 2009, OmniTI Computer Consulting, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *     * Neither the name OmniTI Computer Consulting, Inc. nor the names
 *       of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Author: Theo Schlossnagle
 *
 */

#include <unistd.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "access/xact.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/builtins.h"

/*
 * rabbitmq-c headers.  The layout changed across releases: 0.14+ installs them
 * under <rabbitmq-c/...>, while older releases (e.g. the librabbitmq-dev shipped
 * by Ubuntu/Debian) install them at the top level.  Support both so the
 * extension builds on Ubuntu (primary target) and on newer systems alike.
 */
#if defined(__has_include)
#  if __has_include(<rabbitmq-c/amqp.h>)
#    include <rabbitmq-c/amqp.h>
#    include <rabbitmq-c/tcp_socket.h>
#    include <rabbitmq-c/ssl_socket.h>
#    include <rabbitmq-c/framing.h>
#    define PG_AMQP_HAVE_RABBITMQ_C_PREFIX 1
#  endif
#endif
#ifndef PG_AMQP_HAVE_RABBITMQ_C_PREFIX
#  include <amqp.h>
#  include <amqp_tcp_socket.h>
#  include <amqp_ssl_socket.h>
#  include <amqp_framing.h>
#endif

#define set_bytes_from_text(var,col) do { \
  if(!PG_ARGISNULL(col)) { \
    text *txt = PG_GETARG_TEXT_PP(col); \
    var.bytes = VARDATA_ANY(txt); \
    var.len = VARSIZE_ANY_EXHDR(txt); \
  } \
} while(0)

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
void _PG_init(void);
Datum pg_amqp_exchange_declare(PG_FUNCTION_ARGS);
Datum pg_amqp_publish(PG_FUNCTION_ARGS);
Datum pg_amqp_autonomous_publish(PG_FUNCTION_ARGS);
Datum pg_amqp_disconnect(PG_FUNCTION_ARGS);

/*
 * Tunable timeouts (GUCs, milliseconds).  Historically these were a single
 * hard-coded 30s recv/send timeout that governed connect, login, every publish
 * AND the commit-time flush, so any momentary broker slowness stalled the
 * backend for the full 30s.  They are now split by phase and operator-tunable:
 *
 *   pg_amqp.connect_timeout_ms    TCP/TLS connect handshake (per failover host)
 *   pg_amqp.handshake_timeout_ms  AMQP login / channel.open / tx.select
 *   pg_amqp.operation_timeout_ms  steady-state publish recv/send
 *   pg_amqp.commit_timeout_ms     tx.commit / tx.rollback in the xact callback
 *
 * The commit timeout is deliberately small: amqp_tx_commit runs inside
 * PostgreSQL's COMMIT (XACT_EVENT_COMMIT) AFTER the transaction is already
 * durable and while heavyweight locks are still held, so it must not be allowed
 * to block the backend (and pile up lock waiters / pool slots) for long.
 */
static int amqp_connect_timeout_ms   = 2000;
static int amqp_handshake_timeout_ms = 2000;
static int amqp_operation_timeout_ms = 30000;
static int amqp_commit_timeout_ms    = 2000;
/* Optional guard: reject message bodies larger than this many bytes (0 = no limit). */
static int amqp_max_body_bytes       = 0;
/* Optional aggregate wall-clock budget for ONE connect attempt across ALL failover
 * hosts (0 = unlimited: try every host).  Each host is already bounded by
 * connect_timeout_ms + handshake_timeout_ms; this caps the worst case when a
 * broker_id has many failover rows that all accept TCP but never answer AMQP. */
static int amqp_connect_budget_ms    = 0;

/* Reconnect backoff (seconds).  After a failed connect we suppress further
 * connect attempts to the same broker for an exponentially growing window
 * (base << consecutive_failures, capped), plus a per-backend jitter so a fleet
 * of backends does not form a synchronized reconnect storm against a recovering
 * broker. */
#define AMQP_RECONNECT_BACKOFF_BASE_SEC 2
#define AMQP_RECONNECT_BACKOFF_CAP_SEC  30

/* Bound for the graceful Connection.Close handshake on the explicit
 * amqp.disconnect()/backend-exit paths so a dead broker cannot hang us. */
#define AMQP_GRACEFUL_CLOSE_MS 500

/* Cached connection parameters for one host of a broker.  Strings are owned by
 * TopMemoryContext and released by local_amqp_free_hosts(). */
struct broker_host {
  char *host;
  int   port;
  char *vhost;
  char *user;
  char *pass;
  bool  use_ssl;
  int   ssl_verify;
  char *ssl_cacert;
  char *ssl_cert;
  char *ssl_key;
};

struct brokerstate {
  int broker_id;
  amqp_connection_state_t conn;
  int sockfd;
  int uncommitted;
  int inerror;
  int blocked;                  /* broker sent connection.blocked (resource alarm) */
  int idx;
  int consecutive_failures;     /* drives exponential reconnect backoff */
  time_t last_failed_connect;   /* 0 if last connect succeeded, else time() of failure */
  struct broker_host *hosts;    /* cached amqp.broker rows (NULL until first load) */
  int nhosts;
  struct brokerstate *next;
};

static struct brokerstate *HEAD_BS = NULL;

/* Apply a recv/send timeout (and the rabbitmq-c RPC timeout) to an established
 * connection.  Used to keep different phases on different budgets: a short bound
 * for the login handshake and the commit flush, a longer one for steady-state
 * publishing. */
/* Milliseconds since the epoch; used only for the coarse aggregate connect
 * budget, so wall-clock (rather than monotonic) time is acceptable. */
static long
local_amqp_now_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (long) tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void
local_amqp_set_timeout(struct brokerstate *bs, int ms) {
  struct timeval tv;
  if(!bs) return;
  tv.tv_sec = ms / 1000;
  tv.tv_usec = (ms % 1000) * 1000;
  if(bs->sockfd >= 0) {
    setsockopt(bs->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(bs->sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  }
  if(bs->conn)
    amqp_set_rpc_timeout(bs->conn, &tv);
}

static void
local_amqp_disconnect_bs(struct brokerstate *bs, bool graceful) {
  if(bs && bs->conn) {
    /* graceful close performs a Connection.Close round-trip; only do it when the
     * connection is believed healthy (explicit amqp.disconnect / backend exit),
     * and bound it tightly so a brown-out broker cannot hang the backend.  On
     * error/commit/failover/redo paths we pass graceful=false and skip straight
     * to amqp_destroy_connection(), which closes the underlying socket and frees
     * the TLS state without any blocking handshake. */
    if(graceful) {
      struct timeval tv = { .tv_sec = 0, .tv_usec = AMQP_GRACEFUL_CLOSE_MS * 1000 };
      amqp_set_rpc_timeout(bs->conn, &tv);
      amqp_connection_close(bs->conn, AMQP_REPLY_SUCCESS);
    }
    amqp_destroy_connection(bs->conn);
    bs->conn = NULL;
    bs->sockfd = -1;
    bs->uncommitted = 0;
    bs->blocked = 0;
    /* broker_id, idx, inerror, consecutive_failures, last_failed_connect and the
     * cached host list are deliberately preserved so reconnects keep failing over
     * across hosts and honour the backoff window. */
  }
}

/* Drain any frames the broker has already sent us (without blocking).  Surfaces:
 *   - basic.return (unroutable messages published with mandatory=true)
 *   - connection.blocked / connection.unblocked (broker resource alarms)
 *   - a broker-initiated channel/connection close (connection is dead)
 * Returns 0 if the connection is still usable, -1 if the broker has torn it down
 * or a socket error was seen (caller should reconnect).  In rabbitmq-c there is
 * no return/blocked callback; these arrive as ordinary frames, so we poll for
 * them before every publish and at commit. */
static int
local_amqp_drain_returns(struct brokerstate *bs) {
  amqp_frame_t frame;
  struct timeval zero = { 0, 0 };
  int status;
  if(!bs || !bs->conn) return -1;
  while((status = amqp_simple_wait_frame_noblock(bs->conn, &frame, &zero)) == AMQP_STATUS_OK) {
    if(frame.frame_type == AMQP_FRAME_METHOD) {
      switch(frame.payload.method.id) {
        case AMQP_BASIC_RETURN_METHOD: {
          amqp_basic_return_t *m = (amqp_basic_return_t *) frame.payload.method.decoded;
          elog(WARNING,
               "amqp: unroutable message returned by broker %d "
               "(reply_code=%u reply_text=%.*s exchange=%.*s routing_key=%.*s)",
               bs->broker_id, m->reply_code,
               (int) m->reply_text.len,   (char *) m->reply_text.bytes,
               (int) m->exchange.len,     (char *) m->exchange.bytes,
               (int) m->routing_key.len,  (char *) m->routing_key.bytes);
          break;
        }
        case AMQP_CONNECTION_BLOCKED_METHOD:
          if(!bs->blocked)
            elog(WARNING, "amqp: broker %d has blocked publishers (resource alarm); "
                          "publishes will fail fast until it is unblocked", bs->broker_id);
          bs->blocked = 1;
          break;
        case AMQP_CONNECTION_UNBLOCKED_METHOD:
          bs->blocked = 0;
          break;
        case AMQP_CHANNEL_CLOSE_METHOD:
        case AMQP_CONNECTION_CLOSE_METHOD:
          /* The broker is tearing the channel/connection down; treat as dead. */
          amqp_maybe_release_buffers(bs->conn);
          return -1;
        default:
          break;
      }
    }
    amqp_maybe_release_buffers(bs->conn);
  }
  /* AMQP_STATUS_TIMEOUT means "no frame currently pending" => healthy.  Anything
   * else (connection closed, socket error) means the connection is unusable. */
  return (status == AMQP_STATUS_TIMEOUT) ? 0 : -1;
}

static void amqp_local_phase2(XactEvent event, void *arg) {
  amqp_rpc_reply_t reply;
  struct brokerstate *bs;
  switch(event) {
    case XACT_EVENT_PRE_PREPARE:
      /* Two-phase commit cannot carry the channel-2 AMQP tx buffer across to the
       * resolving session, so PREPARE TRANSACTION would otherwise leave the
       * buffer tied to the wrong transaction's eventual outcome.  Reject the
       * prepare loudly instead of silently mis-delivering. */
      for(bs = HEAD_BS; bs; bs = bs->next) {
        if(bs->uncommitted > 0)
          elog(ERROR,
               "amqp: cannot PREPARE TRANSACTION after amqp.publish(): %d message(s) "
               "are buffered on broker %d and transactional AMQP publishing is not "
               "compatible with two-phase commit. Use amqp.autonomous_publish(), or "
               "publish from a non-prepared transaction / an outbox.",
               bs->uncommitted, bs->broker_id);
      }
      break;
    case XACT_EVENT_PARALLEL_PRE_COMMIT:
    case XACT_EVENT_PRE_COMMIT:
      /* no-op */
      break;
    case XACT_EVENT_COMMIT:
    case XACT_EVENT_PARALLEL_COMMIT:
      for(bs = HEAD_BS; bs; bs = bs->next) {
        /* A channel that errored mid-transaction (or vanished) cannot flush its
         * buffered messages.  Those messages already returned true to the caller;
         * make the resulting loss LOUD instead of silently dropping it -- this
         * runs after PostgreSQL has already durably committed, so it cannot be
         * undone, only reported. */
        if(bs->uncommitted > 0 && (bs->inerror || !bs->conn)) {
          elog(WARNING,
               "amqp: DISCARDING %d already-acknowledged message(s) buffered on "
               "broker %d because the channel errored before commit; these messages "
               "are LOST even though the PostgreSQL transaction committed",
               bs->uncommitted, bs->broker_id);
          local_amqp_disconnect_bs(bs, false);
          bs->inerror = 0;
          bs->uncommitted = 0;
          continue;
        }
        if(bs->inerror) {
          local_amqp_disconnect_bs(bs, false);
          bs->inerror = 0;
        }
        if(!bs->uncommitted) continue;
        if(bs->conn) {
          /* Bound the commit flush tightly: this is on PostgreSQL's commit path
           * with locks held, so a slow/half-open broker must fail fast rather
           * than stall every committing backend for the steady-state timeout. */
          local_amqp_set_timeout(bs, amqp_commit_timeout_ms);
          local_amqp_drain_returns(bs);
          amqp_tx_commit(bs->conn, 2);
          reply = amqp_get_rpc_reply(bs->conn);
          if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
            elog(WARNING,
                 "amqp: COMMIT to broker %d FAILED after PostgreSQL already committed; "
                 "%d message(s) may be LOST (reply_type=%d, library_errno=%d)",
                 bs->broker_id, bs->uncommitted, reply.reply_type, reply.library_error);
            local_amqp_disconnect_bs(bs, false);
          } else {
            /* restore the steady-state budget for the connection's future use */
            local_amqp_set_timeout(bs, amqp_operation_timeout_ms);
          }
        }
        bs->uncommitted = 0;
      }
      break;
    case XACT_EVENT_ABORT:
    case XACT_EVENT_PARALLEL_ABORT:
      for(bs = HEAD_BS; bs; bs = bs->next) {
        if(bs->inerror) local_amqp_disconnect_bs(bs, false);
        bs->inerror = 0;
        if(!bs->uncommitted) continue;
        if(bs->conn) {
          local_amqp_set_timeout(bs, amqp_commit_timeout_ms);
          amqp_tx_rollback(bs->conn, 2);
          reply = amqp_get_rpc_reply(bs->conn);
          if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
            elog(WARNING, "amqp could not rollback tx mode on broker %d, reply_type=%d, library_errno=%d", bs->broker_id, reply.reply_type, reply.library_error);
            local_amqp_disconnect_bs(bs, false);
          } else {
            local_amqp_set_timeout(bs, amqp_operation_timeout_ms);
          }
        }
        bs->uncommitted = 0;
      }
      break;
    case XACT_EVENT_PREPARE:
      /* nothin' */
      return;
      break;
  }
}

/* AMQP tx mode (channel 2) has no nested transactions, so a savepoint /
 * PL/pgSQL EXCEPTION-block rollback cannot selectively undo the messages
 * published inside it.  To avoid delivering rolled-back work, conservatively
 * mark the whole transaction's buffer for discard (it will be dropped, loudly,
 * at commit) rather than letting phantom messages through. */
static void amqp_local_subxact(SubXactEvent event, SubTransactionId mySubid,
                               SubTransactionId parentSubid, void *arg) {
  struct brokerstate *bs;
  if(event != SUBXACT_EVENT_ABORT_SUB) return;
  for(bs = HEAD_BS; bs; bs = bs->next) {
    if(bs->uncommitted > 0 && !bs->inerror) {
      elog(WARNING,
           "amqp: a savepoint/subtransaction rolled back after %d message(s) were "
           "published on broker %d; to avoid delivering rolled-back work the entire "
           "transaction's buffered AMQP messages will be discarded at commit",
           bs->uncommitted, bs->broker_id);
      bs->inerror = 1;
    }
  }
}

static void amqp_atexit(int code, Datum arg) {
  struct brokerstate *bs;
  /* Best-effort clean shutdown so the broker is not left with zombie
   * connections; bounded by AMQP_GRACEFUL_CLOSE_MS inside disconnect_bs. */
  for(bs = HEAD_BS; bs; bs = bs->next)
    local_amqp_disconnect_bs(bs, true);
}

void _PG_init(void) {
  DefineCustomIntVariable("pg_amqp.connect_timeout_ms",
                          "TCP/TLS connect handshake timeout per broker host.",
                          NULL, &amqp_connect_timeout_ms, 2000, 1, INT_MAX,
                          PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
  DefineCustomIntVariable("pg_amqp.handshake_timeout_ms",
                          "AMQP login/channel.open/tx.select timeout per broker host.",
                          NULL, &amqp_handshake_timeout_ms, 2000, 1, INT_MAX,
                          PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
  DefineCustomIntVariable("pg_amqp.operation_timeout_ms",
                          "Steady-state AMQP recv/send timeout for publishing.",
                          NULL, &amqp_operation_timeout_ms, 30000, 1, INT_MAX,
                          PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
  DefineCustomIntVariable("pg_amqp.commit_timeout_ms",
                          "Timeout for the AMQP tx.commit/tx.rollback issued on the "
                          "PostgreSQL commit/abort path. Keep small: it runs with "
                          "locks held after the transaction is already durable.",
                          NULL, &amqp_commit_timeout_ms, 2000, 1, INT_MAX,
                          PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
  DefineCustomIntVariable("pg_amqp.max_body_bytes",
                          "Reject message bodies larger than this many bytes (0 = no limit).",
                          NULL, &amqp_max_body_bytes, 0, 0, INT_MAX,
                          PGC_USERSET, GUC_UNIT_BYTE, NULL, NULL, NULL);
  DefineCustomIntVariable("pg_amqp.connect_budget_ms",
                          "Total wall-clock budget for one connect attempt across all "
                          "failover hosts (0 = unlimited; try every host).",
                          NULL, &amqp_connect_budget_ms, 0, 0, INT_MAX,
                          PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);

  RegisterXactCallback(amqp_local_phase2, NULL);
  RegisterSubXactCallback(amqp_local_subxact, NULL);
  before_shmem_exit(amqp_atexit, 0);
}

static struct brokerstate *
local_amqp_get_a_bs(int broker_id) {
  struct brokerstate *bs;
  for(bs = HEAD_BS; bs; bs = bs->next) {
    if(bs->broker_id == broker_id) return bs;
  }
  bs = MemoryContextAllocZero(TopMemoryContext, sizeof(*bs));
  bs->broker_id = broker_id;
  bs->sockfd = -1;
  bs->next = HEAD_BS;
  HEAD_BS = bs;
  return bs;
}
static char *
local_amqp_top_strdup(const char *s) {
  return s ? MemoryContextStrdup(TopMemoryContext, s) : NULL;
}

static void
local_amqp_free_hosts(struct brokerstate *bs) {
  int i;
  if(!bs->hosts) return;
  for(i = 0; i < bs->nhosts; i++) {
    if(bs->hosts[i].host)       pfree(bs->hosts[i].host);
    if(bs->hosts[i].vhost)      pfree(bs->hosts[i].vhost);
    if(bs->hosts[i].user)       pfree(bs->hosts[i].user);
    if(bs->hosts[i].pass)       pfree(bs->hosts[i].pass);
    if(bs->hosts[i].ssl_cacert) pfree(bs->hosts[i].ssl_cacert);
    if(bs->hosts[i].ssl_cert)   pfree(bs->hosts[i].ssl_cert);
    if(bs->hosts[i].ssl_key)    pfree(bs->hosts[i].ssl_key);
  }
  pfree(bs->hosts);
  bs->hosts = NULL;
  bs->nhosts = 0;
}

/* Enable dead-peer detection (SO_KEEPALIVE tuned to detect a dead broker in tens
 * of seconds rather than the OS default of ~2h) and disable Nagle so small
 * publish frames are not delayed.  heartbeat stays 0 (a synchronous backend
 * cannot emit heartbeat frames while idle), so keepalive is our liveness check. */
static void
local_amqp_set_keepalive(int sockfd) {
  int one = 1;
  if(sockfd < 0) return;
  setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef TCP_KEEPIDLE
  { int v = 30; setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPIDLE, &v, sizeof(v)); }
#elif defined(TCP_KEEPALIVE)
  /* macOS/Darwin: TCP_KEEPALIVE is the idle time before the first probe. */
  { int v = 30; setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPALIVE, &v, sizeof(v)); }
#endif
#ifdef TCP_KEEPINTVL
  { int v = 10; setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPINTVL, &v, sizeof(v)); }
#endif
#ifdef TCP_KEEPCNT
  { int v = 3;  setsockopt(sockfd, IPPROTO_TCP, TCP_KEEPCNT,  &v, sizeof(v)); }
#endif
}

/* Load and cache the amqp.broker rows for this broker.  Returns the number of
 * hosts available (>0), or <= 0 on failure.  The cached copy is reused on every
 * reconnect and only invalidated by amqp.disconnect(), so a single SPI lookup
 * serves the lifetime of the connection. */
static int
local_amqp_load_hosts(struct brokerstate *bs) {
  char sql[1024];
  if(bs->hosts) return bs->nhosts;
  if(SPI_connect() == SPI_ERROR_CONNECT) return -1;
  snprintf(sql, sizeof(sql), "SELECT host, port, vhost, username, password,"
                             "       ssl, ssl_verify, ssl_cacert, ssl_cert, ssl_key"
                             "  FROM amqp.broker "
                             " WHERE broker_id = %d "
                             " ORDER BY host DESC, port", bs->broker_id);
  /* tcount=0: return all configured failover hosts (do not silently truncate). */
  if(SPI_OK_SELECT == SPI_execute(sql, true, 0)) {
    if(SPI_processed > 0) {
      int n = SPI_processed;
      int i;
      bs->hosts = MemoryContextAllocZero(TopMemoryContext,
                                         sizeof(struct broker_host) * n);
      bs->nhosts = n;
      for(i = 0; i < n; i++) {
        struct broker_host *h = &bs->hosts[i];
        HeapTuple tup = SPI_tuptable->vals[i];
        TupleDesc desc = SPI_tuptable->tupdesc;
        Datum d;
        bool is_null;
        char *s;

        s = SPI_getvalue(tup, desc, 1);
        h->host = local_amqp_top_strdup(s ? s : "localhost");

        h->port = 5672;
        d = SPI_getbinval(tup, desc, 2, &is_null);
        if(!is_null) h->port = DatumGetInt32(d);
        if(h->port < 1 || h->port > 65535) {
          elog(WARNING, "amqp: broker %d host '%s' has invalid port %d; using 5672",
               bs->broker_id, h->host, h->port);
          h->port = 5672;
        }

        s = SPI_getvalue(tup, desc, 3);
        h->vhost = local_amqp_top_strdup(s ? s : "/");

        s = SPI_getvalue(tup, desc, 4);
        h->user = local_amqp_top_strdup(s ? s : "guest");

        s = SPI_getvalue(tup, desc, 5);
        h->pass = local_amqp_top_strdup(s ? s : "guest");

        h->use_ssl = false;
        d = SPI_getbinval(tup, desc, 6, &is_null);
        if(!is_null) h->use_ssl = DatumGetBool(d);

        h->ssl_verify = 1;
        d = SPI_getbinval(tup, desc, 7, &is_null);
        if(!is_null) h->ssl_verify = DatumGetBool(d) ? 1 : 0;

        h->ssl_cacert = local_amqp_top_strdup(SPI_getvalue(tup, desc, 8));
        h->ssl_cert   = local_amqp_top_strdup(SPI_getvalue(tup, desc, 9));
        h->ssl_key    = local_amqp_top_strdup(SPI_getvalue(tup, desc, 10));

        if(h->use_ssl && !h->ssl_verify)
          elog(WARNING, "amqp: broker %d host '%s' uses TLS with ssl_verify=false; "
                        "the server certificate is NOT validated (MITM/credential "
                        "capture risk). Restrict write access to amqp.broker.",
               bs->broker_id, h->host);
      }
      /* Seed the starting host per-backend so a fleet of backends spreads its
       * connections across cluster nodes instead of all pinning to the same row. */
      if(n > 0)
        bs->idx = (int)(((unsigned int) MyProcPid) % (unsigned int) n);
    } else {
      elog(WARNING, "amqp can't find broker %d", bs->broker_id);
    }
  } else {
    elog(WARNING, "amqp broker lookup query failed");
  }
  SPI_finish();
  return bs->nhosts;
}

/* Create and configure the socket (TCP or TLS) for one broker host and open it.
 * Returns the amqp_socket_t on success (owned by bs->conn) or NULL on failure. */
static amqp_socket_t *
local_amqp_open_socket(struct brokerstate *bs, struct broker_host *h,
                       const char *host_copy) {
  struct timeval ctv;
  amqp_socket_t *socket;

  ctv.tv_sec  = amqp_connect_timeout_ms / 1000;
  ctv.tv_usec = (amqp_connect_timeout_ms % 1000) * 1000;

  if(h->use_ssl) {
    socket = amqp_ssl_socket_new(bs->conn);
    if(!socket) {
      elog(WARNING, "amqp[%s] could not allocate TLS socket on broker %d",
           host_copy, bs->broker_id);
      return NULL;
    }
    /* Require TLSv1.2 or newer (matches the previous custom handshake). */
    amqp_ssl_socket_set_ssl_versions(socket, AMQP_TLSv1_2, AMQP_TLSvLATEST);
    amqp_ssl_socket_set_verify_peer(socket, h->ssl_verify ? 1 : 0);
    amqp_ssl_socket_set_verify_hostname(socket, h->ssl_verify ? 1 : 0);
    if(h->ssl_cacert)
      amqp_ssl_socket_set_cacert(socket, h->ssl_cacert);
    if(h->ssl_cert && h->ssl_key)
      amqp_ssl_socket_set_key(socket, h->ssl_cert, h->ssl_key);
  } else {
    socket = amqp_tcp_socket_new(bs->conn);
    if(!socket) {
      elog(WARNING, "amqp[%s] could not allocate TCP socket on broker %d",
           host_copy, bs->broker_id);
      return NULL;
    }
  }

  if(amqp_socket_open_noblock(socket, h->host, h->port, &ctv) != AMQP_STATUS_OK) {
    elog(WARNING, "amqp[%s] socket/connect failed on broker %d",
         host_copy, bs->broker_id);
    return NULL;
  }
  return socket;
}

/* Log advertising the connection.blocked capability so the broker will notify us
 * (via connection.blocked / connection.unblocked frames) when it applies
 * resource-alarm flow control, letting publishes fail fast instead of stalling
 * on a full socket buffer. */
static amqp_rpc_reply_t
local_amqp_login(struct brokerstate *bs, struct broker_host *h) {
  amqp_table_entry_t cap_entries[1];
  amqp_table_entry_t prop_entries[1];
  amqp_table_t capabilities;
  amqp_table_t client_properties;

  cap_entries[0].key = amqp_cstring_bytes("connection.blocked");
  cap_entries[0].value.kind = AMQP_FIELD_KIND_BOOLEAN;
  cap_entries[0].value.value.boolean = 1;
  capabilities.num_entries = 1;
  capabilities.entries = cap_entries;

  prop_entries[0].key = amqp_cstring_bytes("capabilities");
  prop_entries[0].value.kind = AMQP_FIELD_KIND_TABLE;
  prop_entries[0].value.value.table = capabilities;
  client_properties.num_entries = 1;
  client_properties.entries = prop_entries;

  /* heartbeat=0: a synchronous PG backend cannot emit heartbeat frames while
   * idle, so we rely on TCP keepalive (set on the socket) for dead-peer
   * detection. frame_max=131072. */
  return amqp_login_with_properties(bs->conn, h->vhost, 0, 131072, 0,
                                    &client_properties,
                                    AMQP_SASL_METHOD_PLAIN, h->user, h->pass);
}

static struct brokerstate *
local_amqp_get_bs(int broker_id) {
  int tries;
  long deadline = 0;
  struct brokerstate *bs = local_amqp_get_a_bs(broker_id);
  if(bs->conn) return bs;

  /* Negative cache with exponential backoff + per-backend jitter: don't
   * re-attempt (and pay the connect timeout) against a broker we just failed to
   * reach, and don't let a fleet form a synchronized reconnect storm. */
  if(bs->last_failed_connect != 0) {
    int shift = bs->consecutive_failures > 4 ? 4 : bs->consecutive_failures;
    int backoff = AMQP_RECONNECT_BACKOFF_BASE_SEC << shift;
    if(backoff > AMQP_RECONNECT_BACKOFF_CAP_SEC)
      backoff = AMQP_RECONNECT_BACKOFF_CAP_SEC;
    backoff += (int)(((unsigned int) MyProcPid) % (AMQP_RECONNECT_BACKOFF_BASE_SEC + 1));
    if((time(NULL) - bs->last_failed_connect) < backoff)
      return bs;
  }

  if(local_amqp_load_hosts(bs) <= 0) {
    bs->consecutive_failures++;
    bs->last_failed_connect = time(NULL);
    return bs;
  }

  if(amqp_connect_budget_ms > 0)
    deadline = local_amqp_now_ms() + amqp_connect_budget_ms;

  for(tries = bs->nhosts; tries > 0; tries--) {
    struct broker_host *h;
    char host_copy[300];
    amqp_rpc_reply_t s_reply;

    bs->idx = (bs->idx + 1) % bs->nhosts;
    h = &bs->hosts[bs->idx];
    snprintf(host_copy, sizeof(host_copy), "%s:%d", h->host, h->port);

    bs->conn = amqp_new_connection();
    if(!bs->conn) break;
    if(!local_amqp_open_socket(bs, h, host_copy))
      goto next_host;

    /* Tune the established socket and bound the AMQP handshake with the short
     * handshake timeout (not the long steady-state timeout), so a host that
     * accepts TCP but never answers login cannot stall the backend for the full
     * operation timeout. */
    bs->sockfd = amqp_get_sockfd(bs->conn);
    local_amqp_set_keepalive(bs->sockfd);
    local_amqp_set_timeout(bs, amqp_handshake_timeout_ms);

    s_reply = local_amqp_login(bs, h);
    if(s_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      elog(WARNING, "amqp[%s] login failed on broker %d", host_copy, broker_id);
      goto next_host;
    }
    amqp_channel_open(bs->conn, 1);
    s_reply = amqp_get_rpc_reply(bs->conn);
    if(s_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      elog(WARNING, "amqp[%s] channel open failed on broker %d", host_copy, broker_id);
      goto next_host;
    }
    amqp_channel_open(bs->conn, 2);
    s_reply = amqp_get_rpc_reply(bs->conn);
    if(s_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      elog(WARNING, "amqp[%s] channel open failed on broker %d", host_copy, broker_id);
      goto next_host;
    }
    amqp_tx_select(bs->conn, 2);
    s_reply = amqp_get_rpc_reply(bs->conn);
    if(s_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      elog(WARNING, "amqp[%s] could not start tx mode on broker %d", host_copy, broker_id);
      goto next_host;
    }
    /* Connected: switch to the steady-state budget and clear failure state. */
    local_amqp_set_timeout(bs, amqp_operation_timeout_ms);
    bs->last_failed_connect = 0;
    bs->consecutive_failures = 0;
    bs->blocked = 0;
    return bs;

   next_host:
    local_amqp_disconnect_bs(bs, false);
    if(deadline != 0 && local_amqp_now_ms() >= deadline) {
      elog(WARNING, "amqp: connect budget (%dms) exhausted for broker %d; "
                    "abandoning failover", amqp_connect_budget_ms, broker_id);
      break;
    }
    if(tries > 1)
      elog(WARNING, "amqp[%s] failed on trying next host", host_copy);
  }

  /* Every host failed: grow the backoff window. */
  bs->consecutive_failures++;
  bs->last_failed_connect = time(NULL);
  return bs;
}
static void
local_amqp_disconnect(int broker_id) {
  struct brokerstate *bs = local_amqp_get_a_bs(broker_id);
  /* User-initiated, connection presumed healthy: close gracefully (bounded). */
  local_amqp_disconnect_bs(bs, true);
  /* Full reset: clear error/backoff state and drop the cached broker config so
   * the next publish re-reads amqp.broker.  This is how configuration changes
   * (e.g. rotated credentials or a new failover host) take effect without
   * restarting the backend. */
  bs->inerror = 0;
  bs->idx = 0;
  bs->consecutive_failures = 0;
  bs->last_failed_connect = 0;
  local_amqp_free_hosts(bs);
}

PG_FUNCTION_INFO_V1(pg_amqp_exchange_declare);
Datum
pg_amqp_exchange_declare(PG_FUNCTION_ARGS) {
  struct brokerstate *bs;
  if(!PG_ARGISNULL(0)) {
    int broker_id;
    broker_id = PG_GETARG_INT32(0);
    bs = local_amqp_get_bs(broker_id);
    if(bs && bs->conn) {
      amqp_rpc_reply_t reply;
      amqp_bytes_t exchange_b = amqp_empty_bytes;
      amqp_bytes_t exchange_type_b = amqp_empty_bytes;
      amqp_boolean_t passive = 0;
      amqp_boolean_t durable = 0;
      amqp_boolean_t auto_delete = 0;

      set_bytes_from_text(exchange_b,1);
      set_bytes_from_text(exchange_type_b,2);
      /* Tolerate NULL boolean arguments (default them to false) instead of
       * reading indeterminate values. */
      passive     = PG_ARGISNULL(3) ? 0 : PG_GETARG_BOOL(3);
      durable     = PG_ARGISNULL(4) ? 0 : PG_GETARG_BOOL(4);
      auto_delete = PG_ARGISNULL(5) ? 0 : PG_GETARG_BOOL(5);
      amqp_exchange_declare(bs->conn, 1,
                            exchange_b, exchange_type_b,
                            passive, durable, auto_delete, 0 /* internal */,
                            amqp_empty_table);
      reply = amqp_get_rpc_reply(bs->conn);
      if(reply.reply_type == AMQP_RESPONSE_NORMAL)
        PG_RETURN_BOOL(0 == 0);
      /* A declare failure is a channel-1 problem and must NOT poison the
       * channel-2 transactional buffer (which would silently discard unrelated
       * amqp.publish() messages at commit).  The broker closes channel 1 on a
       * channel exception, so drop the connection; the next publish reconnects. */
      local_amqp_disconnect_bs(bs, false);
    }
  }
  PG_RETURN_BOOL(0 != 0);
}
static Datum
pg_amqp_publish_opt(PG_FUNCTION_ARGS, int channel) {
  struct brokerstate *bs;
  if(!PG_ARGISNULL(0)) {
    int broker_id;
    amqp_basic_properties_t properties;

    int once_more = 1;
    broker_id = PG_GETARG_INT32(0);

    /* Optional body-size guard, evaluated before we touch the broker. */
    if(amqp_max_body_bytes > 0 && !PG_ARGISNULL(3)) {
      text *body = PG_GETARG_TEXT_PP(3);
      int blen = (int) VARSIZE_ANY_EXHDR(body);
      if(blen > amqp_max_body_bytes) {
        elog(WARNING, "amqp: message body (%d bytes) exceeds pg_amqp.max_body_bytes "
                      "(%d bytes); not published", blen, amqp_max_body_bytes);
        PG_RETURN_BOOL(0 != 0);
      }
    }
  redo:
    bs = local_amqp_get_bs(broker_id);
    if(bs && bs->conn && (channel == 1 || !bs->inerror)) {
      int rv;
      /* mandatory (optional arg 8): ask the broker to return the message via
       * basic.return instead of silently dropping it when it is unroutable.
       * Returned messages are reported by local_amqp_drain_returns(). */
      amqp_boolean_t mandatory = 0;
      amqp_boolean_t immediate = 0;
      amqp_bytes_t exchange_b = amqp_cstring_bytes("amq.direct");
      amqp_bytes_t routing_key_b = amqp_cstring_bytes("");
      amqp_bytes_t body_b = amqp_cstring_bytes("");

      /* Surface anything the broker already pushed (returns from earlier
       * publishes, connection.blocked, or a broker-initiated close) before we
       * publish.  A dead connection here is reconnected once. */
      if(local_amqp_drain_returns(bs) != 0) {
        if(once_more && (channel == 1 || bs->uncommitted == 0)) {
          once_more = 0;
          local_amqp_disconnect_bs(bs, false);
          goto redo;
        }
        if(channel == 2) bs->inerror = 1;
        PG_RETURN_BOOL(0 != 0);
      }

      /* If the broker has flagged us blocked (resource alarm), fail fast rather
       * than block on a full socket buffer for the operation timeout. */
      if(bs->blocked) {
        elog(WARNING, "amqp: broker %d is blocking publishers (resource alarm); "
                      "message not published", broker_id);
        PG_RETURN_BOOL(0 != 0);
      }

      properties._flags = 0;
      if(PG_NARGS() > 8 && !PG_ARGISNULL(8))
        mandatory = PG_GETARG_BOOL(8);

      /* Sets delivery_mode */
      if (!PG_ARGISNULL(4)) {
	  if (PG_GETARG_INT32(4) == 1 || PG_GETARG_INT32(4) == 2) {
	      properties._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
              properties.delivery_mode = PG_GETARG_INT32(4);
	  } else {
              elog(WARNING, "Ignored delivery_mode %d, value should be 1 or 2",
                  PG_GETARG_INT32(4));
	  }
      }

      /* Sets content_type */
      if (!PG_ARGISNULL(5)) {
	  properties._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
	  set_bytes_from_text(properties.content_type, 5);
      }

      /* Sets reply_to */
      if (!PG_ARGISNULL(6)) {
	  properties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
	  set_bytes_from_text(properties.reply_to, 6);
      }

      /* Sets correlation_id */
      if (!PG_ARGISNULL(7)) {
	  properties._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
	  set_bytes_from_text(properties.correlation_id, 7);
      }

      set_bytes_from_text(exchange_b,1);
      set_bytes_from_text(routing_key_b,2);
      set_bytes_from_text(body_b,3);

      if (properties._flags == 0) {
          rv = amqp_basic_publish(bs->conn, channel, exchange_b, routing_key_b,
                                  mandatory, immediate, NULL, body_b);
      } else {
          rv = amqp_basic_publish(bs->conn, channel, exchange_b, routing_key_b,
                                  mandatory, immediate, &properties, body_b);
      }

      /* On the autonomous (non-transactional) channel there is no later read in
       * which to surface a basic.return, so poll for one now (best-effort). */
      if(channel == 1 && mandatory)
        local_amqp_drain_returns(bs);

      /* The int return of amqp_basic_publish is the only authoritative signal
       * for an asynchronous basic.publish (it reflects the local socket write);
       * amqp_get_rpc_reply would only echo stale state from an earlier RPC, so we
       * do not consult it here. */
      if(rv != AMQP_STATUS_OK) {
        if(once_more && (channel == 1 || bs->uncommitted == 0)) {
          once_more = 0;
          local_amqp_disconnect_bs(bs, false);
          goto redo;
        }
        bs->inerror = 1;
        PG_RETURN_BOOL(0 != 0);
      }
      /* channel two is transactional */
      if(channel == 2) bs->uncommitted++;
      PG_RETURN_BOOL(rv == AMQP_STATUS_OK);
    }
  }
  PG_RETURN_BOOL(0 != 0);
}

PG_FUNCTION_INFO_V1(pg_amqp_publish);
Datum
pg_amqp_publish(PG_FUNCTION_ARGS) {
  return pg_amqp_publish_opt(fcinfo, 2);
}

PG_FUNCTION_INFO_V1(pg_amqp_autonomous_publish);
Datum
pg_amqp_autonomous_publish(PG_FUNCTION_ARGS) {
  return pg_amqp_publish_opt(fcinfo, 1);
}

PG_FUNCTION_INFO_V1(pg_amqp_disconnect);
Datum
pg_amqp_disconnect(PG_FUNCTION_ARGS) {
  if(!PG_ARGISNULL(0)) {
    int broker_id;
    broker_id = PG_GETARG_INT32(0);
    local_amqp_disconnect(broker_id);
  }
  PG_RETURN_VOID();
}
