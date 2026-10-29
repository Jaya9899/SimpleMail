/* smserver.c - SimpleMail frontend server (producer side of SM_SPOOL).
 *
 * Usage: ./smserver <port> <userfile>      (start AFTER smspooler, same cwd)
 *
 * Single-threaded, poll()-driven. Every socket is non-blocking and each
 * client is a small state machine, so one slow client never stalls others.
 *
 *   SMTP2 (MODE SEND): FROM -> TO+ -> SUB -> BODY | BODY_STREAM -> queued
 *   SMP   (MODE RECV): AUTH (nonce + DJB2) -> LIST/READ/DELETE/COUNT/QUIT
 *
 * The server never creates mail files: completed messages go to the shared
 * ring and smspooler writes them. (It only reads / deletes mailbox files
 * for SMP.)
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "smcommon.h"
#include "smutil.h"

#define MAX_CLIENTS       256
#define INBUF_CAP         8192
#define MODE_TIMEOUT_SEC  30
#define OUT_HIGH_WATER    (256 * 1024)
#define MAX_AUTH_FAILURES 3

typedef enum {
    ST_MODE,      /* waiting for MODE SEND / MODE RECV            */
    ST_FROM,      /* SMTP2: expect FROM (or QUIT)                  */
    ST_RCPT,      /* SMTP2: expect TO (SUB once >=1 accepted)      */
    ST_BODYCMD,   /* SMTP2: expect BODY or BODY_STREAM             */
    ST_BODY,      /* SMTP2: legacy dot-terminated body             */
    ST_STREAM,    /* SMTP2: BODY_STREAM chunks / END_BODY          */
    ST_ENQUEUE,   /* SMTP2: complete, waiting for a free ring slot */
    ST_AUTH,      /* SMP: waiting for AUTH                         */
    ST_MAIL       /* SMP: authenticated                            */
} State;

typedef struct {
    int      fd;
    unsigned id;
    char     addr[64];
    State    state;
    time_t   connected;
    int      closing;            /* close once output is flushed */
    int      dead;               /* remove at end of loop iteration */

    unsigned char inbuf[INBUF_CAP];
    size_t   in_len;

    unsigned char *out;
    size_t   out_len, out_off, out_cap;

    /* SMTP2 message under construction */
    char     sender[128];
    char     rcpt[SM_MAX_RECIPIENTS][SM_MAX_USERNAME + 1];
    int      nrcpt;
    char     subject[256];
    unsigned char *body;         /* SM_MAX_BODY bytes, allocated when body starts */
    size_t   body_len;
    int      mid_line;           /* legacy BODY: inside a line split across reads */

    /* credit-based flow control (BODY_STREAM) */
    long     window;             /* credits the client may still spend        */
    long     pending;            /* consumed bytes not yet returned as credit */
    size_t   chunk_need;         /* raw bytes still expected for current CHUNK */

    /* SMP */
    char     nonce[9];
    int      auth_fails;
    char     user[SM_MAX_USERNAME + 1];
} Client;

static volatile sig_atomic_t g_stop = 0;
static SMUser   g_users[SM_MAX_USERS];
static int      g_nusers;
static SMSPool *g_spool;
static Client  *g_clients[MAX_CLIENTS];
static int      g_nclients;
static unsigned g_next_cid = 1;

static void on_signal(int sig) { (void)sig; g_stop = 1; }

/* ------------------------------------------------------------------ */
/* Output buffering                                                    */
/* ------------------------------------------------------------------ */

static void out_append(Client *c, const void *data, size_t n)
{
    if (c->out_off == c->out_len)
        c->out_off = c->out_len = 0;
    if (c->out_len + n > c->out_cap) {
        if (c->out_off) {
            memmove(c->out, c->out + c->out_off, c->out_len - c->out_off);
            c->out_len -= c->out_off;
            c->out_off = 0;
        }
        if (c->out_len + n > c->out_cap) {
            size_t ncap = c->out_cap ? c->out_cap * 2 : 1024;
            while (ncap < c->out_len + n)
                ncap *= 2;
            unsigned char *p = realloc(c->out, ncap);
            if (!p) {
                c->dead = 1;
                return;
            }
            c->out = p;
            c->out_cap = ncap;
        }
    }
    memcpy(c->out + c->out_len, data, n);
    c->out_len += n;
}

static void send_line(Client *c, const char *fmt, ...)
{
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof buf - 2, fmt, ap);
    va_end(ap);
    if (n < 0)
        return;
    if (n > (int)sizeof buf - 3)
        n = (int)sizeof buf - 3;
    buf[n++] = '\r';
    buf[n++] = '\n';
    out_append(c, buf, (size_t)n);
}

static size_t out_pending(const Client *c) { return c->out_len - c->out_off; }

static void flush_out(Client *c)
{
    while (!c->dead && c->out_off < c->out_len) {
        ssize_t n = send(c->fd, c->out + c->out_off, c->out_len - c->out_off,
                         MSG_NOSIGNAL);
        if (n > 0)
            c->out_off += (size_t)n;
        else if (n < 0 && errno == EINTR)
            continue;
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            break;
        else
            c->dead = 1;
    }
}

/* ------------------------------------------------------------------ */
/* Input helpers                                                       */
/* ------------------------------------------------------------------ */

static void consume(Client *c, size_t n)
{
    memmove(c->inbuf, c->inbuf + n, c->in_len - n);
    c->in_len -= n;
}

/* 1 = line ready (NUL-terminated in place, CR/LF stripped),
 * 0 = need more data, -1 = longer than maxlen. */
static int get_line(Client *c, char **line, size_t *consumed, size_t maxlen)
{
    unsigned char *nl = memchr(c->inbuf, '\n', c->in_len);
    if (!nl)
        return c->in_len >= maxlen ? -1 : 0;
    size_t n = (size_t)(nl - c->inbuf) + 1;
    if (n > maxlen)
        return -1;
    size_t l = n - 1;
    if (l > 0 && c->inbuf[l - 1] == '\r')
        l--;
    c->inbuf[l] = '\0';
    *line = (char *)c->inbuf;
    *consumed = n;
    return 1;
}

static void rtrim(char *s)
{
    size_t l = strlen(s);
    while (l > 0 && (s[l - 1] == ' ' || s[l - 1] == '\t'))
        s[--l] = '\0';
}

static char *split_cmd(char *line, char **arg)
{
    rtrim(line);
    char *sp = strchr(line, ' ');
    if (sp) {
        *sp++ = '\0';
        while (*sp == ' ')
            sp++;
        *arg = sp;
    } else {
        *arg = line + strlen(line);
    }
    return line;
}

/* ------------------------------------------------------------------ */
/* Mailbox access (read / delete only - never creates mail files)      */
/* ------------------------------------------------------------------ */

static int cmp_ul(const void *a, const void *b)
{
    unsigned long x = *(const unsigned long *)a, y = *(const unsigned long *)b;
    return (x > y) - (x < y);
}

/* Sorted ids of N.txt files in the user's mailbox. Caller frees *out. */
static int mailbox_ids(const char *user, unsigned long **out)
{
    char dir[256];
    snprintf(dir, sizeof dir, "%s/%.*s", SM_MAILBOX_DIR, SM_MAX_USERNAME, user);
    *out = NULL;
    DIR *d = opendir(dir);
    if (!d)
        return 0;
    size_t cap = 16, n = 0;
    unsigned long *ids = malloc(cap * sizeof *ids);
    if (!ids) {
        closedir(d);
        return -1;
    }
    struct dirent *e;
    while ((e = readdir(d))) {
        char *end;
        unsigned long v = strtoul(e->d_name, &end, 10);
        if (end == e->d_name || strcmp(end, ".txt") != 0)
            continue;
        if (n == cap) {
            unsigned long *t = realloc(ids, cap * 2 * sizeof *ids);
            if (!t) {
                free(ids);
                closedir(d);
                return -1;
            }
            ids = t;
            cap *= 2;
        }
        ids[n++] = v;
    }
    closedir(d);
    qsort(ids, n, sizeof *ids, cmp_ul);
    *out = ids;
    return (int)n;
}

static void mail_path(char *buf, size_t len, const char *user, unsigned long id)
{
    snprintf(buf, len, "%s/%.*s/%lu.txt", SM_MAILBOX_DIR, SM_MAX_USERNAME, user, id);
}

static void read_headers(const char *path, char *from, size_t fl,
                         char *subj, size_t sl, char *date, size_t dl)
{
    from[0] = subj[0] = date[0] = '\0';
    FILE *f = fopen(path, "r");
    if (!f)
        return;
    char line[1024];
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strcmp(line, "---") == 0)
            break;
        if (strncmp(line, "From: ", 6) == 0)
            snprintf(from, fl, "%.*s", (int)fl - 1, line + 6);
        else if (strncmp(line, "Subject: ", 9) == 0)
            snprintf(subj, sl, "%.*s", (int)sl - 1, line + 9);
        else if (strncmp(line, "Date: ", 6) == 0)
            snprintf(date, dl, "%.*s", (int)dl - 1, line + 6);
    }
    fclose(f);
}

static unsigned char *read_file(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) {
        fclose(f);
        return NULL;
    }
    unsigned char *buf = malloc((size_t)sz + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    *len = got;
    return buf;
}

static void cmd_list(Client *c)
{
    unsigned long *ids;
    int n = mailbox_ids(c->user, &ids);
    if (n < 0) {
        send_line(c, "ERR Server error");
        return;
    }
    send_line(c, "OK %d messages", n);
    for (int i = 0; i < n; i++) {
        char path[256], from[256], subj[512], date[64], row[1024];
        mail_path(path, sizeof path, c->user, ids[i]);
        read_headers(path, from, sizeof from, subj, sizeof subj, date, sizeof date);
        int l = snprintf(row, sizeof row - 2, "%lu\t%s\t%s\t%s", ids[i], from, subj, date);
        if (l > (int)sizeof row - 3)
            l = (int)sizeof row - 3;
        row[l++] = '\r';
        row[l++] = '\n';
        out_append(c, row, (size_t)l);
    }
    out_append(c, ".\r\n", 3);
    free(ids);
    sm_log("client#%u: %s LIST (%d messages)", c->id, c->user, n);
}

static void cmd_read(Client *c, const char *arg)
{
    char *end;
    errno = 0;
    unsigned long id = strtoul(arg, &end, 10);
    if (end == arg || *end != '\0' || errno) {
        send_line(c, "ERR Invalid message id");
        return;
    }
    char path[256];
    size_t len = 0;
    mail_path(path, sizeof path, c->user, id);
    unsigned char *buf = read_file(path, &len);
    if (!buf) {
        send_line(c, "ERR No such message");
        return;
    }
    send_line(c, "OK");
    size_t i = 0;
    while (i < len) {
        size_t j = i;
        while (j < len && buf[j] != '\n')
            j++;
        size_t e = j;
        if (e > i && buf[e - 1] == '\r')
            e--;
        if (e > i && buf[i] == '.')
            out_append(c, ".", 1);          /* dot-stuffing */
        out_append(c, buf + i, e - i);
        out_append(c, "\r\n", 2);
        i = j + 1;
    }
    out_append(c, ".\r\n", 3);
    free(buf);
    sm_log("client#%u: %s READ %lu", c->id, c->user, id);
}

static void cmd_delete(Client *c, const char *arg)
{
    char *end;
    errno = 0;
    unsigned long id = strtoul(arg, &end, 10);
    if (end == arg || *end != '\0' || errno) {
        send_line(c, "ERR Invalid message id");
        return;
    }
    char path[256];
    mail_path(path, sizeof path, c->user, id);
    if (unlink(path) == 0) {
        send_line(c, "OK Deleted");
        sm_log("client#%u: %s DELETE %lu", c->id, c->user, id);
    } else {
        send_line(c, "ERR No such message");
    }
}

static void cmd_count(Client *c)
{
    unsigned long *ids;
    int n = mailbox_ids(c->user, &ids);
    free(ids);
    if (n < 0) {
        send_line(c, "ERR Server error");
        return;
    }
    send_line(c, "OK %d", n);
    sm_log("client#%u: %s COUNT (%d)", c->id, c->user, n);
}

/* ------------------------------------------------------------------ */
/* SM_SPOOL enqueue                                                    */
/* ------------------------------------------------------------------ */

static void reset_message(Client *c)
{
    c->sender[0] = '\0';
    c->nrcpt = 0;
    c->subject[0] = '\0';
    free(c->body);
    c->body = NULL;
    c->body_len = 0;
    c->mid_line = 0;
    c->window = 0;
    c->pending = 0;
    c->chunk_need = 0;
}

/* Non-blocking: returns 1 if the message was queued, 0 if the ring is full
 * (caller leaves the client in ST_ENQUEUE and retries on the next loop). */
static int try_enqueue(Client *c)
{
    if (sem_trywait(&g_spool->empty_slots) != 0)
        return 0;

    while (sem_wait(&g_spool->ring_mutex) == -1 && errno == EINTR)
        ;
    uint32_t idx = g_spool->head;
    g_spool->head = (idx + 1) % SM_SPOOL_CAPACITY;
    uint64_t ticket = ++g_spool->next_ticket_id;
    sem_post(&g_spool->ring_mutex);

    /* Slot idx is reserved for us; the spooler will not touch it until we
     * post full_slots. (Single producer, so slots fill in ring order.) */
    SMMailMessage *m = &g_spool->slots[idx];
    snprintf(m->sender, sizeof m->sender, "%s", c->sender);
    m->recipient_count = (uint32_t)c->nrcpt;
    for (int i = 0; i < c->nrcpt; i++)
        snprintf(m->recipients[i], sizeof m->recipients[i], "%s", c->rcpt[i]);
    snprintf(m->subject, sizeof m->subject, "%s", c->subject);
    m->body_len = (uint32_t)c->body_len;
    if (c->body_len)
        memcpy(m->body, c->body, c->body_len);
    m->ticket_id = ticket;
    sem_post(&g_spool->full_slots);

    send_line(c, "OK Queued %llu to %d mailboxes", (unsigned long long)ticket, c->nrcpt);
    sm_log("client#%u: mail queued to shared memory (ticket %llu, from '%s', %d recipient(s), %zu bytes)",
           c->id, (unsigned long long)ticket, c->sender, c->nrcpt, c->body_len);
    reset_message(c);
    c->state = ST_FROM;
    return 1;
}

static void finish_body(Client *c)
{
    c->state = ST_ENQUEUE;
    try_enqueue(c);
}

static int body_append(Client *c, const void *p, size_t n)
{
    if (c->body_len + n > SM_MAX_BODY) {
        send_line(c, "ERR Body too large");
        sm_log("client#%u: body exceeds %d bytes, closing", c->id, SM_MAX_BODY);
        c->closing = 1;
        return -1;
    }
    memcpy(c->body + c->body_len, p, n);
    c->body_len += n;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Protocol handlers                                                   */
/* ------------------------------------------------------------------ */

static void make_nonce(char *out)
{
    static const char cs[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    unsigned char r[8];
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f || fread(r, 1, 8, f) != 8)
        for (int i = 0; i < 8; i++)
            r[i] = (unsigned char)rand();
    if (f)
        fclose(f);
    for (int i = 0; i < 8; i++)
        out[i] = cs[r[i] % 62];
    out[8] = '\0';
}

static void handle_mode(Client *c, char *line)
{
    char *arg;
    char *cmd = split_cmd(line, &arg);

    if (strcasecmp(cmd, "MODE") == 0 && strcasecmp(arg, "SEND") == 0) {
        c->state = ST_FROM;
        send_line(c, "OK");
        sm_log("client#%u: mode SEND selected", c->id);
    } else if (strcasecmp(cmd, "MODE") == 0 && strcasecmp(arg, "RECV") == 0) {
        make_nonce(c->nonce);
        c->state = ST_AUTH;
        send_line(c, "OK");
        send_line(c, "AUTH REQUIRED %s", c->nonce);
        sm_log("client#%u: mode RECV selected, nonce issued", c->id);
    } else {
        send_line(c, "ERR Unknown mode");
    }
}

static int is_smtp_verb(const char *s)
{
    static const char *v[] = { "FROM", "TO", "SUB", "BODY", "BODY_STREAM",
                               "CHUNK", "END_BODY", NULL };
    for (int i = 0; v[i]; i++)
        if (strcasecmp(s, v[i]) == 0)
            return 1;
    return 0;
}

static void start_body(Client *c)
{
    c->body = malloc(SM_MAX_BODY);
    if (!c->body) {
        send_line(c, "ERR Server error");
        c->closing = 1;
        return;
    }
    c->body_len = 0;
    c->mid_line = 0;
}

static void handle_smtp(Client *c, char *line)
{
    char *arg;
    char *cmd = split_cmd(line, &arg);

    if (strcasecmp(cmd, "QUIT") == 0) {
        send_line(c, "BYE");
        c->closing = 1;
        return;
    }
    if (!is_smtp_verb(cmd)) {
        send_line(c, "ERR Unknown command");
        return;
    }

    if (strcasecmp(cmd, "FROM") == 0 && c->state == ST_FROM) {
        if (!*arg) {
            send_line(c, "ERR Bad syntax");
            return;
        }
        reset_message(c);
        snprintf(c->sender, sizeof c->sender, "%s", arg);
        c->state = ST_RCPT;
        send_line(c, "OK Sender accepted.");

    } else if (strcasecmp(cmd, "TO") == 0 && c->state == ST_RCPT) {
        char name[SM_MAX_USERNAME + 2];
        snprintf(name, sizeof name, "%s", arg);
        sm_lowercase(name);
        if (!*name || !sm_find_user(g_users, g_nusers, name)) {
            send_line(c, "ERR No such user.");
            return;
        }
        int dup = 0;
        for (int i = 0; i < c->nrcpt; i++)
            if (strcmp(c->rcpt[i], name) == 0)
                dup = 1;
        if (!dup) {
            if (c->nrcpt >= SM_MAX_RECIPIENTS) {
                send_line(c, "ERR Too many recipients");
                return;
            }
            strcpy(c->rcpt[c->nrcpt++], name);
        }
        send_line(c, "OK Recipient accepted.");

    } else if (strcasecmp(cmd, "SUB") == 0 && c->state == ST_RCPT && c->nrcpt > 0) {
        snprintf(c->subject, sizeof c->subject, "%s", *arg ? arg : "(no subject)");
        c->state = ST_BODYCMD;
        send_line(c, "OK Subject accepted.");

    } else if (strcasecmp(cmd, "BODY") == 0 && c->state == ST_BODYCMD) {
        start_body(c);
        if (c->closing)
            return;
        c->state = ST_BODY;
        send_line(c, "OK Send body, end with CRLF.CRLF.");

    } else if (strcasecmp(cmd, "BODY_STREAM") == 0 && c->state == ST_BODYCMD) {
        start_body(c);
        if (c->closing)
            return;
        c->state = ST_STREAM;
        c->window = SM_INITIAL_WINDOW;
        c->pending = 0;
        c->chunk_need = 0;
        send_line(c, "OK Window %ld", c->window);

    } else if (strcasecmp(cmd, "CHUNK") == 0 && c->state == ST_STREAM) {
        char *end;
        errno = 0;
        unsigned long n = strtoul(arg, &end, 10);
        if (end == arg || *end != '\0' || errno || n == 0) {
            send_line(c, "ERR Bad syntax");
            return;
        }
        if ((long)n > c->window || n > SM_MAX_BODY) {
            send_line(c, "ERR Window overflow");
            sm_log("client#%u: window overflow (chunk %lu, credit %ld), dropping",
                   c->id, n, c->window);
            c->closing = 1;
            return;
        }
        if (c->body_len + n > SM_MAX_BODY) {
            send_line(c, "ERR Body too large");
            sm_log("client#%u: body exceeds %d bytes, closing", c->id, SM_MAX_BODY);
            c->closing = 1;
            return;
        }
        c->window -= (long)n;
        c->chunk_need = n;                 /* raw bytes follow the header line */

    } else if (strcasecmp(cmd, "END_BODY") == 0 && c->state == ST_STREAM) {
        finish_body(c);

    } else {
        send_line(c, "ERR Bad sequence");
    }
}

/* Legacy BODY mode. Returns 1 if progress was made, 0 if more data needed. */
static int handle_body_data(Client *c)
{
    unsigned char *nl = memchr(c->inbuf, '\n', c->in_len);
    if (!nl) {
        if (c->in_len < INBUF_CAP)
            return 0;
        /* Over-long line: take it as a fragment (keep a trailing CR back). */
        size_t take = c->in_len;
        if (c->inbuf[take - 1] == '\r')
            take--;
        if (body_append(c, c->inbuf, take) < 0)
            return 1;
        c->mid_line = 1;
        consume(c, take);
        return 1;
    }
    size_t n = (size_t)(nl - c->inbuf) + 1;
    size_t l = n - 1;
    if (l > 0 && c->inbuf[l - 1] == '\r')
        l--;

    if (!c->mid_line && l == 1 && c->inbuf[0] == '.') {   /* CRLF.CRLF */
        consume(c, n);
        finish_body(c);
        return 1;
    }
    size_t off = (!c->mid_line && l > 0 && c->inbuf[0] == '.') ? 1 : 0; /* un-stuff */
    if (body_append(c, c->inbuf + off, l - off) < 0 || body_append(c, "\n", 1) < 0)
        return 1;
    c->mid_line = 0;
    consume(c, n);
    return 1;
}

static void handle_smp(Client *c, char *line)
{
    char *arg;
    char *cmd = split_cmd(line, &arg);

    if (strcasecmp(cmd, "QUIT") == 0) {
        send_line(c, "BYE");
        c->closing = 1;
        return;
    }

    if (c->state == ST_AUTH) {
        if (strcasecmp(cmd, "AUTH") != 0) {
            send_line(c, "ERR Authentication required");
            return;
        }
        char *hs = strchr(arg, ' ');
        if (!*arg || !hs) {
            send_line(c, "ERR Bad syntax");
            return;
        }
        *hs++ = '\0';
        while (*hs == ' ')
            hs++;
        if (!*hs) {
            send_line(c, "ERR Bad syntax");
            return;
        }

        char uname[SM_MAX_USERNAME + 2];
        snprintf(uname, sizeof uname, "%s", arg);
        sm_lowercase(uname);
        const SMUser *u = sm_find_user(g_users, g_nusers, uname);

        int ok = 0;
        if (u) {
            char buf[SM_MAX_PASSWORD + 16];
            snprintf(buf, sizeof buf, "%s%s", u->password, c->nonce);
            char *end;
            errno = 0;
            unsigned long long given = strtoull(hs, &end, 10);
            if (end != hs && *end == '\0' && errno == 0 &&
                (uint64_t)given == sm_djb2(buf))
                ok = 1;
        }
        if (ok) {
            snprintf(c->user, sizeof c->user, "%s", u->username);
            c->state = ST_MAIL;
            send_line(c, "OK Welcome %s", c->user);
            sm_log("client#%u: authentication success for '%s'", c->id, c->user);
        } else {
            c->auth_fails++;
            sm_log("client#%u: authentication FAILED for '%.20s' (attempt %d/%d)",
                   c->id, uname, c->auth_fails, MAX_AUTH_FAILURES);
            if (c->auth_fails >= MAX_AUTH_FAILURES) {
                send_line(c, "ERR Too many failures");
                c->closing = 1;
            } else {
                send_line(c, "ERR Authentication failed");
            }
        }
        return;
    }

    /* ST_MAIL */
    if (strcasecmp(cmd, "LIST") == 0)
        cmd_list(c);
    else if (strcasecmp(cmd, "READ") == 0)
        cmd_read(c, arg);
    else if (strcasecmp(cmd, "DELETE") == 0)
        cmd_delete(c, arg);
    else if (strcasecmp(cmd, "COUNT") == 0)
        cmd_count(c);
    else
        send_line(c, "ERR Unknown command");
}

static void process_input(Client *c)
{
    while (!c->closing && !c->dead && c->in_len > 0) {
        if (c->state == ST_ENQUEUE)
            break;                                  /* hold input until queued */

        if (c->state == ST_BODY) {
            if (!handle_body_data(c))
                break;
            continue;
        }

        if (c->state == ST_STREAM && c->chunk_need > 0) {
            size_t take = c->in_len < c->chunk_need ? c->in_len : c->chunk_need;
            memcpy(c->body + c->body_len, c->inbuf, take);
            c->body_len += take;
            c->chunk_need -= take;
            c->pending += (long)take;
            consume(c, take);
            if (c->chunk_need == 0)
                send_line(c, "OK Window %ld", c->window);
            continue;
        }

        char *line;
        size_t used;
        int r = get_line(c, &line, &used, SM_LINE_MAX);
        if (r == 0)
            break;
        if (r < 0) {
            send_line(c, "ERR Line too long");
            c->closing = 1;
            break;
        }
        if (c->state == ST_MODE)
            handle_mode(c, line);
        else if (c->state == ST_AUTH || c->state == ST_MAIL)
            handle_smp(c, line);
        else
            handle_smtp(c, line);
        consume(c, used);
    }
}

/* ------------------------------------------------------------------ */
/* Credit replenishment & deferred enqueue (run every loop iteration)  */
/* ------------------------------------------------------------------ */

/* Returns 1 if some client is waiting on the ring (poll should wake soon). */
static int service_clients(void)
{
    int fast = 0;
    for (int i = 0; i < g_nclients; i++) {
        Client *c = g_clients[i];
        if (c->dead || c->closing)
            continue;

        if (c->state == ST_ENQUEUE) {
            if (try_enqueue(c))
                process_input(c);              /* pipelined next command */
            else
                fast = 1;
        } else if (c->state == ST_STREAM && c->chunk_need == 0 && c->pending > 0 &&
                   (c->window == 0 || c->pending >= SM_INITIAL_WINDOW / 2)) {
            /* Backpressure: credit is returned only while the ring has room
             * for the message we are accumulating. A slow spooler (full
             * ring) therefore stalls senders at zero window. */
            int free_slots = 0;
            sem_getvalue(&g_spool->empty_slots, &free_slots);
            if (free_slots > 0) {
                send_line(c, "WINDOW_UPDATE %ld", c->pending);
                c->window += c->pending;
                c->pending = 0;
            } else {
                fast = 1;
            }
        }
    }
    return fast;
}

/* ------------------------------------------------------------------ */
/* Connection management                                               */
/* ------------------------------------------------------------------ */

static void destroy_client(Client *c)
{
    sm_log("client#%u (%s) disconnected", c->id, c->addr);
    close(c->fd);
    free(c->out);
    free(c->body);
    free(c);
}

static void accept_clients(int lfd)
{
    for (;;) {
        struct sockaddr_in sa;
        socklen_t sl = sizeof sa;
        int fd = accept(lfd, (struct sockaddr *)&sa, &sl);
        if (fd < 0)
            return;                              /* EAGAIN / EINTR / error */

        if (g_nclients >= MAX_CLIENTS) {
            const char *msg = "ERR Server busy\r\n";
            send(fd, msg, strlen(msg), MSG_NOSIGNAL);
            close(fd);
            continue;
        }
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

        Client *c = calloc(1, sizeof *c);
        if (!c) {
            close(fd);
            continue;
        }
        c->fd = fd;
        c->id = g_next_cid++;
        c->state = ST_MODE;
        c->connected = time(NULL);
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sa.sin_addr, ip, sizeof ip);
        snprintf(c->addr, sizeof c->addr, "%s:%u", ip, ntohs(sa.sin_port));
        g_clients[g_nclients++] = c;

        sm_log("new connection client#%u from %s", c->id, c->addr);
        send_line(c, "WELCOME SimpleMail v1.0");
        flush_out(c);
    }
}

static void do_read(Client *c)
{
    size_t space = INBUF_CAP - c->in_len;
    if (space == 0)
        return;
    ssize_t n = recv(c->fd, c->inbuf + c->in_len, space, 0);
    if (n > 0) {
        c->in_len += (size_t)n;
        process_input(c);
    } else if (n == 0) {
        c->dead = 1;
    } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        c->dead = 1;
    }
}

/* ------------------------------------------------------------------ */

static int attach_spool(void)
{
    int id = shmget(SM_SPOOL_KEY, sizeof(SMSPool), 0666);
    if (id < 0)
        return -1;
    void *p = shmat(id, NULL, 0);
    if (p == (void *)-1)
        return -1;
    g_spool = p;
    if (g_spool->magic != SM_SPOOL_MAGIC || !g_spool->initialized) {
        shmdt(p);
        g_spool = NULL;
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <port> <userfile>\n", argv[0]);
        return 1;
    }
    int port = atoi(argv[1]);
    if (port < 1 || port > 65535) {
        fprintf(stderr, "smserver: invalid port '%s'\n", argv[1]);
        return 1;
    }

    char err[256];
    g_nusers = sm_load_users(argv[2], g_users, SM_MAX_USERS, err, sizeof err);
    if (g_nusers < 0) {
        fprintf(stderr, "smserver: %s\n", err);
        return 1;
    }
    if (attach_spool() != 0) {
        fprintf(stderr, "smserver: cannot attach to the shared-memory spool. "
                        "Start smspooler first (from this same directory).\n");
        return 1;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL) ^ (unsigned)getpid());

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("socket");
        return 1;
    }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(lfd, 64) < 0) {
        perror("listen");
        return 1;
    }
    fcntl(lfd, F_SETFL, fcntl(lfd, F_GETFL, 0) | O_NONBLOCK);

    sm_log("smserver listening on port %d (%d users)", port, g_nusers);

    struct pollfd pfds[MAX_CLIENTS + 1];
    Client *map[MAX_CLIENTS];
    int fast = 0;

    while (!g_stop) {
        int nfds = 0, nmap = 0;
        pfds[nfds].fd = lfd;
        pfds[nfds].events = POLLIN;
        pfds[nfds].revents = 0;
        nfds++;
        for (int i = 0; i < g_nclients; i++) {
            Client *c = g_clients[i];
            short ev = 0;
            if (!c->closing && c->state != ST_ENQUEUE && out_pending(c) < OUT_HIGH_WATER)
                ev |= POLLIN;
            if (out_pending(c))
                ev |= POLLOUT;
            pfds[nfds].fd = c->fd;
            pfds[nfds].events = ev;
            pfds[nfds].revents = 0;
            map[nmap++] = c;
            nfds++;
        }

        int rc = poll(pfds, (nfds_t)nfds, fast ? 50 : 1000);
        if (rc < 0 && errno != EINTR) {
            perror("poll");
            break;
        }

        if (rc > 0) {
            if (pfds[0].revents & POLLIN)
                accept_clients(lfd);
            for (int i = 0; i < nmap; i++) {
                Client *c = map[i];
                short re = pfds[i + 1].revents;
                if (c->dead || !re)
                    continue;
                if (re & POLLERR) {
                    c->dead = 1;
                    continue;
                }
                if (re & POLLOUT)
                    flush_out(c);
                if ((re & (POLLIN | POLLHUP)) && c->state != ST_ENQUEUE && !c->closing)
                    do_read(c);
            }
        }

        fast = service_clients();

        time_t now = time(NULL);
        int j = 0;
        for (int i = 0; i < g_nclients; i++) {
            Client *c = g_clients[i];
            if (!c->dead)
                flush_out(c);
            if (c->state == ST_MODE && !c->dead && now - c->connected >= MODE_TIMEOUT_SEC) {
                sm_log("client#%u: no MODE within %d seconds, closing", c->id, MODE_TIMEOUT_SEC);
                c->dead = 1;
            }
            if (c->closing && !c->dead && out_pending(c) == 0)
                c->dead = 1;
            if (c->dead)
                destroy_client(c);
            else
                g_clients[j++] = c;
        }
        g_nclients = j;
    }

    sm_log("smserver shutting down");
    for (int i = 0; i < g_nclients; i++)
        destroy_client(g_clients[i]);
    close(lfd);
    shmdt(g_spool);          /* the spooler owns and removes the segment */
    return 0;
}
