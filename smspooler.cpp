/* smspooler.cpp - SimpleMail background spooler (consumer side of SM_SPOOL).
 *
 * Usage: ./smspooler <userfile>
 *
 * Creates and initialises the System V shared-memory ring, then loops:
 *   sem_wait(full) -> lock ring_mutex -> take slot at tail -> unlock
 *   -> write mail files -> sem_post(empty)
 * Only this process ever writes into mailboxes/.
 */
#include <dirent.h>
#include <cerrno>
#include <semaphore.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctime>
#include <unistd.h>

#include "smutil.h"   /* also pulls in smcommon.h */

static volatile sig_atomic_t g_stop = 0;
static SMUser   g_users[SM_MAX_USERS];
static uint64_t g_next_id[SM_MAX_USERS];
static int      g_nusers;
static int      g_shmid = -1;
static SMSPool *g_spool = nullptr;

static void on_signal(int sig) { (void)sig; g_stop = 1; }

/* ------------------------------------------------------------------ */
/* Mailbox management                                                  */
/* ------------------------------------------------------------------ */

/* Next id = max(highest existing N.txt + 1, persisted .next_id counter).
 * The persisted counter is what keeps ids from being reused after the
 * highest-numbered mail was deleted and the spooler restarted. */
static uint64_t scan_next_id(const char *dir)
{
    uint64_t maxid = 0, saved = 0;
    DIR *d = opendir(dir);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            char *end;
            unsigned long long v = strtoull(e->d_name, &end, 10);
            if (end != e->d_name && strcmp(end, ".txt") == 0 && v > maxid)
                maxid = v;
        }
        closedir(d);
    }
    char p[512];
    snprintf(p, sizeof p, "%s/.next_id", dir);
    FILE *f = fopen(p, "r");
    if (f) {
        unsigned long long v;
        if (fscanf(f, "%llu", &v) == 1)
            saved = v;
        fclose(f);
    }
    return (saved > maxid + 1) ? saved : maxid + 1;
}

static void persist_next_id(const char *dir, uint64_t next)
{
    char tmp[512], fin[512];
    snprintf(tmp, sizeof tmp, "%s/.next_id.tmp", dir);
    snprintf(fin, sizeof fin, "%s/.next_id", dir);
    FILE *f = fopen(tmp, "w");
    if (!f)
        return;
    fprintf(f, "%llu\n", (unsigned long long)next);
    if (fclose(f) == 0)
        rename(tmp, fin);
    else
        unlink(tmp);
}

static void init_mailboxes(void)
{
    if (mkdir(SM_MAILBOX_DIR, 0755) != 0 && errno != EEXIST) {
        perror("mkdir " SM_MAILBOX_DIR);
        exit(1);
    }
    for (int i = 0; i < g_nusers; i++) {
        char dir[256];
        snprintf(dir, sizeof dir, "%s/%.*s", SM_MAILBOX_DIR, SM_MAX_USERNAME, g_users[i].username);
        if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
            perror(dir);
            exit(1);
        }
        g_next_id[i] = scan_next_id(dir);
        sm_log("mailbox ready: %s (next id %llu)", dir,
               (unsigned long long)g_next_id[i]);
    }
}

static int user_index(const char *name)
{
    for (int i = 0; i < g_nusers; i++)
        if (strcmp(g_users[i].username, name) == 0)
            return i;
    return -1;
}

static int deliver(const SMMailMessage *m, int ui, const char *tolist, const char *date)
{
    char dir[256], tmp[512], fin[512];
    uint64_t id = g_next_id[ui];

    snprintf(dir, sizeof dir, "%s/%.*s", SM_MAILBOX_DIR, SM_MAX_USERNAME, g_users[ui].username);
    snprintf(tmp, sizeof tmp, "%s/.tmp_%llu", dir, (unsigned long long)id);
    snprintf(fin, sizeof fin, "%s/%llu.txt", dir, (unsigned long long)id);

    FILE *f = fopen(tmp, "w");
    if (!f) {
        sm_log("spooler ERROR: cannot create %s: %s", tmp, strerror(errno));
        return -1;
    }
    int bad = fprintf(f, "From: %s\nTo: %s\nSubject: %s\nDate: %s\n---\n",
                      m->sender, tolist, m->subject, date) < 0;
    if (!bad && m->body_len && fwrite(m->body, 1, m->body_len, f) != m->body_len)
        bad = 1;
    if (fclose(f) != 0)
        bad = 1;
    if (bad || rename(tmp, fin) != 0) {
        sm_log("spooler ERROR: cannot write %s: %s", fin, strerror(errno));
        unlink(tmp);
        return -1;
    }

    g_next_id[ui] = id + 1;
    persist_next_id(dir, g_next_id[ui]);
    sm_log("mail spooled to disk: ticket %llu -> %s", 
           (unsigned long long)m->ticket_id, fin);
    return 0;
}

static void process_message(const SMMailMessage *m)
{
    char date[32];
    char tolist[SM_MAX_RECIPIENTS * (SM_MAX_USERNAME + 2) + 1];
    int ok = 0;

    sm_timestamp(date, sizeof date);
    tolist[0] = '\0';
    for (uint32_t i = 0; i < m->recipient_count && i < SM_MAX_RECIPIENTS; i++) {
        if (i)
            strcat(tolist, ", ");
        strcat(tolist, m->recipients[i]);
    }
    for (uint32_t i = 0; i < m->recipient_count && i < SM_MAX_RECIPIENTS; i++) {
        int ui = user_index(m->recipients[i]);
        if (ui < 0) {
            sm_log("spooler: skipping unknown recipient '%s' (ticket %llu)",
                   m->recipients[i], (unsigned long long)m->ticket_id);
            continue;
        }
        if (deliver(m, ui, tolist, date) == 0)
            ok++;
    }
    sm_log("ticket %llu complete: %d/%u mailboxes written",
           (unsigned long long)m->ticket_id, ok, (unsigned)m->recipient_count);
}

/* ------------------------------------------------------------------ */
/* Shared memory                                                       */
/* ------------------------------------------------------------------ */

static void create_shm(void)
{
    size_t sz = sizeof(SMSPool);

    g_shmid = shmget(SM_SPOOL_KEY, sz, IPC_CREAT | IPC_EXCL | 0666);
    if (g_shmid < 0 && errno == EEXIST) {
        int old = shmget(SM_SPOOL_KEY, 0, 0666);
        if (old >= 0) {
            struct shmid_ds ds;
            if (shmctl(old, IPC_STAT, &ds) == 0 && ds.shm_nattch > 0) {
                fprintf(stderr, "Shared memory segment is in use (another smspooler/smserver running?)\n");
                exit(1);
            }
            shmctl(old, IPC_RMID, nullptr);
            sm_log("removed stale shared memory segment");
        }
        g_shmid = shmget(SM_SPOOL_KEY, sz, IPC_CREAT | IPC_EXCL | 0666);
    }
    if (g_shmid < 0) {
        perror("shmget");
        exit(1);
    }
    void *mem = shmat(g_shmid, nullptr, 0);
    if (mem == (void *)-1) {
        perror("shmat");
        shmctl(g_shmid, IPC_RMID, nullptr);
        exit(1);
    }
    g_spool = static_cast<SMSPool *>(mem);

    memset(g_spool, 0, sizeof *g_spool);
    if (sem_init(&g_spool->empty_slots, 1, SM_SPOOL_CAPACITY) != 0 ||
        sem_init(&g_spool->full_slots,  1, 0) != 0 ||
        sem_init(&g_spool->ring_mutex,  1, 1) != 0) {
        perror("sem_init");
        shmdt(g_spool);
        shmctl(g_shmid, IPC_RMID, nullptr);
        exit(1);
    }
    g_spool->head = g_spool->tail = 0;
    g_spool->next_ticket_id = 0;
    __sync_synchronize();
    g_spool->magic = SM_SPOOL_MAGIC;       /* published last: server checks this */
    g_spool->initialized = 1;
}

static void destroy_shm(void)
{
    if (!g_spool)
        return;
    sem_destroy(&g_spool->empty_slots);
    sem_destroy(&g_spool->full_slots);
    sem_destroy(&g_spool->ring_mutex);
    shmdt(g_spool);
    shmctl(g_shmid, IPC_RMID, nullptr);
    g_spool = nullptr;
}

/* Take one slot (caller already consumed full_slots) and spool it. */
static void consume_slot(void)
{
    while (sem_wait(&g_spool->ring_mutex) == -1 && errno == EINTR)
        ;
    uint32_t idx = g_spool->tail;
    g_spool->tail = (idx + 1) % SM_SPOOL_CAPACITY;
    sem_post(&g_spool->ring_mutex);

    /* The slot is ours until we post empty_slots; the single producer
     * cannot reuse it before then, so no copy/lock is needed here. */
    process_message(&g_spool->slots[idx]);
    sem_post(&g_spool->empty_slots);
}

/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <userfile>\n", argv[0]);
        return 1;
    }

    char err[256];
    g_nusers = sm_load_users(argv[1], g_users, SM_MAX_USERS, err, sizeof err);
    if (g_nusers < 0) {
        fprintf(stderr, "smspooler: %s\n", err);
        return 1;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;            /* no SA_RESTART: wake blocking calls */
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    init_mailboxes();
    create_shm();
    sm_log("smspooler started: %d users, ring capacity %d, shm id %d",
           g_nusers, SM_SPOOL_CAPACITY, g_shmid);

    while (!g_stop) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        if (sem_timedwait(&g_spool->full_slots, &ts) != 0) {
            if (errno == EINTR || errno == ETIMEDOUT)
                continue;
            perror("sem_timedwait");
            break;
        }
        consume_slot();
    }

    sm_log("smspooler shutting down, draining queue");
    while (sem_trywait(&g_spool->full_slots) == 0)
        consume_slot();

    destroy_shm();
    sm_log("smspooler stopped");
    return 0;
}
