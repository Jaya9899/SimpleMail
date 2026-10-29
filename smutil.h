#ifndef SMUTIL_H
#define SMUTIL_H

/* Backend-only helpers (Person 1). smcommon.h is left untouched. */

#include <stddef.h>
#include <stdint.h>
#include "smcommon.h"

#define SM_SPOOL_MAGIC  0x534D5350u
#define SM_MAX_USERS    256
#define SM_MAILBOX_DIR  "mailboxes"
#define SM_LINE_MAX     512      /* max SMTP2/SMP command line incl. terminator */

void sm_timestamp(char *buf, size_t len);                 /* "YYYY-MM-DD HH:MM:SS" */
void sm_log(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void sm_lowercase(char *s);

/* Returns number of users loaded, or -1 and fills err. */
int  sm_load_users(const char *path, SMUser *users, int max_users,
                   char *err, size_t errlen);
const SMUser *sm_find_user(const SMUser *users, int n, const char *name);

#endif
