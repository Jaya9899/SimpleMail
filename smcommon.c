/* smcommon.c - shared helpers: DJB2, logging, user registry */
#define _GNU_SOURCE
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "smcommon.h"
#include "smutil.h"

uint64_t sm_djb2(const char *text)
{
    uint64_t hash = 5381;
    int c;
    while ((c = *text++))
        hash = ((hash << 5) + hash) + c;   /* hash * 33 + c */
    return hash;
}

void sm_timestamp(char *buf, size_t len)
{
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(buf, len, "%Y-%m-%d %H:%M:%S", &tmv);
}

void sm_log(const char *fmt, ...)
{
    char ts[32];
    va_list ap;
    sm_timestamp(ts, sizeof ts);
    printf("[%s] ", ts);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
}

void sm_lowercase(char *s)
{
    for (; *s; s++)
        *s = (char)tolower((unsigned char)*s);
}

const SMUser *sm_find_user(const SMUser *users, int n, const char *name)
{
    for (int i = 0; i < n; i++)
        if (strcmp(users[i].username, name) == 0)
            return &users[i];
    return NULL;
}

#define BAD(...) do { snprintf(err, errlen, __VA_ARGS__); goto fail; } while (0)

int sm_load_users(const char *path, SMUser *users, int max_users,
                  char *err, size_t errlen)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        snprintf(err, errlen, "cannot open user file '%s'", path);
        return -1;
    }

    char line[256];
    int n = 0, lineno = 0;

    while (fgets(line, sizeof line, f)) {
        lineno++;
        size_t l = strlen(line);
        if (l == sizeof line - 1 && line[l - 1] != '\n' && !feof(f))
            BAD("line %d: too long", lineno);
        while (l > 0 && strchr("\r\n \t", line[l - 1]))
            line[--l] = '\0';
        if (l == 0)
            continue;                         /* blank line */

        char *sp = strchr(line, ' ');
        if (!sp)
            BAD("line %d: expected '<username> <password>'", lineno);
        *sp = '\0';
        char *user = line, *pass = sp + 1;
        if (strpbrk(pass, " \t"))
            BAD("line %d: expected exactly two fields", lineno);

        sm_lowercase(user);
        size_t ul = strlen(user), pl = strlen(pass);
        if (ul == 0 || ul > SM_MAX_USERNAME)
            BAD("line %d: username must be 1-%d characters", lineno, SM_MAX_USERNAME);
        for (size_t i = 0; i < ul; i++)
            if (user[i] < 'a' || user[i] > 'z')
                BAD("line %d: username must be letters a-z only", lineno);
        if (pl == 0 || pl > SM_MAX_PASSWORD)
            BAD("line %d: password must be 1-%d characters", lineno, SM_MAX_PASSWORD);
        for (size_t i = 0; i < pl; i++)
            if (!isalnum((unsigned char)pass[i]))
                BAD("line %d: password must be alphanumeric", lineno);
        for (int j = 0; j < n; j++)
            if (strcmp(users[j].username, user) == 0)
                BAD("line %d: duplicate user '%s'", lineno, user);
        if (n >= max_users)
            BAD("too many users (max %d)", max_users);

        strcpy(users[n].username, user);
        strcpy(users[n].password, pass);
        n++;
    }
    fclose(f);
    if (n == 0) {
        snprintf(err, errlen, "no users found in '%s'", path);
        return -1;
    }
    return n;

fail:
    fclose(f);
    return -1;
}
