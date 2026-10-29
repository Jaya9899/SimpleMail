#ifndef SMCOMMON_H
#define SMCOMMON_H

#include <stdint.h>
#include <semaphore.h>

#define SM_SPOOL_KEY      0x534D31
#define SM_SPOOL_CAPACITY 16
#define SM_MAX_BODY       65536
#define SM_MAX_USERNAME   20
#define SM_MAX_PASSWORD   30
#define SM_MAX_RECIPIENTS 100
#define SM_INITIAL_WINDOW 2048

typedef struct {
    char username[SM_MAX_USERNAME + 1];
    char password[SM_MAX_PASSWORD + 1];
} SMUser;

typedef struct {
    char sender[128];
    uint32_t recipient_count;
    char recipients[SM_MAX_RECIPIENTS][SM_MAX_USERNAME + 1];
    char subject[256];
    uint32_t body_len;
    unsigned char body[SM_MAX_BODY];
    uint64_t ticket_id;
} SMMailMessage;

typedef struct {
    uint32_t magic;
    uint32_t initialized;
    uint32_t head;
    uint32_t tail;
    uint64_t next_ticket_id;

    sem_t empty_slots;
    sem_t full_slots;
    sem_t ring_mutex;

    SMMailMessage slots[SM_SPOOL_CAPACITY];
} SMSPool;

uint64_t sm_djb2(const char *text);

#endif
