#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdint.h>
#include <string.h>

static inline int popcount_u64(uint64_t x) {
#if defined(__POPCNT__) || defined(__x86_64__)
    return __builtin_popcountll(x);
#else
    /* portable fallback */
    x = x - ((x >> 1) & 0x5555555555555555ULL);
    x = (x & 0x3333333333333333ULL) + ((x >> 2) & 0x3333333333333333ULL);
    return (int)((((x + (x >> 4)) & 0x0F0F0F0F0F0F0F0FULL) * 0x0101010101010101ULL) >> 56);
#endif
}

static int hamming_distance_blob(const unsigned char *a, int alen,
                                 const unsigned char *b, int blen)
{
    /* 优先处理长度等于 8 的 fast path */
    if (alen == 8 && blen == 8) {
        uint64_t x, y;
        memcpy(&x, a, 8);
        memcpy(&y, b, 8);
        return popcount_u64(x ^ y);
    }

    /* fallback: 任意长度 */
    int len = alen < blen ? alen : blen;
    int diff = 0;

    /* 逐字节计算 */
    for (int i = 0; i < len; i++) {
        diff += __builtin_popcount((unsigned)a[i] ^ (unsigned)b[i]);
    }

    /* 处理长度不同的额外尾部 */
    if (alen > len) {
        for (int i = len; i < alen; i++)
            diff += __builtin_popcount((unsigned)a[i]);
    } else if (blen > len) {
        for (int i = len; i < blen; i++)
            diff += __builtin_popcount((unsigned)b[i]);
    }

    return diff;
}

static void hamming_distance_sql(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc != 2) {
        sqlite3_result_null(ctx);
        return;
    }

    const unsigned char *a = sqlite3_value_blob(argv[0]);
    const unsigned char *b = sqlite3_value_blob(argv[1]);
    int alen = sqlite3_value_bytes(argv[0]);
    int blen = sqlite3_value_bytes(argv[1]);

    if (!a || !b) {
        sqlite3_result_null(ctx);
        return;
    }

    int dist = hamming_distance_blob(a, alen, b, blen);
    sqlite3_result_int(ctx, dist);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_hammdist_init(sqlite3 *db, char **pzErrMsg,
                          const sqlite3_api_routines *pApi)
{
    SQLITE_EXTENSION_INIT2(pApi);
    return sqlite3_create_function(
        db, "hamming_distance", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        NULL, hamming_distance_sql,
        NULL, NULL
    );
}
