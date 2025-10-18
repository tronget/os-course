// ioload_mt.c — многопоточный параметризуемый IO-генератор нагрузки
// macOS: fcntl(F_NOCACHE); Linux: O_DIRECT (если доступен)
// block_count — ОБЩЕЕ число I/O (распределяется по потокам)

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <sys/sysctl.h>
#define HAS_ARC4RANDOM 1
#define USE_F_NOCACHE 1
#else
#define HAS_ARC4RANDOM 0
#define USE_F_NOCACHE 0
#endif

#ifndef O_DIRECT
#define O_DIRECT 0
#endif

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

// Включите 1, чтобы печатать краткие пер-поточные строки
#define PRINT_PER_THREAD 1

// --------- Время ---------
static double now_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

// --------- RNG ---------
static uint64_t rng_u64_global(void) {
#if HAS_ARC4RANDOM
  return (((uint64_t)arc4random()) << 32) ^ (uint64_t)arc4random();
#else
  static __thread uint64_t x = 88172645463393265ULL;
  x ^= x << 7;
  x ^= x >> 9;
  return x;
#endif
}
static uint64_t rng_step(uint64_t* s) {
#if HAS_ARC4RANDOM
  (void)s;
  return (((uint64_t)arc4random()) << 32) ^ (uint64_t)arc4random();
#else
  uint64_t x = *s;
  x ^= x << 7;
  x ^= x >> 9;
  *s = x;
  return x;
#endif
}

// --------- Параметры ---------
typedef enum { RW_READ, RW_WRITE } rw_t;
typedef enum { PICK_SEQ, PICK_RAND } pick_t;

typedef struct {
  rw_t rw;                // read / write
  size_t block_size;      // bytes
  uint64_t block_count;   // ОБЩЕЕ число I/O (на все потоки)
  const char* file_path;  // path
  off_t range_lo;         // inclusive, bytes
  off_t range_hi;  // inclusive, bytes; 0-0 => весь файл (для чтения) или
                   // [0..bc*bs) для записи
  bool direct;  // bypass caches: macOS F_NOCACHE / Linux O_DIRECT
  pick_t pick;  // sequence / random
  int threads;  // кол-во потоков
} params_t;

static void usage(const char* prog) {
  fprintf(
      stderr,
      "Usage:\n"
      "  %s rw=<read|write> block_size=<bytes> block_count=<n> file=<path>\n"
      "     range=<start>-<end> direct=<on|off> type=<sequence|random> "
      "threads=<N>\n"
      "\n"
      "Notes:\n"
      "  - block_count — ОБЩЕЕ число I/O, делится между потоками.\n"
      "  - range=0-0: чтение по всему файлу; запись — "
      "[0..block_count*block_size).\n"
      "  - direct=on: macOS -> fcntl(F_NOCACHE,1); Linux -> O_DIRECT (если "
      "доступен).\n"
      "  - type=sequence: потоки равномерно чередуются по блокам; random — "
      "блок выбирается случайно.\n"
      "\n",
      prog
  );
}

static int parse_range(const char* s, off_t* lo, off_t* hi) {
  const char* dash = strchr(s, '-');
  if (!dash)
    return -1;
  char* endp = NULL;
  errno = 0;
  long long a = strtoll(s, &endp, 10);
  if (errno != 0 || endp != dash)
    return -1;
  errno = 0;
  long long b = strtoll(dash + 1, &endp, 10);
  if (errno != 0 || *endp != '\0')
    return -1;
  if (a < 0 || b < 0)
    return -1;
  *lo = (off_t)a;
  *hi = (off_t)b;
  return 0;
}

static int parse_kv_arg(const char* arg, char** key, char** val) {
  const char* eq = strchr(arg, '=');
  if (!eq)
    return -1;
  *key = strndup(arg, (size_t)(eq - arg));
  *val = strdup(eq + 1);
  return (*key && *val) ? 0 : -1;
}

static int parse_args(int argc, char** argv, params_t* p) {
  if (argc < 2) {
    usage(argv[0]);
    return -1;
  }
  memset(p, 0, sizeof(*p));
  p->rw = RW_READ;
  p->block_size = 4096;
  p->block_count = 0;
  p->file_path = NULL;
  p->range_lo = 0;
  p->range_hi = 0;
  p->direct = false;
  p->pick = PICK_SEQ;
  p->threads = 1;

  for (int i = 1; i < argc; ++i) {
    char *key = NULL, *val = NULL;
    if (parse_kv_arg(argv[i], &key, &val) != 0) {
      fprintf(stderr, "Bad arg: %s\n", argv[i]);
      usage(argv[0]);
      return -1;
    }
    if (strcmp(key, "rw") == 0) {
      if (strcmp(val, "read") == 0)
        p->rw = RW_READ;
      else if (strcmp(val, "write") == 0)
        p->rw = RW_WRITE;
      else {
        fprintf(stderr, "rw must be read|write\n");
        return -1;
      }
    } else if (strcmp(key, "block_size") == 0) {
      char* endp = NULL;
      errno = 0;
      long long bs = strtoll(val, &endp, 10);
      if (errno || *endp != '\0' || bs <= 0) {
        fprintf(stderr, "block_size invalid\n");
        return -1;
      }
      p->block_size = (size_t)bs;
    } else if (strcmp(key, "block_count") == 0) {
      char* endp = NULL;
      errno = 0;
      long long bc = strtoll(val, &endp, 10);
      if (errno || *endp != '\0' || bc <= 0) {
        fprintf(stderr, "block_count invalid\n");
        return -1;
      }
      p->block_count = (uint64_t)bc;
    } else if (strcmp(key, "file") == 0) {
      p->file_path = val;
      val = NULL;
    } else if (strcmp(key, "range") == 0) {
      if (parse_range(val, &p->range_lo, &p->range_hi) != 0) {
        fprintf(stderr, "range invalid, expected A-B\n");
        return -1;
      }
    } else if (strcmp(key, "direct") == 0) {
      if (strcmp(val, "on") == 0)
        p->direct = true;
      else if (strcmp(val, "off") == 0)
        p->direct = false;
      else {
        fprintf(stderr, "direct must be on|off\n");
        return -1;
      }
    } else if (strcmp(key, "type") == 0) {
      if (strcmp(val, "sequence") == 0)
        p->pick = PICK_SEQ;
      else if (strcmp(val, "random") == 0)
        p->pick = PICK_RAND;
      else {
        fprintf(stderr, "type must be sequence|random\n");
        return -1;
      }
    } else if (strcmp(key, "threads") == 0) {
      char* endp = NULL;
      errno = 0;
      long long th = strtoll(val, &endp, 10);
      if (errno || *endp != '\0' || th <= 0 || th > 1024) {
        fprintf(stderr, "threads invalid (1..1024)\n");
        return -1;
      }
      p->threads = (int)th;
    } else {
      fprintf(stderr, "Unknown key: %s\n", key);
      return -1;
    }
    free(key);
    free(val);
  }

  if (!p->file_path) {
    fprintf(stderr, "file=<path> is required\n");
    return -1;
  }
  if (p->block_count == 0) {
    fprintf(stderr, "block_count must be > 0\n");
    return -1;
  }
  if (p->block_size == 0) {
    fprintf(stderr, "block_size must be > 0\n");
    return -1;
  }
  if (p->threads <= 0) {
    fprintf(stderr, "threads must be > 0\n");
    return -1;
  }

  return 0;
}

// --------- FS/IO helpers ---------
static off_t file_size_or_zero(int fd) {
  struct stat st;
  if (fstat(fd, &st) == 0)
    return st.st_size;
  return 0;
}

static void* alloc_aligned(size_t size, size_t align) {
  void* p = NULL;
  if (posix_memalign(&p, (align < 4096 ? 4096 : align), size) != 0)
    return NULL;
  return p;
}

static void fill_pattern(uint8_t* buf, size_t n, uint64_t seed) {
  uint64_t x = seed ^ 0x9E3779B97F4A7C15ULL;
  for (size_t i = 0; i < n; ++i) {
    x ^= x << 7;
    x ^= x >> 9;
    buf[i] = (uint8_t)(x & 0xFFu);
  }
}

static int set_direct_mode_mac(int fd, bool on) {
#if USE_F_NOCACHE
  if (fcntl(fd, F_NOCACHE, on ? 1 : 0) == -1) {
    perror("fcntl(F_NOCACHE)");
    return -1;
  }
  (void)fcntl(fd, F_RDAHEAD, on ? 0 : 1);
#else
  (void)fd;
  (void)on;
#endif
  return 0;
}

static int open_target(const params_t* p, int* out_fd, bool* used_direct_flag) {
  int flags = (p->rw == RW_READ) ? O_RDONLY : (O_WRONLY | O_CREAT);
#if !USE_F_NOCACHE
  if (p->direct && O_DIRECT) {
    flags |= O_DIRECT;
    *used_direct_flag = true;
  }
#endif
  int fd = open(p->file_path, flags, 0666);
  if (fd < 0 && p->rw == RW_WRITE && errno == ENOENT) {
    fd = open(p->file_path, O_WRONLY | O_CREAT, 0666);
  }
  if (fd < 0) {
    perror("open");
    return -1;
  }
#if USE_F_NOCACHE
  if (p->direct) {
    if (set_direct_mode_mac(fd, true) != 0) {
      close(fd);
      return -1;
    }
    *used_direct_flag = true;
  }
#endif
  return *out_fd = fd, 0;
}

// --------- Потоки ---------
typedef struct {
  const params_t* p;
  int fd;
  int tid;  // 0..threads-1
  size_t bs;
  off_t lo, hi;
  uint64_t region_blocks;
  uint64_t ios_assigned;  // сколько I/O должен сделать поток
  pick_t pick;
  rw_t rw;
  // результаты:
  uint64_t done_ios;
  uint64_t bytes_total;
  int err_no;
  off_t err_off;
  double elapsed_ms;
} thread_ctx_t;

static void* worker(void* arg) {
  thread_ctx_t* t = (thread_ctx_t*)arg;
  const size_t bs = t->bs;
  uint8_t* buf = (uint8_t*)alloc_aligned(bs, 4096);
  if (!buf) {
    t->err_no = ENOMEM;
    return NULL;
  }
  if (t->rw == RW_WRITE)
    fill_pattern(buf, bs, 0xC0FFEEULL ^ (uint64_t)t->tid);

  uint64_t seed =
      (rng_u64_global() ^ (0xA5A5A5A5ULL * (uint64_t)t->tid)) | 1ULL;

  double t0 = now_ms();
  uint64_t done = 0, bytes = 0;

  for (uint64_t i = 0; i < t->ios_assigned; ++i) {
    uint64_t blk_index;
    if (t->pick == PICK_SEQ) {
      // Равномерное чередование блоков между потоками:
      // общий индекс = i*threads + tid
      uint64_t g = i * (uint64_t)t->p->threads + (uint64_t)t->tid;
      blk_index = g % t->region_blocks;
    } else {
      uint64_t r = rng_step(&seed);
      // Масштабируем к региону
      blk_index = (uint64_t)(r % t->region_blocks);
    }
    off_t off = t->lo + (off_t)(blk_index * bs);

    ssize_t n;
    if (t->rw == RW_WRITE) {
      // добавить небольшую вариативность данных
      fill_pattern(buf, bs, 0xC0FFEEULL ^ (uint64_t)t->tid ^ i);
      n = pwrite(t->fd, buf, bs, off);
    } else {
      n = pread(t->fd, buf, bs, off);
    }

    if (unlikely(n < 0)) {
      t->err_no = errno;
      t->err_off = off;
      break;
    }
    if (unlikely((size_t)n != bs)) {
      t->err_no = EIO;
      t->err_off = off;
      break;
    }

    bytes += (uint64_t)bs;
    done++;
  }

  double t1 = now_ms();
  t->done_ios = done;
  t->bytes_total = bytes;
  t->elapsed_ms = t1 - t0;

#if PRINT_PER_THREAD
  fprintf(
      stdout,
      "[thread %d] ios=%" PRIu64 ", bytes=%" PRIu64
      " (%.2f MiB), time=%.3f ms, iops=%.2f, thr=%.2f MiB/s%s\n",
      t->tid,
      t->done_ios,
      t->bytes_total,
      (double)t->bytes_total / (1024.0 * 1024.0),
      t->elapsed_ms,
      (t->elapsed_ms > 0.0) ? ((double)t->done_ios / (t->elapsed_ms / 1000.0))
                            : 0.0,
      (t->elapsed_ms > 0.0) ? (((double)t->bytes_total / (1024.0 * 1024.0)) /
                               (t->elapsed_ms / 1000.0))
                            : 0.0,
      (t->err_no ? " [ERROR]" : "")
  );
  fflush(stdout);
#endif

  free(buf);
  return NULL;
}

static int do_ioload_mt(const params_t* p) {
  int fd = -1;
  bool used_direct_flag = false;
  if (open_target(p, &fd, &used_direct_flag) != 0)
    return -1;

  const size_t bs = p->block_size;
  if (bs > (size_t)SSIZE_MAX) {
    fprintf(stderr, "block_size too large for this platform\n");
    close(fd);
    return -1;
  }

  // Диапазон
  off_t file_sz = file_size_or_zero(fd);
  off_t lo = p->range_lo;
  off_t hi = p->range_hi;

  if (lo < 0)
    lo = 0;

  if (lo == 0 && hi == 0) {
    if (p->rw == RW_READ) {
      hi = (file_sz > 0) ? (file_sz - 1) : 0;
    } else {
      off_t end = (off_t)p->block_count * (off_t)bs;
      hi = (end > 0) ? (end - 1) : 0;
    }
  }
  if ((hi >= 0) && (hi < lo)) {
    fprintf(stderr, "range end < start\n");
    close(fd);
    return -1;
  }

  // Выравниваем границы под размер блока
  lo = (lo / (off_t)bs) * (off_t)bs;
  if (hi > 0)
    hi = ((hi + 1) / (off_t)bs) * (off_t)bs - 1;

  if (p->rw == RW_READ) {
    if (file_sz == 0) {
      fprintf(stderr, "file is empty or size is 0 — nothing to read\n");
      close(fd);
      return -1;
    }
    if (lo >= file_sz) {
      fprintf(stderr, "range start >= file size\n");
      close(fd);
      return -1;
    }
    if (hi <= 0 || hi >= file_sz) {
      hi = file_sz - 1;
    }
  }

  const uint64_t region_blocks = (uint64_t)((hi - lo + 1) / (off_t)bs);
  if (region_blocks == 0) {
    fprintf(stderr, "range too small for given block_size\n");
    close(fd);
    return -1;
  }

  // Распределение I/O по потокам
  int T = p->threads;
  pthread_t* ths = (pthread_t*)calloc((size_t)T, sizeof(pthread_t));
  thread_ctx_t* ctx = (thread_ctx_t*)calloc((size_t)T, sizeof(thread_ctx_t));
  if (!ths || !ctx) {
    fprintf(stderr, "alloc threads failed\n");
    free(ths);
    free(ctx);
    close(fd);
    return -1;
  }

  uint64_t base = p->block_count / (uint64_t)T;
  uint64_t rem = p->block_count % (uint64_t)T;

  // Старт
  double T0 = now_ms();

  for (int t = 0; t < T; ++t) {
    ctx[t].p = p;
    ctx[t].fd = fd;
    ctx[t].tid = t;
    ctx[t].bs = bs;
    ctx[t].lo = lo;
    ctx[t].hi = hi;
    ctx[t].region_blocks = region_blocks;
    ctx[t].pick = p->pick;
    ctx[t].rw = p->rw;
    ctx[t].done_ios = 0;
    ctx[t].bytes_total = 0;
    ctx[t].err_no = 0;
    ctx[t].err_off = -1;
    ctx[t].elapsed_ms = 0.0;
    ctx[t].ios_assigned = base + ((uint64_t)t < rem ? 1ULL : 0ULL);

    int rc = pthread_create(&ths[t], NULL, worker, &ctx[t]);
    if (rc != 0) {
      fprintf(stderr, "pthread_create(%d) failed: %s\n", t, strerror(rc));
      // Попробуем дождаться уже созданные
      for (int j = 0; j < t; ++j)
        pthread_join(ths[j], NULL);
      free(ths);
      free(ctx);
      close(fd);
      return -1;
    }
  }

  // Join
  for (int t = 0; t < T; ++t)
    pthread_join(ths[t], NULL);

  // Для записи — принудительный сброс
  if (p->rw == RW_WRITE) {
    if (fsync(fd) != 0)
      perror("fsync");
  }

#if USE_F_NOCACHE
  if (p->direct)
    (void)set_direct_mode_mac(fd, false);
#endif

  double T1 = now_ms();
  double ms = T1 - T0;
  double sec = ms / 1000.0;

  // Агрегация результатов
  uint64_t agg_ios = 0, agg_bytes = 0;
  int any_err = 0;
  int err_no = 0;
  off_t err_off = -1;
  for (int t = 0; t < T; ++t) {
    agg_ios += ctx[t].done_ios;
    agg_bytes += ctx[t].bytes_total;
    if (!any_err && ctx[t].err_no) {
      any_err = 1;
      err_no = ctx[t].err_no;
      err_off = ctx[t].err_off;
    }
  }

  double iops = (sec > 0.0) ? ((double)agg_ios / sec) : 0.0;
  double mbps =
      (sec > 0.0) ? ((double)agg_bytes / (1024.0 * 1024.0) / sec) : 0.0;

  printf(
      "ioload_mt summary:\n"
      "  mode          : %s\n"
      "  direct        : %s (%s)\n"
      "  type          : %s\n"
      "  threads       : %d\n"
      "  file          : %s\n"
      "  range         : %lld-%lld (bytes)\n"
      "  block_size    : %zu bytes\n"
      "  block_count   : %" PRIu64
      " (total across all threads)\n"
      "  bytes_total   : %" PRIu64
      " (%.2f MiB)\n"
      "  duration      : %.3f ms\n"
      "  IOPS          : %.2f\n"
      "  throughput    : %.2f MiB/s\n",
      (p->rw == RW_WRITE ? "write" : "read"),
      (p->direct ? "on" : "off"),
      (used_direct_flag
           ? (USE_F_NOCACHE ? "F_NOCACHE" : (O_DIRECT ? "O_DIRECT" : "N/A"))
           : "pagecache"),
      (p->pick == PICK_SEQ ? "sequence" : "random"),
      p->threads,
      p->file_path,
      (long long)lo,
      (long long)hi,
      p->block_size,
      p->block_count,
      agg_bytes,
      (double)agg_bytes / (1024.0 * 1024.0),
      ms,
      iops,
      mbps
  );

  if (any_err) {
    fprintf(
        stderr,
        "error: errno=%d (%s) at off=%lld\n",
        err_no,
        strerror(err_no),
        (long long)err_off
    );
  }

  free(ths);
  free(ctx);
  close(fd);
  return any_err ? -1 : 0;
}

int main(int argc, char** argv) {
  signal(SIGPIPE, SIG_IGN);

  params_t p;
  if (parse_args(argc, argv, &p) != 0)
    return 2;

  int rc = do_ioload_mt(&p);
  return (rc == 0) ? 0 : 1;
}