#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
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

#define HAS_ARC4RANDOM 0
#define USE_F_NOCACHE 0

// --------- Утилиты времени ---------
static double now_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

// --------- Случайные числа ---------
static uint64_t rng_u64(void) {
  static uint64_t x = 88172645463393265ULL;
  x ^= x << 7;
  x ^= x >> 9;
  return x;
}

// --------- Параметры ---------
typedef enum { RW_READ, RW_WRITE } rw_t;
typedef enum { PICK_SEQ, PICK_RAND } pick_t;

typedef struct {
  rw_t rw;                // read / write
  size_t block_size;      // bytes
  uint64_t block_count;   // number of IOs
  const char* file_path;  // path
  off_t range_lo;         // inclusive, bytes
  off_t range_hi;         // inclusive, bytes; 0-0 => весь файл
  bool direct;            // bypass caches: macOS F_NOCACHE / Linux O_DIRECT
  pick_t pick;            // sequence / random
} params_t;

static void usage(const char* prog) {
  fprintf(
      stderr,
      "Usage:\n"
      "  %s rw=<read|write> block_size=<bytes> block_count=<n> file=<path>\n"
      "     range=<start>-<end> direct=<on|off> type=<sequence|random>\n"
      "\n"
      "Notes:\n"
      "  - range=0-0 означает \"весь файл\" для чтения.\n"
      "    Для записи, если range=0-0, диапазон берётся как [0 .. "
      "block_count*block_size).\n"
      "  - direct=on: macOS -> fcntl(F_NOCACHE,1), Linux -> O_DIRECT (если "
      "доступен).\n"
      "  - type=sequence: блоки идут подряд; type=random: блок выбирается "
      "случайно в диапазоне.\n"
      "\n",
      prog
  );
}

static int parse_range(const char* s, off_t* lo, off_t* hi) {
  // Формат: "<number>-<number>"
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
  // Значения по умолчанию
  memset(p, 0, sizeof(*p));
  p->rw = RW_READ;
  p->block_size = 4096;
  p->block_count = 0;
  p->file_path = NULL;
  p->range_lo = 0;
  p->range_hi = 0;
  p->direct = false;
  p->pick = PICK_SEQ;

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
      val = NULL;  // владение переносим
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

  return 0;
}

// Получить размер файла; если файла нет — для записи вернём 0, для чтения
// ругнёмся позже
static off_t file_size_or_zero(int fd) {
  struct stat st;
  if (fstat(fd, &st) == 0)
    return st.st_size;
  return 0;
}

// Выравнивание — полезно для O_DIRECT на Linux (и в целом не мешает)
static void* alloc_aligned(size_t size, size_t align) {
  void* p = NULL;
  if (posix_memalign(&p, (align < 4096 ? 4096 : align), size) != 0)
    return NULL;
  return p;
}

// Заполнить буфер детерминированным паттерном
static void fill_pattern(uint8_t* buf, size_t n, uint64_t seed) {
  uint64_t x = seed ^ 0x9E3779B97F4A7C15ULL;
  for (size_t i = 0; i < n; ++i) {
    x ^= x << 7;
    x ^= x >> 9;  // простейший PRNG
    buf[i] = (uint8_t)(x & 0xFFu);
  }
}

static int set_direct_mode_mac(int fd, bool on) {
#if USE_F_NOCACHE
  if (fcntl(fd, F_NOCACHE, on ? 1 : 0) == -1) {
    perror("fcntl(F_NOCACHE)");
    return -1;
  }
  // Полезно ещё отключить readahead при чтении (не обязательно):
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
    // попытка создать файл (если вдруг O_CREAT не помог из-за комбинации
    // флагов)
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

static int do_ioload(const params_t* p) {
  int fd = -1;
  bool used_direct_flag = false;
  if (open_target(p, &fd, &used_direct_flag) != 0)
    return -1;

  const size_t bs = p->block_size;
  if (bs > (size_t)SIZE_MAX) {
    fprintf(stderr, "block_size too large for this platform\n");
    close(fd);
    return -1;
  }

  // Буфер (выровненный — на всякий случай)
  uint8_t* buf = (uint8_t*)alloc_aligned(bs, 4096);
  if (!buf) {
    fprintf(stderr, "posix_memalign failed\n");
    close(fd);
    return -1;
  }
  if (p->rw == RW_WRITE)
    fill_pattern(buf, bs, 0xC0FFEEULL);

  // Определяем рабочий диапазон
  off_t file_sz = file_size_or_zero(fd);
  off_t lo = p->range_lo;
  off_t hi = p->range_hi;

  if (lo < 0)
    lo = 0;

  if (lo == 0 && hi == 0) {
    if (p->rw == RW_READ) {
      // Весь файл
      hi = (file_sz > 0) ? (file_sz - 1) : 0;
    } else {
      // Для записи, если диапазон не задан — позволяем писать в [0 ..
      // block_count*block_size) чтобы не зависеть от текущего размера файла.
      off_t end = (off_t)p->block_count * (off_t)bs;
      hi = (end > 0) ? (end - 1) : 0;
    }
  }
  if (hi >= 0 && hi < lo) {
    fprintf(stderr, "range end < start\n");
    free(buf);
    close(fd);
    return -1;
  }

  // Округлим вниз границы к размеру блока
  lo = (lo / (off_t)bs) * (off_t)bs;
  if (hi > 0)
    hi = ((hi + 1) / (off_t)bs) * (off_t)bs -
         1;  // последняя байтовая позиция внутри кратного блока

  // Проверки для чтения
  if (p->rw == RW_READ) {
    if (file_sz == 0) {
      fprintf(stderr, "file is empty or size is 0 — nothing to read\n");
      free(buf);
      close(fd);
      return -1;
    }
    if (lo >= file_sz) {
      fprintf(stderr, "range start >= file size\n");
      free(buf);
      close(fd);
      return -1;
    }
    if (hi <= 0 || hi >= file_sz) {
      hi = file_sz - 1;
    }
  }

  const uint64_t total_ios = p->block_count;
  const uint64_t region_blocks = (uint64_t)((hi - lo + 1) / (off_t)bs);
  if (region_blocks == 0) {
    fprintf(stderr, "range too small for given block_size\n");
    free(buf);
    close(fd);
    return -1;
  }

  // Основной цикл
  double t0 = now_ms();
  uint64_t done = 0;
  uint64_t bytes_total = 0;

  for (uint64_t i = 0; i < total_ios; ++i) {
    uint64_t blk_index;
    if (p->pick == PICK_SEQ) {
      blk_index = i % region_blocks;
    } else {
      blk_index = rng_u64() % region_blocks;
    }
    off_t off = lo + (off_t)(blk_index * bs);

    ssize_t n;
    if (p->rw == RW_WRITE) {
      // Обновим паттерн, чтобы были разные данные
      fill_pattern(buf, bs, 0xC0FFEEULL ^ i);
      n = pwrite(fd, buf, bs, off);
    } else {
      n = pread(fd, buf, bs, off);
    }

    if (n < 0) {
      // На Linux с O_DIRECT часто нужна выравненность offset и буфера — мы её
      // обеспечили. Если всё же ошибка — покажем и завершимся.
      fprintf(
          stderr,
          "%s at off=%lld, errno=%d (%s)\n",
          (p->rw == RW_WRITE ? "pwrite" : "pread"),
          (long long)off,
          errno,
          strerror(errno)
      );
      free(buf);
      close(fd);
      return -1;
    }
    if ((size_t)n != bs) {
      // Для чтения в конце файла это возможно, но мы выравнивали диапазон — не
      // ожидаем коротких I/O
      fprintf(
          stderr,
          "short %s: got %zd, expected %zu at off=%lld\n",
          (p->rw == RW_WRITE ? "write" : "read"),
          n,
          bs,
          (long long)off
      );
      free(buf);
      close(fd);
      return -1;
    }

    bytes_total += (uint64_t)bs;
    done++;
  }

  // Для записи — сбросить на диск (по желанию пользователя можно сделать
  // отдельно)
  if (p->rw == RW_WRITE) {
    // На macOS fsync достаточно; на Linux с O_DIRECT операции и так минуют
    // pagecache, но метаданные могут кэшироваться.
    if (fsync(fd) != 0) {
      perror("fsync");
    }
  }

#if USE_F_NOCACHE
  // Вернуть стандартное кэширование (не обязательно)
  if (p->direct)
    (void)set_direct_mode_mac(fd, false);
#endif

  double t1 = now_ms();
  double ms = t1 - t0;
  double sec = ms / 1000.0;
  double iops = (sec > 0.0) ? ((double)done / sec) : 0.0;
  double mbps =
      (sec > 0.0) ? ((double)bytes_total / (1024.0 * 1024.0) / sec) : 0.0;

  printf(
      "ioload summary:\n"
      "  mode          : %s\n"
      "  direct        : %s (%s)\n"
      "  type          : %s\n"
      "  file          : %s\n"
      "  range         : %lld-%lld (bytes)\n"
      "  block_size    : %zu bytes\n"
      "  block_count   : %" PRIu64
      "\n"
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
      p->file_path,
      (long long)lo,
      (long long)hi,
      p->block_size,
      p->block_count,
      bytes_total,
      (double)bytes_total / (1024.0 * 1024.0),
      ms,
      iops,
      mbps
  );

  free(buf);
  close(fd);
  return 0;
}

int main(int argc, char** argv) {
  // Нормальная работа с SIGINT (Ctrl+C) — просто прерываем цикл/процесс.
  signal(SIGPIPE, SIG_IGN);

  params_t p;
  if (parse_args(argc, argv, &p) != 0) {
    return 2;
  }
  int rc = do_ioload(&p);
  return (rc == 0) ? 0 : 1;
}