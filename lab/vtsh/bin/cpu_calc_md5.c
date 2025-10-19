// cpu-calc-md5.c
// Compile: gcc -O0 cpu-calc-md5.c -lcrypto -o cpu-calc-md5
// (no extra optimization flags requested by the task -> compile without
// -O2/-O3)

#include <getopt.h>
#include <openssl/md5.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
  unsigned int seed;
  long repetitions;
  size_t total_bytes;
  size_t frag_size;
} worker_arg_t;

static inline void compute_md5(
    const unsigned char* buf, size_t len, unsigned char out[16]
) {
  MD5(buf, len, out);
}

void* worker(void* argp) {
  worker_arg_t arg = *(worker_arg_t*)argp;
  unsigned char* buf = malloc(arg.total_bytes);
  if (!buf) {
    perror("malloc");
    return NULL;
  }
  unsigned char md[16];
  for (long r = 0; r < arg.repetitions; ++r) {
    // fill buffer with concatenation of random fragments
    size_t off = 0;
    while (off < arg.total_bytes) {
      size_t chunk = arg.frag_size;
      if (off + chunk > arg.total_bytes)
        chunk = arg.total_bytes - off;
      // fill chunk pseudo-randomly using seed
      for (size_t i = 0; i < chunk; ++i) {
        arg.seed = arg.seed * 1103515245 + 12345;
        buf[off + i] = (unsigned char)((arg.seed >> 16) & 0xFF);
      }
      off += chunk;
    }
    compute_md5(buf, arg.total_bytes, md);
    // optional: touch md to avoid being optimized out (store first byte)
    if (md[0] == 0xFF) {
      // no-op branch extremely unlikely; keeps compiler honest
    }
  }
  free(buf);
  return NULL;
}

void usage(const char* pname) {
  fprintf(
      stderr,
      "cpu_calc_md5: MD5-based CPU load generator\n"
      "Usage: %s [options]\n"
      " -r N    repetitions per thread (default 100)\n"
      " -s B    total bytes to build and hash per repetition (default 10M)\n"
      " -f B    fragment size for concatenation (default 4096)\n"
      " -t T    number of threads (default 1)\n"
      " -h      help\n",
      pname
  );
}

int main(int argc, char** argv) {
  long repetitions = 100;
  size_t total_bytes = 10 * 1024 * 1024;  // 10 MB
  size_t frag_size = 4096;
  int threads = 1;
  int opt;
  while ((opt = getopt(argc, argv, "r:s:f:t:h")) != -1) {
    switch (opt) {
      case 'r':
        repetitions = atol(optarg);
        break;
      case 's':
        total_bytes = (size_t)atol(optarg);
        break;
      case 'f':
        frag_size = (size_t)atol(optarg);
        break;
      case 't':
        threads = atoi(optarg);
        break;
      case 'h':
      default:
        usage(argv[0]);
        return 1;
    }
  }
  if (threads < 1)
    threads = 1;
  pthread_t* ths = calloc(threads, sizeof(pthread_t));
  worker_arg_t* args = calloc(threads, sizeof(worker_arg_t));
  if (!ths || !args) {
    perror("calloc");
    return 2;
  }

  unsigned int seed_base = (unsigned int)time(NULL) ^ (unsigned int)getpid();
  for (int i = 0; i < threads; ++i) {
    args[i].seed = seed_base + i * 1234567;
    args[i].repetitions = repetitions;
    args[i].total_bytes = total_bytes;
    args[i].frag_size = frag_size;
    if (pthread_create(&ths[i], NULL, worker, &args[i]) != 0) {
      perror("pthread_create");
      return 3;
    }
  }
  for (int i = 0; i < threads; ++i) {
    pthread_join(ths[i], NULL);
  }
  free(ths);
  free(args);
  printf(
      "Done. threads=%d repetitions=%ld total_bytes=%zu frag_size=%zu\n",
      threads,
      repetitions,
      total_bytes,
      frag_size
  );
  return 0;
}
