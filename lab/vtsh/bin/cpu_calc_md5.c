// #include <errno.h>
// #include <inttypes.h>
// #include <stdbool.h>
// #include <stddef.h>
// #include <stdint.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>

// struct options {
//   size_t repeat;
//   size_t array_size;
//   uint32_t max_value;
//   uint64_t seed;
//   const char* algorithm;
// };

// static void usage(const char* prog) {
//   fprintf(
//       stderr,
//       "Usage: %s --repeat=<count> --size=<elements> [--max-value=<value>] "
//       "[--seed=<number>] [--algorithm=<hash|naive>]\n",
//       prog
//   );
// }

// static bool parse_size_t(const char* arg, size_t* out) {
//   char* end = NULL;
//   errno = 0;
//   unsigned long long val = strtoull(arg, &end, 10);
//   if (errno != 0 || end == arg || *end != '\0') {
//     return false;
//   }
//   *out = (size_t)val;
//   return true;
// }

// static bool parse_uint32(const char* arg, uint32_t* out) {
//   char* end = NULL;
//   errno = 0;
//   unsigned long val = strtoul(arg, &end, 10);
//   if (errno != 0 || end == arg || *end != '\0') {
//     return false;
//   }
//   if (val > UINT32_MAX) {
//     return false;
//   }
//   *out = (uint32_t)val;
//   return true;
// }

// static uint64_t mix_seed(uint64_t seed) {
//   uint64_t z = seed + UINT64_C(0x9E3779B97F4A7C15);
//   z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
//   z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
//   return z ^ (z >> 31);
// }

// static uint32_t rng_next(uint64_t* state) {
//   *state += UINT64_C(0x9E3779B97F4A7C15);
//   uint64_t z = *state;
//   z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
//   z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
//   z = z ^ (z >> 31);
//   return (uint32_t)z;
// }

// static size_t next_pow2(size_t value) {
//   if (value == 0) {
//     return 1;
//   }
//   value--;
//   for (size_t shift = 1; shift < sizeof(size_t) * 8; shift <<= 1) {
//     value |= value >> shift;
//   }
//   return value + 1;
// }

// static size_t dedup_hash(uint32_t* data, size_t n) {
//   size_t table_size = next_pow2(n * 2);
//   uint32_t* table = malloc(table_size * sizeof(uint32_t));
//   uint8_t* used = calloc(table_size, sizeof(uint8_t));
//   if (!table || !used) {
//     free(table);
//     free(used);
//     fprintf(stderr, "Allocation failure in dedup_hash.\n");
//     exit(EXIT_FAILURE);
//   }

//   size_t unique = 0;
//   for (size_t i = 0; i < n; ++i) {
//     uint32_t value = data[i];
//     size_t mask = table_size - 1;
//     size_t idx = ((uint64_t)value * UINT64_C(11400714819323198485)) & mask;
//     while (used[idx]) {
//       if (table[idx] == value) {
//         goto skip_insert;
//       }
//       idx = (idx + 1) & mask;
//     }
//     used[idx] = 1;
//     table[idx] = value;
//     data[unique++] = value;
//   skip_insert:;
//   }

//   free(table);
//   free(used);
//   return unique;
// }

// static size_t dedup_naive(uint32_t* data, size_t n) {
//   size_t unique = 0;
//   for (size_t i = 0; i < n; ++i) {
//     uint32_t value = data[i];
//     bool seen = false;
//     for (size_t j = 0; j < unique; ++j) {
//       if (data[j] == value) {
//         seen = true;
//         break;
//       }
//     }
//     if (!seen) {
//       data[unique++] = value;
//     }
//   }
//   return unique;
// }

// int main(int argc, char** argv) {
//   struct options opts = {
//       .repeat = 0,
//       .array_size = 0,
//       .max_value = UINT32_MAX,
//       .seed = (uint64_t)time(NULL),
//       .algorithm = "hash",
//   };

//   for (int i = 1; i < argc; ++i) {
//     const char* arg = argv[i];
//     const char* value = strchr(arg, '=');
//     char key_buf[64];
//     if (value) {
//       size_t key_len = (size_t)(value - arg);
//       if (key_len >= sizeof(key_buf)) {
//         fprintf(stderr, "Option name too long: %s\n", arg);
//         return EXIT_FAILURE;
//       }
//       memcpy(key_buf, arg, key_len);
//       key_buf[key_len] = '\0';
//       value += 1;
//       arg = key_buf;
//     }

//     if (strcmp(arg, "--repeat") == 0) {
//       if (!value && i + 1 < argc) {
//         value = argv[++i];
//       }
//       if (!value || !parse_size_t(value, &opts.repeat) || opts.repeat == 0) {
//         fprintf(stderr, "Invalid repeat value.\n");
//         return EXIT_FAILURE;
//       }
//     } else if (strcmp(arg, "--size") == 0) {
//       if (!value && i + 1 < argc) {
//         value = argv[++i];
//       }
//       if (!value || !parse_size_t(value, &opts.array_size) ||
//           opts.array_size == 0) {
//         fprintf(stderr, "Invalid size value.\n");
//         return EXIT_FAILURE;
//       }
//     } else if (strcmp(arg, "--max-value") == 0) {
//       if (!value && i + 1 < argc) {
//         value = argv[++i];
//       }
//       if (!value || !parse_uint32(value, &opts.max_value) ||
//           opts.max_value == 0) {
//         fprintf(stderr, "Invalid max-value.\n");
//         return EXIT_FAILURE;
//       }
//     } else if (strcmp(arg, "--seed") == 0) {
//       if (!value && i + 1 < argc) {
//         value = argv[++i];
//       }
//       if (!value) {
//         fprintf(stderr, "Missing seed value.\n");
//         return EXIT_FAILURE;
//       }
//       char* end = NULL;
//       errno = 0;
//       unsigned long long parsed = strtoull(value, &end, 10);
//       if (errno != 0 || end == value || *end != '\0') {
//         fprintf(stderr, "Invalid seed value.\n");
//         return EXIT_FAILURE;
//       }
//       opts.seed = parsed;
//     } else if (strcmp(arg, "--algorithm") == 0) {
//       if (!value && i + 1 < argc) {
//         value = argv[++i];
//       }
//       if (!value) {
//         fprintf(stderr, "Missing algorithm value.\n");
//         return EXIT_FAILURE;
//       }
//       if (strcmp(value, "hash") != 0 && strcmp(value, "naive") != 0) {
//         fprintf(stderr, "Unsupported algorithm: %s\n", value);
//         return EXIT_FAILURE;
//       }
//       opts.algorithm = value;
//     } else if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
//       usage(argv[0]);
//       return EXIT_SUCCESS;
//     } else {
//       fprintf(stderr, "Unknown option: %s\n", argv[i]);
//       usage(argv[0]);
//       return EXIT_FAILURE;
//     }
//   }

//   if (opts.repeat == 0 || opts.array_size == 0) {
//     usage(argv[0]);
//     return EXIT_FAILURE;
//   }

//   uint32_t* array = malloc(opts.array_size * sizeof(uint32_t));
//   if (!array) {
//     fprintf(
//         stderr, "Failed to allocate array of %zu elements.\n",
//         opts.array_size
//     );
//     return EXIT_FAILURE;
//   }

//   uint64_t rng_state = mix_seed(opts.seed);
//   size_t total_unique = 0;
//   size_t max_unique = 0;
//   size_t min_unique = SIZE_MAX;

//   for (size_t iter = 0; iter < opts.repeat; ++iter) {
//     for (size_t i = 0; i < opts.array_size; ++i) {
//       uint32_t value = rng_next(&rng_state);
//       if (opts.max_value != UINT32_MAX) {
//         value %= (uint64_t)opts.max_value;
//       }
//       array[i] = value;
//     }

//     size_t unique = 0;
//     if (strcmp(opts.algorithm, "hash") == 0) {
//       unique = dedup_hash(array, opts.array_size);
//     } else {
//       unique = dedup_naive(array, opts.array_size);
//     }

//     total_unique += unique;
//     if (unique > max_unique) {
//       max_unique = unique;
//     }
//     if (unique < min_unique) {
//       min_unique = unique;
//     }
//   }

//   double avg_unique = (double)total_unique / (double)opts.repeat;
//   if (min_unique == SIZE_MAX) {
//     min_unique = 0;
//   }

//   printf("Iterations: %zu\n", opts.repeat);
//   printf("Array size: %zu\n", opts.array_size);
//   printf("Average unique elements: %.2f\n", avg_unique);
//   printf("Min unique elements: %zu\n", min_unique);
//   printf("Max unique elements: %zu\n", max_unique);

//   free(array);
//   return EXIT_SUCCESS;
// }

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
      "cpu-calc-md5: MD5-based CPU load generator\n"
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
