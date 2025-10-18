// /*
// ema_sort_int.c
// External-memory integer sort (single-threaded).
// Sorts a file containing 32-bit signed integers (binary format).
// If the input file is text, you can pre-convert or adapt program.

// Build:
// gcc -std=c11 -Wall -Wextra -O0 -o ema_sort_int ema_sort_int.c

// Usage:
// ema_sort_int --file in.bin --mem_limit 10000000 --tmpdir /tmp --repeat 1

// Parameters:
// --file PATH         : input file with int32_t values (binary little-endian)
// --mem_limit N       : memory limit for in-memory chunk in bytes (must be >=
// 4)
// --tmpdir PATH       : temporary directory for runs (default /tmp)
// --repeat N          : repeat full sort N times (to increase runtime)

// Notes:

// * Program writes sorted result to <file>.sorted
// * Basic two-phase external merge sort: produce sorted runs that fit into
// mem_limit, then do k-way merge using simple priority queue (binary heap).
//   */

// #define _GNU_SOURCE
// #include <inttypes.h>
// #include <limits.h>
// #include <stdbool.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <sys/stat.h>
// #include <time.h>
// #include <unistd.h>

// typedef int32_t val_t;

// static void usage(const char* p) {
//   fprintf(
//       stderr,
//       "Usage: %s --file PATH --mem_limit N [--tmpdir DIR] [--repeat N]\n",
//       p
//   );
// }

// static int cmp_val(const void* a, const void* b) {
//   val_t va = *(const val_t*)a;
//   val_t vb = *(const val_t*)b;
//   if (va < vb)
//     return -1;
//   if (va > vb)
//     return 1;
//   return 0;
// }

// struct heap_node {
//   val_t value;
//   FILE* fp;
//   int idx;
// };

// static void heap_swap(struct heap_node* a, struct heap_node* b) {
//   struct heap_node t = *a;
//   *a = *b;
//   *b = t;
// }

// static void heapify_down(struct heap_node* heap, int n, int i) {
//   int smallest = i;
//   int l = 2 * i + 1;
//   int r = 2 * i + 2;
//   if (l < n && heap[l].value < heap[smallest].value)
//     smallest = l;
//   if (r < n && heap[r].value < heap[smallest].value)
//     smallest = r;
//   if (smallest != i) {
//     heap_swap(&heap[i], &heap[smallest]);
//     heapify_down(heap, n, smallest);
//   }
// }

// int main(int argc, char** argv) {
//   const char* file = NULL;
//   size_t mem_limit = 0;
//   const char* tmpdir = "/tmp";
//   uint64_t repeat = 1;

//   for (int i = 1; i < argc; ++i) {
//     if (strcmp(argv[i], "--file") == 0 && i + 1 < argc)
//       file = argv[++i];
//     else if (strcmp(argv[i], "--mem_limit") == 0 && i + 1 < argc)
//       mem_limit = (size_t)strtoull(argv[++i], NULL, 10);
//     else if (strcmp(argv[i], "--tmpdir") == 0 && i + 1 < argc)
//       tmpdir = argv[++i];
//     else if (strcmp(argv[i], "--repeat") == 0 && i + 1 < argc)
//       repeat = (uint64_t)strtoull(argv[++i], NULL, 10);
//     else {
//       usage(argv[0]);
//       return 2;
//     }
//   }

//   if (!file || mem_limit < sizeof(val_t)) {
//     usage(argv[0]);
//     return 2;
//   }

//   // Get file size
//   FILE* in = fopen(file, "rb");
//   if (!in) {
//     perror("fopen");
//     return 3;
//   }
//   if (fseek(in, 0, SEEK_END) != 0) {
//     perror("fseek");
//     fclose(in);
//     return 3;
//   }
//   long fsize = ftell(in);
//   if (fsize < 0) {
//     perror("ftell");
//     fclose(in);
//     return 3;
//   }
//   rewind(in);

//   size_t total_vals = (size_t)fsize / sizeof(val_t);
//   if (total_vals == 0) {
//     fprintf(stderr, "Empty input\n");
//     fclose(in);
//     return 3;
//   }

//   size_t vals_per_chunk = mem_limit / sizeof(val_t);
//   if (vals_per_chunk == 0) {
//     fprintf(stderr, "mem_limit too small\n");
//     fclose(in);
//     return 2;
//   }

//   // create runs
//   char run_template[PATH_MAX];
//   snprintf(run_template, sizeof(run_template), "%s/ema_run_XXXXXX", tmpdir);
//   struct timespec t0, t1;
//   clock_gettime(CLOCK_MONOTONIC, &t0);

//   for (uint64_t rep = 0; rep < repeat; ++rep) {
//     rewind(in);
//     // produce sorted runs
//     size_t runs = 0;
//     val_t* buffer = malloc(vals_per_chunk * sizeof(val_t));
//     if (!buffer) {
//       perror("malloc");
//       fclose(in);
//       return 4;
//     }

//     while (!feof(in)) {
//       size_t read_vals = fread(buffer, sizeof(val_t), vals_per_chunk, in);
//       if (read_vals == 0)
//         break;
//       qsort(buffer, read_vals, sizeof(val_t), cmp_val);

//       // write to temp file
//       char path[PATH_MAX];
//       strcpy(path, run_template);
//       int fd = mkstemp(path);
//       if (fd == -1) {
//         perror("mkstemp");
//         free(buffer);
//         fclose(in);
//         return 5;
//       }
//       FILE* rf = fdopen(fd, "wb");
//       if (!rf) {
//         perror("fdopen");
//         close(fd);
//         unlink(path);
//         free(buffer);
//         fclose(in);
//         return 5;
//       }
//       size_t w = fwrite(buffer, sizeof(val_t), read_vals, rf);
//       if (w != read_vals) {
//         perror("fwrite");
//         fclose(rf);
//         unlink(path);
//         free(buffer);
//         fclose(in);
//         return 5;
//       }
//       fclose(rf);
//       ++runs;
//       // store run path for merge by re-opening later via naming convention
//     }
//     free(buffer);

//     if (runs == 0) {
//       fprintf(stderr, "No runs produced\n");
//       fclose(in);
//       return 5;
//     }

//     // Collect run file names
//     // Simpler approach: list tmpdir files matching pattern is complex;
//     instead
//     // we will reuse mkstemp names stored during creation. For clarity we
//     will
//     // create run paths with increasing suffix stored in array. But above we
//     // didn't store them; to keep code short, re-create runs with
//     deterministic
//     // names: Instead of mkstemp, better to create:
//     // <tmpdir>/ema_run_<rep>_<i>.bin For simplicity in this demo, assume
//     only
//     // one repetition and rename above approach in robust implementation.

//     // For this exercise, let's do a simple external merge: reopen generated
//     run
//     // files by scanning tmpdir is complex here. A production-ready
//     // implementation would store run paths in vector during creation.

//     // As placeholder: copy input sorted into output (if input fits). We'll
//     // instead write sorted output directly if runs==1.

//     // If only one run, rename it to output
//     if (runs == 1) {
//       // find run file created in tmpdir by pattern; this is simplified: user
//       // can check result manually
//     }

//     // For brevity: instead of a full k-way merge implementation, we'll print
//     // message indicating where runs are.
//     fprintf(
//         stderr,
//         "Produced %" PRIu64 " sorted run(s) in %s (manual merge expected)\n",
//         (uint64_t)runs,
//         tmpdir
//     );
//   }

//   clock_gettime(CLOCK_MONOTONIC, &t1);
//   double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
//   fprintf(stderr, "ema_sort_int completed in %.3f s\n", elapsed);

//   fclose(in);
//   return 0;
// }

// ema-sort-int.c
// Compile: gcc -O0 ema-sort-int.c -o ema-sort-int
// Usage: see below

#define _GNU_SOURCE
#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void usage(const char* pname) {
  fprintf(
      stderr,
      "ema-sort-int: external-memory integer sort\n"
      "Usage: %s [options]\n"
      " -n N    total number of ints to sort (default 10000000)\n"
      " -m M    chunk size (ints) that fits in memory (default 1000000)\n"
      " -r R    repetitions (repeat whole sort R times) (default 1)\n        "
      "Example: %s -n 10000000 -m 1000000 -r 1\n",
      pname,
      pname
  );
}

static int cmp_int(const void* a, const void* b) {
  int32_t va = *(int32_t*)a;
  int32_t vb = *(int32_t*)b;
  return (va < vb) ? -1 : (va > vb);
}

// write random ints to binary file
int generate_input(const char* fname, size_t n) {
  FILE* f = fopen(fname, "wb");
  if (!f) {
    perror("fopen");
    return -1;
  }
  srand((unsigned)time(NULL) ^ (unsigned)getpid());
  for (size_t i = 0; i < n; ++i) {
    int32_t v = (int32_t)rand();
    if (fwrite(&v, sizeof(v), 1, f) != 1) {
      perror("fwrite");
      fclose(f);
      return -1;
    }
  }
  fclose(f);
  return 0;
}

typedef struct {
  FILE* f;
  int32_t cur;
  int eof;
} reader_t;

// read next int, set eof when done
int read_next(reader_t* r) {
  if (r->eof)
    return 0;
  if (fread(&r->cur, sizeof(r->cur), 1, r->f) != 1) {
    r->eof = 1;
    return 0;
  }
  return 1;
}

// minimal heap for k-way merge
typedef struct {
  int32_t value;
  int idx;
} heapitem_t;

static void swap(heapitem_t* a, heapitem_t* b) {
  heapitem_t t = *a;
  *a = *b;
  *b = t;
}
static void heapify_down(heapitem_t* h, int n, int i) {
  int smallest = i;
  int l = 2 * i + 1, r = 2 * i + 2;
  if (l < n && h[l].value < h[smallest].value)
    smallest = l;
  if (r < n && h[r].value < h[smallest].value)
    smallest = r;
  if (smallest != i) {
    swap(&h[i], &h[smallest]);
    heapify_down(h, n, smallest);
  }
}
static void heapify_up(heapitem_t* h, int idx) {
  while (idx > 0) {
    int p = (idx - 1) / 2;
    if (h[p].value <= h[idx].value)
      break;
    swap(&h[p], &h[idx]);
    idx = p;
  }
}

// perform external sort (input file -> output file)
int external_sort(
    const char* infile, const char* outfile, size_t total_n, size_t chunk_n
) {
  // 1) split into sorted chunk files
  size_t chunks = (total_n + chunk_n - 1) / chunk_n;
  char** tmpnames = calloc(chunks, sizeof(char*));
  int32_t* buf = malloc(chunk_n * sizeof(int32_t));
  if (!tmpnames || !buf) {
    perror("malloc");
    return -1;
  }

  FILE* in = fopen(infile, "rb");
  if (!in) {
    perror("fopen in");
    return -1;
  }

  for (size_t c = 0; c < chunks; ++c) {
    size_t need = chunk_n;
    if (c == chunks - 1)
      need = total_n - c * chunk_n;
    size_t got = fread(buf, sizeof(int32_t), need, in);
    if (got != need) {
      if (feof(in)) { /* ok if smaller */
      } else {
        perror("fread");
        fclose(in);
        return -1;
      }
    }
    qsort(buf, got, sizeof(int32_t), cmp_int);
    tmpnames[c] = malloc(64);
    snprintf(tmpnames[c], 64, "tmp_chunk_%zu.bin", c);
    FILE* tf = fopen(tmpnames[c], "wb");
    if (!tf) {
      perror("fopen tmp");
      fclose(in);
      return -1;
    }
    if (fwrite(buf, sizeof(int32_t), got, tf) != got) {
      perror("fwrite tmp");
      fclose(tf);
      fclose(in);
      return -1;
    }
    fclose(tf);
  }
  fclose(in);
  free(buf);

  // 2) k-way merge
  reader_t* readers = calloc(chunks, sizeof(reader_t));
  for (size_t i = 0; i < chunks; ++i) {
    readers[i].f = fopen(tmpnames[i], "rb");
    readers[i].eof = 0;
    if (!readers[i].f) {
      perror("fopen tmp for merge");
      return -1;
    }
    read_next(&readers[i]);
  }
  FILE* out = fopen(outfile, "wb");
  if (!out) {
    perror("fopen out");
    return -1;
  }

  heapitem_t* heap = malloc(chunks * sizeof(heapitem_t));
  int heap_n = 0;
  for (int i = 0; i < (int)chunks; ++i) {
    if (!readers[i].eof) {
      heap[heap_n].value = readers[i].cur;
      heap[heap_n].idx = i;
      heapify_up(heap, heap_n);
      heap_n++;
    }
  }

  while (heap_n > 0) {
    // pop min
    int32_t val = heap[0].value;
    int idx = heap[0].idx;
    // replace root with last
    heap[0] = heap[heap_n - 1];
    heap_n--;
    heapify_down(heap, heap_n, 0);
    // write val
    if (fwrite(&val, sizeof(val), 1, out) != 1) {
      perror("fwrite out");
      return -1;
    }
    // advance reader idx
    if (read_next(&readers[idx])) {
      // push new value
      heap[heap_n].value = readers[idx].cur;
      heap[heap_n].idx = idx;
      heapify_up(heap, heap_n);
      heap_n++;
    } else {
      // reader exhausted
    }
  }

  // cleanup
  fclose(out);
  for (size_t i = 0; i < chunks; ++i) {
    if (readers[i].f)
      fclose(readers[i].f);
    if (tmpnames[i]) {
      remove(tmpnames[i]);
      free(tmpnames[i]);
    }
  }
  free(tmpnames);
  free(readers);
  free(heap);
  return 0;
}

int main(int argc, char** argv) {
  size_t n = 10000000;  // total ints
  size_t m = 1000000;   // chunk ints
  int repetitions = 1;
  int opt;
  while ((opt = getopt(argc, argv, "n:m:r:h")) != -1) {
    switch (opt) {
      case 'n':
        n = (size_t)atol(optarg);
        break;
      case 'm':
        m = (size_t)atol(optarg);
        break;
      case 'r':
        repetitions = atoi(optarg);
        break;
      default:
        usage(argv[0]);
        return 1;
    }
  }

  const char* infile = "ema_input.bin";
  const char* outfile = "ema_output.bin";

  // generate input once
  fprintf(stderr, "Generating input file (%s) with %zu ints...\n", infile, n);
  if (generate_input(infile, n) != 0)
    return 2;
  fprintf(stderr, "Input generated.\n");

  for (int rep = 0; rep < repetitions; ++rep) {
    fprintf(
        stderr,
        "Run %d/%d: external sorting (n=%zu, chunk=%zu)...\n",
        rep + 1,
        repetitions,
        n,
        m
    );
    time_t t0 = time(NULL);
    int rc = external_sort(infile, outfile, n, m);
    time_t t1 = time(NULL);
    if (rc != 0) {
      fprintf(stderr, "external_sort failed\n");
      return 3;
    }
    fprintf(
        stderr, "Run %d finished, elapsed ~%ld s\n", rep + 1, (long)(t1 - t0)
    );
    // Optionally rename output to avoid overwrite in next repetition
    char outname[64];
    snprintf(outname, sizeof(outname), "ema_output_run%d.bin", rep + 1);
    rename(outfile, outname);
  }
  fprintf(stderr, "All runs completed.\n");
  return 0;
}
