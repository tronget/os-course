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

    // Добавляем PID к имени временного файла
    pid_t pid = getpid();
    snprintf(tmpnames[c], 64, "tmp_chunk_%d_%zu.bin", pid, c);

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
