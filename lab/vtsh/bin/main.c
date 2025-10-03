/* Feature test macro for POSIX APIs (getline, clock_gettime, vfork) */
#define _GNU_SOURCE
// #define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "vtsh.h"

/* Maximum number of arguments (excluding terminating NULL) */
enum {
  MAX_ARGS = 256,
  ERRBUF_SIZE = 128,
  NEWLINE_CHAR = '\n',
  ERRNO_STATUS_MASK = 0xFF, /* mask errno to single byte */
};

#define CD_COMMAND "cd"
#define EXIT_COMMAND "exit"
#define ELAPSED_FMT "Elapsed: %ld.%06ld s\n"
enum {
  NSEC_PER_SEC = 1000000000L,
  NSEC_PER_USEC = 1000L,
};
#define TOKEN_DELIMS " \t"
#define MSG_CMD_NOT_FOUND "Command not found"

static void timespec_diff(
    const struct timespec* start,
    const struct timespec* end,
    struct timespec* res
) {
  res->tv_sec = end->tv_sec - start->tv_sec;
  res->tv_nsec = end->tv_nsec - start->tv_nsec;
  if (res->tv_nsec < 0) {
    res->tv_sec -= 1;
    res->tv_nsec += NSEC_PER_SEC;
  }
}

/* Free argv vector */
static void free_argv(char** argv, int argc) {
  if (!argv) {
    return;
  }
  for (int i = 0; i < argc; ++i) {
    free(argv[i]);
  }
  free((void*)argv);
}

// Парсинг строки в argv; возвращает NULL-terminated массив аргументов.

static char** parse_line(char* line, int* out_argc) {
  char** argv = NULL;
  int argc = 0;
  char* saveptr = NULL;
  char* tok = NULL;

  /* Удаляем завершающий перевод строки, если есть */
  size_t len = strlen(line);
  if (len > 0 && line[len - 1] == NEWLINE_CHAR) {
    line[len - 1] = '\0';
  }

  argv = (char**)calloc(MAX_ARGS + 1, sizeof(char*));
  if (argv == NULL) {
    perror("calloc");
    return NULL;
  }

  tok = strtok_r(line, TOKEN_DELIMS, &saveptr);
  while (tok != NULL && argc < MAX_ARGS) {
    argv[argc] = strdup(tok);
    if (argv[argc] == NULL) {
      perror("strdup");
      free_argv(argv, argc);
      return NULL;
    }
    ++argc;
    tok = strtok_r(NULL, TOKEN_DELIMS, &saveptr);
  }

  argv[argc] = NULL;

  if (out_argc) {
    *out_argc = argc;
  }

  return argv;
}

/* Returns 1 if line contains only whitespace, 0 otherwise */
static int is_blank_line(const char* line) {
  if (!line) {
    return 1;
  }
  const unsigned char* ptr = (const unsigned char*)line;
  while (*ptr != '\0') {
    if (*ptr != ' ' && *ptr != '\t' && *ptr != '\n' && *ptr != '\r' &&
        *ptr != '\v' && *ptr != '\f') {
      return 0;
    }
    ++ptr;
  }
  return 1;
}

/* Print prompt if interactive; returns 0 on success, -1 to terminate */
static int print_prompt(void) {
  int is_terminal = isatty(STDIN_FILENO);
  if (is_terminal == 1) {
    const char* prompt = vtsh_prompt();
    if (prompt == NULL) {
      prompt = ""; /* should not happen */
    }
    if (fputs(prompt, stdout) == EOF) {
      perror("fputs");
      return -1;
    }
    if (fflush(stdout) == EOF) {
      perror("fflush");
      return -1;
    }
  } else if (is_terminal == -1) {
    perror("isatty");
    /* Non-fatal: continue */
  }
  return 0;
}

enum {
  BUILTIN_NONE = 0,
  BUILTIN_EXIT = 1,
  BUILTIN_CD = 2,
};

/* Handle builtin commands; returns BUILTIN_EXIT if shell must exit */
static int handle_builtin(int argc, char** argv) {
  if (argc <= 0) {
    return BUILTIN_NONE;
  }

  if (strcmp(argv[0], EXIT_COMMAND) == 0) {
    return BUILTIN_EXIT;
  }

  if (strcmp(argv[0], CD_COMMAND) == 0) {
    if (argc < 2) {
      if (fprintf(stderr, "cd: missing operand\n") < 0) {
        perror("fprintf");
      }
      return BUILTIN_CD;
    }

    if (argc > 2) {
      if (fprintf(stderr, "cd: too many arguments\n") < 0) {
        perror("fprintf");
      }
      return BUILTIN_CD;
    }

    if (chdir(argv[1]) != 0) {
      perror("cd");
    }
    return BUILTIN_CD;
  }

  return BUILTIN_NONE;
}

/* Report execution failure (non-ENOENT) */
static void report_exec_error(const char* cmd, int errcode) {
  char errbuf[ERRBUF_SIZE];
  char* errstr = strerror_r(errcode, errbuf, sizeof(errbuf));
  if (errstr == NULL) {
    if (fprintf(
            stderr,
            "Child exited with errno=%d (message unavailable)\n",
            errcode
        ) < 0) {
      perror("fprintf");
    }
  } else {
    if (fprintf(
            stderr, "Failed to exec '%s': errno=%d (%s)\n", cmd, errcode, errstr
        ) < 0) {
      perror("fprintf");
    }
  }
}

/* Execute a command vector using vfork/execvp; prints errors itself */
static void execute_command(char** argv) {
  /* Start time */
  struct timespec t_start;
  if (clock_gettime(CLOCK_MONOTONIC, &t_start) == -1) {
    perror("clock_gettime");
    t_start.tv_sec = 0;
    t_start.tv_nsec = 0;
  }

  pid_t pid = vfork();
  if (pid == -1) {
    perror("vfork");
    return;
  }
  if (pid == 0) {
    execvp(argv[0], argv);
    int err = errno; /* async-signal-safe only */
    _exit((unsigned char)(((unsigned int)err) &
                          (unsigned int)ERRNO_STATUS_MASK));
  }

  int status = 0;
  if (waitpid(pid, &status, 0) == -1) {
    perror("waitpid");
  }

  struct timespec t_end;
  if (clock_gettime(CLOCK_MONOTONIC, &t_end) == -1) {
    perror("clock_gettime");
    t_end.tv_sec = t_start.tv_sec;
    t_end.tv_nsec = t_start.tv_nsec;
  }
  struct timespec t_diff;
  timespec_diff(&t_start, &t_end, &t_diff);

  if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
    int errcode = WEXITSTATUS(status);
    if (errcode == ENOENT) {
      if (printf("%s\n", MSG_CMD_NOT_FOUND) < 0) {
        perror("printf");
      }
    } else {
      report_exec_error(argv[0], errcode);
    }
  } else if (WIFSIGNALED(status)) {
    if (fprintf(stderr, "Process terminated by signal %d\n", WTERMSIG(status)) <
        0) {
      perror("fprintf");
    }
  }

  long seconds = t_diff.tv_sec;
  long usec = t_diff.tv_nsec / NSEC_PER_USEC;
  if (printf(ELAPSED_FMT, seconds, usec) < 0) {
    perror("printf");
  }
}

int main(void) {
  char* line = NULL;
  size_t linecap = 0;
  for (;;) {
    printf("%s", vtsh_prompt());

    ssize_t linelen = getline(&line, &linecap, stdin);
    if (linelen == -1) {
      if (putchar(NEWLINE_CHAR) == EOF) {
        perror("putchar");
      }
      break;
    }
    if (is_blank_line(line)) {
      continue;
    }

    int argc = 0;
    char** argv = parse_line(line, &argc);
    if (argv == NULL) {
      continue; /* parse error already reported */
    }
    if (argc == 0) {
      free_argv(argv, argc);
      continue;
    }

    int builtin = handle_builtin(argc, argv);
    if (builtin == BUILTIN_EXIT) {
      free_argv(argv, argc);
      break;
    }
    if (builtin == BUILTIN_NONE) {
      execute_command(argv);
    }
    free_argv(argv, argc);
  }
  free(line);
  return EXIT_SUCCESS;
}
