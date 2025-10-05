#define _GNU_SOURCE

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "vtsh.h"

enum {
  MAX_TOKENS = 256,
  STATUS_CMD_NOT_FOUND = 127,
  STATUS_EXEC_ERROR = 126,
  STATUS_SIGNAL_BASE = 128,
  EXIT_BASE = 256,
  EXIT_MAX = 255,
  RADIX_DECIMAL = 10,
  NSEC_PER_SEC = 1000000000L,
  NSEC_PER_USEC = 1000L,
  ERRNO_STATUS_MASK = 0xFF,  // mask errno to single byte
  SHOW_ELAPSED_TIME = 0,     // set to 1 to show elapsed time for each command
  NULL_CHAR = '\0',
  NEWLINE_CHAR = '\n',
  CARRIAGE_RETURN_CHAR = '\r'
};

#define CD_COMMAND "cd"
#define EXIT_COMMAND "exit"
#define OR_TOKEN "||"
#define MSG_CMD_NOT_FOUND "Command not found"
#define TOKEN_DELIMS " \t"
#define ELAPSED_FMT "Elapsed: %ld.%06ld s\n"

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

static void print_elapsed_time(
    const struct timespec* t_start, const struct timespec* t_end
) {
  struct timespec t_diff;
  timespec_diff(t_start, t_end, &t_diff);
  long seconds = t_diff.tv_sec;
  long usec = t_diff.tv_nsec / NSEC_PER_USEC;
  if (printf(ELAPSED_FMT, seconds, usec) < 0) {
    perror("printf");
  }
}

static int is_blank_line(const char* line) {
  if (!line) {
    return 1;
  }
  const unsigned char* ptr = (const unsigned char*)line;
  while (*ptr != NULL_CHAR) {
    if (!isspace(*ptr)) {
      return 0;
    }
    ++ptr;
  }
  return 1;
}

static void free_tokens(char** tokens, size_t count) {
  if (!tokens) {
    return;
  }
  for (size_t i = 0; i < count; ++i) {
    free(tokens[i]);
  }
  free((void*)tokens);
}

static char* normalize_line(const char* line) {
  if (!line) {
    return NULL;
  }
  size_t len = strlen(line);
  size_t buffer_len = (len * 3) + 1;
  char* normalized = (char*)malloc(buffer_len);
  if (!normalized) {
    perror("malloc");
    return NULL;
  }

  size_t out_idx = 0;
  for (size_t i = 0; i < len; ++i) {
    unsigned char current_char = (unsigned char)line[i];
    if (current_char == NEWLINE_CHAR || current_char == CARRIAGE_RETURN_CHAR) {
      continue;
    }
    if (current_char == '|' && i + 1 < len && line[i + 1] == '|') {
      normalized[out_idx++] = ' ';
      normalized[out_idx++] = '|';
      normalized[out_idx++] = '|';
      normalized[out_idx++] = ' ';
      ++i;
      continue;
    }
    normalized[out_idx++] = (char)current_char;
  }
  normalized[out_idx] = NULL_CHAR;
  return normalized;
}

static int tokenize_line(
    const char* line, char*** out_tokens, size_t* out_count
) {
  if (!line || !out_tokens || !out_count) {
    return -1;
  }

  char** tokens = (char**)calloc(MAX_TOKENS + 1, sizeof(char*));
  if (!tokens) {
    perror("calloc");
    return -1;
  }

  char* normalized = normalize_line(line);
  if (!normalized) {
    free((void*)tokens);
    return -1;
  }

  size_t count = 0;
  char* saveptr = NULL;
  for (char* token = strtok_r(normalized, TOKEN_DELIMS, &saveptr);
       token != NULL;
       token = strtok_r(NULL, TOKEN_DELIMS, &saveptr)) {
    if (count >= MAX_TOKENS) {
      if (fprintf(stderr, "Too many tokens\n") < 0) {
        perror("fprintf");
      }
      free_tokens(tokens, count);
      free(normalized);
      return -1;
    }
    tokens[count] = strdup(token);
    if (!tokens[count]) {
      perror("strdup");
      free_tokens(tokens, count);
      free(normalized);
      return -1;
    }
    ++count;
  }

  free(normalized);
  tokens[count] = NULL;
  *out_tokens = tokens;
  *out_count = count;
  return 0;
}

static int validate_tokens(char** tokens, size_t count) {
  if (count == 0) {
    return 0;
  }
  int expect_command = 1;
  for (size_t i = 0; i < count; ++i) {
    if (strcmp(tokens[i], OR_TOKEN) == 0) {
      if (expect_command) {
        return -1;
      }
      expect_command = 1;
    } else {
      expect_command = 0;
    }
  }
  return expect_command ? -1 : 0;
}

static int parse_exit_status(const char* arg) {
  if (!arg) {
    return EXIT_SUCCESS;
  }
  errno = 0;
  char* endptr = NULL;
  long value = strtol(arg, &endptr, RADIX_DECIMAL);
  if (errno != 0 || endptr == arg || *endptr != NULL_CHAR) {
    return EXIT_FAILURE;
  }
  if (value < 0) {
    return EXIT_FAILURE;
  }
  if (value > EXIT_MAX) {
    value %= EXIT_BASE;
  }
  return (int)value;
}

static void close_fds(int in_fd, int out_fd) {
  if (in_fd != -1) {
    close(in_fd);
  }
  if (out_fd != -1) {
    close(out_fd);
  }
}

/*
 * Execute external command with optional input/output file descriptors.
 * in_fd/out_fd are file descriptors already opened in parent (or -1).
 * This function will vfork, dup2 fds in the child, execvp, wait in parent,
 * and close opened fds in parent after wait.
 */
static int execute_external_fds(char** argv, int in_fd, int out_fd) {
  if (!argv || !argv[0]) {
    return EXIT_FAILURE;
  }

  struct timespec t_start;
  if (clock_gettime(CLOCK_MONOTONIC, &t_start) == -1) {
    perror("clock_gettime");
    t_start.tv_sec = 0;
    t_start.tv_nsec = 0;
  }

  pid_t pid = vfork();
  if (pid == -1) {
    perror("vfork");
    // close fds in parent (they were opened here before calling this function)
    close_fds(in_fd, out_fd);
    return EXIT_FAILURE;
  }

  if (pid == 0) {
    /* Child (vfork): perform dup2 for redirections, then exec */
    if (in_fd != -1) {
      if (dup2(in_fd, STDIN_FILENO) == -1) {
        int err = errno;
        _exit((unsigned char)(((unsigned int)err) &
                              (unsigned int)ERRNO_STATUS_MASK));
      }
      close(in_fd);
    }
    if (out_fd != -1) {
      if (dup2(out_fd, STDOUT_FILENO) == -1) {
        int err = errno;
        _exit((unsigned char)(((unsigned int)err) &
                              (unsigned int)ERRNO_STATUS_MASK));
      }
      close(out_fd);
    }
    execvp(argv[0], argv);
    int err = errno; /* async-signal-safe only */
    _exit((unsigned char)(((unsigned int)err) &
                          (unsigned int)ERRNO_STATUS_MASK));
  }

  int status = 0;
  int return_code = 0;
  if (waitpid(pid, &status, 0) == -1) {
    perror("waitpid");
    return_code = EXIT_FAILURE;

  } else if (WIFEXITED(status)) {
    return_code = WEXITSTATUS(status);
    if (return_code == ENOENT) {
      if (printf("%s\n", MSG_CMD_NOT_FOUND) < 0) {
        perror("printf");
      }
    }

  } else if (WIFSIGNALED(status)) {
    return_code = STATUS_SIGNAL_BASE + WTERMSIG(status);

  } else {
    return_code = STATUS_EXEC_ERROR;
  }

  if (SHOW_ELAPSED_TIME) {
    struct timespec t_end;
    if (clock_gettime(CLOCK_MONOTONIC, &t_end) == -1) {
      perror("clock_gettime");
      t_end.tv_sec = t_start.tv_sec;
      t_end.tv_nsec = t_start.tv_nsec;
    }
    print_elapsed_time(&t_start, &t_end);
  }

  /* close fds opened in parent to avoid leaks */
  close_fds(in_fd, out_fd);

  return return_code;
}

static int run_single_command(int argc, char** tokens, int* should_exit) {
  char* argv[MAX_TOKENS + 1];
  for (int i = 0; i < argc; ++i) {
    argv[i] = tokens[i];
  }
  argv[argc] = NULL;

  if (argc == 0) {
    return EXIT_SUCCESS;
  }

  if (strcmp(argv[0], EXIT_COMMAND) == 0) {
    *should_exit = 1;
    if (argc > 1) {
      return parse_exit_status(argv[1]);
    }
    return EXIT_SUCCESS;
  }

  if (strcmp(argv[0], CD_COMMAND) == 0) {
    if (argc < 2) {
      if (fprintf(stderr, "cd: missing operand\n") < 0) {
        perror("fprintf");
      }
      return EXIT_FAILURE;
    }
    if (argc > 2) {
      if (fprintf(stderr, "cd: too many arguments\n") < 0) {
        perror("fprintf");
      }
      return EXIT_FAILURE;
    }
    if (chdir(argv[1]) != 0) {
      perror("cd");
      return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
  }

  /* ---- Parse redirections from argv[] ----
     Recognized forms:
       ">" "filename"
       "<" "filename"
       ">filename"
       "<filename"
     We treat ">>" or ">>filename" as syntax error (append not supported).
     If syntax error detected, print "Syntax error\n" and return.
     If opening file(s) fails, print "I/O error\n" and return.
  */
  char* in_file = NULL;
  char* out_file = NULL;
  int seen_in = 0;
  int seen_out = 0;
  int syntax_err = 0;

  /* mark tokens to skip when building exec argv */
  int skip_mask[MAX_TOKENS] = {0};

  for (int i = 0; i < argc; ++i) {
    char* token = argv[i];
    if (!token || token[0] == NULL_CHAR) {
      continue;
    }
    if (token[0] == '>' || token[0] == '<') {
      /* detect >> as error */
      if (token[0] == '>' && token[1] == '>') {
        syntax_err = 1;
        break;
      }
      if (token[0] == '<' && token[1] == '<') {
        /* here-doc not supported -> syntax error */
        syntax_err = 1;
        break;
      }

      if (token[1] != NULL_CHAR) {
        /* form: >filename or <filename */
        if (token[0] == '>') {
          if (seen_out) {
            syntax_err = 1;
            break;
          }
          out_file = token + 1;
          seen_out = 1;
          skip_mask[i] = 1;
        } else {
          if (seen_in) {
            syntax_err = 1;
            break;
          }
          in_file = token + 1;
          seen_in = 1;
          skip_mask[i] = 1;
        }
      } else {
        /* form: ">" or "<" - next token must be filename */
        if (i + 1 >= argc) {
          syntax_err = 1;
          break;
        }
        char* next = argv[i + 1];
        if (!next || next[0] == NULL_CHAR) {
          syntax_err = 1;
          break;
        }
        /* if next token starts with > or < => syntax error */
        if (next[0] == '>' || next[0] == '<') {
          syntax_err = 1;
          break;
        }
        if (token[0] == '>') {
          if (seen_out) {
            syntax_err = 1;
            break;
          }
          out_file = next;
          seen_out = 1;
        } else {
          if (seen_in) {
            syntax_err = 1;
            break;
          }
          in_file = next;
          seen_in = 1;
        }
        skip_mask[i] = 1;
        skip_mask[i + 1] = 1;
        i++; /* skip next as we've consumed it */
      }
    }
  }

  if (syntax_err) {
    if (printf("Syntax error\n") < 0) {
      perror("printf");
    }
    return EXIT_FAILURE;
  }

  /* Build argv_exec (without redirection tokens/filenames) */
  char* argv_exec[MAX_TOKENS + 1];
  int exec_idx = 0;
  for (int i = 0; i < argc; ++i) {
    if (skip_mask[i]) {
      continue;
    }
    argv_exec[exec_idx++] = argv[i];
  }
  argv_exec[exec_idx] = NULL;

  /* If no command remains (e.g., user typed only redirections), that's a syntax
   * error */
  if (exec_idx == 0) {
    if (printf("Syntax error\n") < 0) {
      perror("printf");
    }
    return EXIT_FAILURE;
  }

  /* Attempt to open files (in parent). If any open fails -> I/O error */
  int in_fd = -1;
  int out_fd = -1;
  if (in_file) {
    in_fd = open(in_file, O_RDONLY);
    if (in_fd == -1) {
      if (printf("I/O error\n") < 0) {
        perror("printf");
      }
      /* ensure we don't leak an out_fd if already opened (not here yet) */
      return EXIT_FAILURE;
    }
  }
  if (out_file) {
    out_fd = open(out_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      if (in_fd != -1) {
        close(in_fd);
      }
      if (printf("I/O error\n") < 0) {
        perror("printf");
      }
      return EXIT_FAILURE;
    }
  }

  /* Execute with fds; execute_external_fds will close in_fd/out_fd in parent
   * after wait. */
  return execute_external_fds(argv_exec, in_fd, out_fd);
}

static int execute_tokens(char** tokens, size_t count, int* should_exit) {
  size_t pos = 0;
  int last_status = EXIT_SUCCESS;
  while (pos < count) {
    size_t next = pos;
    while (next < count && strcmp(tokens[next], OR_TOKEN) != 0) {
      ++next;
    }
    int argc = (int)(next - pos);
    last_status = run_single_command(argc, &tokens[pos], should_exit);
    if (*should_exit || last_status == EXIT_SUCCESS) {
      break;
    }
    if (next >= count) {
      break;
    }
    pos = next + 1;
  }
  return last_status;
}

static int show_prompt(void) {
  const char* prompt = vtsh_prompt();
  if (prompt && fputs(prompt, stdout) == EOF) {
    perror("fputs");
    return -1;
  }
  if (fflush(stdout) == EOF) {
    perror("fflush");
    return -1;
  }
  return 0;
}

static void process_line(char* line, int* exit_code, int* should_exit) {
  if (is_blank_line(line)) {
    return;
  }

  char** tokens = NULL;
  size_t token_count = 0;
  if (tokenize_line(line, &tokens, &token_count) != 0) {
    return;
  }

  if (token_count == 0) {
    free_tokens(tokens, token_count);
    return;
  }

  if (validate_tokens(tokens, token_count) != 0) {
    if (printf("Syntax error\n") < 0) {
      perror("printf");
    }
    free_tokens(tokens, token_count);
    return;
  }

  *exit_code = execute_tokens(tokens, token_count, should_exit);
  free_tokens(tokens, token_count);

  if (!*should_exit) {
    *exit_code = EXIT_SUCCESS;
  }
}

int main(void) {
  char* line = NULL;
  size_t capacity = 0;
  int exit_code = EXIT_SUCCESS;

  if (setvbuf(stdin, NULL, _IONBF, 0) != 0) {
    perror("setvbuf");
  }

  for (;;) {
    if (show_prompt() != 0) {
      exit_code = EXIT_FAILURE;
      break;
    }

    ssize_t len = getline(&line, &capacity, stdin);
    if (len == -1) {
      if (feof(stdin)) {
        if (putchar(NEWLINE_CHAR) == EOF) {
          perror("putchar");
        }
      } else {
        perror("getline");
        exit_code = EXIT_FAILURE;
      }
      break;
    }

    int should_exit = 0;
    process_line(line, &exit_code, &should_exit);
    if (should_exit) {
      break;
    }
  }

  free(line);
  return exit_code;
}
