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
  ERRNO_STATUS_MASK = 0xFF,
  SHOW_ELAPSED_TIME = 0,  // set to 1 to show elapsed time after each command
  NULL_CHAR = '\0',
  NEWLINE_CHAR = '\n',
  CARRIAGE_RETURN_CHAR = '\r',
  READWRITE_MODE = 0666
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

static void print_elapsed_time(const struct timespec* t_start) {
  struct timespec t_end;
  if (clock_gettime(CLOCK_MONOTONIC, &t_end) == -1) {
    perror("clock_gettime");
    t_end.tv_sec = t_start->tv_sec;
    t_end.tv_nsec = t_start->tv_nsec;
  }

  struct timespec t_diff;
  timespec_diff(t_start, &t_end, &t_diff);
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
    close_fds(in_fd, out_fd);
    return EXIT_FAILURE;
  }

  if (pid == 0) {
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
    int err = errno;
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
    print_elapsed_time(&t_start);
  }

  close_fds(in_fd, out_fd);

  return return_code;
}

static int handle_builtin_cd(int argc, char** argv) {
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

static int handle_builtin_exit(
    int* exit_code, int argc, char** argv, int* should_exit
) {
  *should_exit = 1;
  if (argc > 1) {
    *exit_code = parse_exit_status(argv[1]);
  } else {
    *exit_code = EXIT_SUCCESS;
  }
  return 1;
}

static int handle_builtin_command(
    int argc, char** argv, int* should_exit, int* exit_code
) {
  if (argc == 0) {
    *exit_code = EXIT_SUCCESS;
    return 1;
  }

  if (strcmp(argv[0], EXIT_COMMAND) == 0) {
    return handle_builtin_exit(exit_code, argc, argv, should_exit);
  }

  if (strcmp(argv[0], CD_COMMAND) == 0) {
    *exit_code = handle_builtin_cd(argc, argv);
    return 1;
  }

  return 0;
}

static int handle_single_redirection(
    int* i_ptr,
    char** argv,
    int argc,
    int* skip_mask,
    char** in_file,
    char** out_file,
    int* seen_in,
    int* seen_out
) {
  char* token = argv[*i_ptr];

  if ((token[0] == '>' && token[1] == '>') ||
      (token[0] == '<' && token[1] == '<')) {
    return -1;
  }

  char** file_ptr = (token[0] == '>') ? out_file : in_file;
  int* seen_ptr = (token[0] == '>') ? seen_out : seen_in;

  if (*seen_ptr) {
    return -1;
  }

  if (token[1] != NULL_CHAR) {
    *file_ptr = token + 1;
    skip_mask[*i_ptr] = 1;
  } else {
    if (*i_ptr + 1 >= argc || argv[*i_ptr + 1][0] == '>' ||
        argv[*i_ptr + 1][0] == '<') {
      return -1;
    }
    *file_ptr = argv[*i_ptr + 1];
    skip_mask[*i_ptr] = 1;
    skip_mask[*i_ptr + 1] = 1;
    (*i_ptr)++;
  }
  *seen_ptr = 1;
  return 0;
}

static int parse_redirections(
    char** argv, int argc, char** in_file, char** out_file, int* skip_mask
) {
  int seen_in = 0;
  int seen_out = 0;

  for (int i = 0; i < argc; ++i) {
    char* token = argv[i];
    if (token[0] == '>' || token[0] == '<') {
      if (handle_single_redirection(
              &i, argv, argc, skip_mask, in_file, out_file, &seen_in, &seen_out
          ) != 0) {
        return -1;
      }
    }
  }
  return 0;
}

static int open_redirection_files(
    char* in_file, int* in_fd, char* out_file, int* out_fd
) {
  *in_fd = -1;
  *out_fd = -1;
  if (in_file) {
    *in_fd = open(in_file, O_RDONLY);
    if (*in_fd == -1) {
      if (printf("I/O error\n") < 0) {
        perror("printf");
      }
      return EXIT_FAILURE;
    }
  }
  if (out_file) {
    *out_fd = open(out_file, O_WRONLY | O_CREAT | O_TRUNC, READWRITE_MODE);
    if (*out_fd == -1) {
      if (*in_fd != -1) {
        close(*in_fd);
      }
      if (printf("I/O error\n") < 0) {
        perror("printf");
      }
      return EXIT_FAILURE;
    }
  }
  return EXIT_SUCCESS;
}

static int run_external_command(int argc, char** argv) {
  char* in_file = NULL;
  char* out_file = NULL;
  int skip_mask[MAX_TOKENS] = {0};

  if (parse_redirections(argv, argc, &in_file, &out_file, skip_mask) != 0) {
    if (printf("Syntax error\n") < 0) {
      perror("printf");
    }
    return EXIT_FAILURE;
  }

  char* argv_exec[MAX_TOKENS + 1];
  int exec_idx = 0;
  for (int i = 0; i < argc; ++i) {
    if (!skip_mask[i]) {
      argv_exec[exec_idx++] = argv[i];
    }
  }
  argv_exec[exec_idx] = NULL;

  if (exec_idx == 0) {
    if (printf("Syntax error\n") < 0) {
      perror("printf");
    }
    return EXIT_FAILURE;
  }

  int in_fd = 0;
  int out_fd = 0;
  if (open_redirection_files(in_file, &in_fd, out_file, &out_fd) !=
      EXIT_SUCCESS) {
    return EXIT_FAILURE;
  }

  return execute_external_fds(argv_exec, in_fd, out_fd);
}

static int run_single_command(int argc, char** tokens, int* should_exit) {
  int exit_code = 0;
  if (handle_builtin_command(argc, tokens, should_exit, &exit_code)) {
    return exit_code;
  }
  return run_external_command(argc, tokens);
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
