/*
 * FFindex
 * written by Andy Hauser <hauser@genzentrum.lmu.de>.
 * Please add your name here if you distribute modified versions.
 * 
 * FFindex is provided under the Create Commons license "Attribution-ShareAlike
 * 4.0", which basically captures the spirit of the Gnu Public License (GPL).
 * 
 * See:
 * http://creativecommons.org/licenses/by-sa/4.0/
 *
 * ffindex_apply
 * apply a program to each FFindex entry
*/

#define _GNU_SOURCE 1
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>

#include "ffindex.h"
#include "ffutil.h"


int main(int argn, char **argv)
{
  if(argn < 4)
  {
    fprintf(stderr, "USAGE: %s DATA_FILENAME INDEX_FILENAME OUT_DIR\n"
                    "\nDesigned and implemented by Andy Hauser <hauser@genzentrum.lmu.de>.\n",
                    argv[0]);
    return -1;
  }
  char *data_filename  = argv[1];
  char *index_filename = argv[2];
  char *out_dir = argv[3];

  FILE *data_file  = fopen(data_filename,  "r");
  FILE *index_file = fopen(index_filename, "r");

  if( data_file == NULL) { fferror_print(__FILE__, __LINE__, argv[0], data_filename);  exit(EXIT_FAILURE); }
  if(index_file == NULL) { fferror_print(__FILE__, __LINE__, argv[0], index_filename);  exit(EXIT_FAILURE); }

  size_t data_size;
  char *data = ffindex_mmap_data(data_file, &data_size);

  size_t entries = ffcount_lines(index_filename);
  ffindex_index_t* index = ffindex_index_parse(index_file, entries);
  if(index == NULL)
  {
    fferror_print(__FILE__, __LINE__, "ffindex_index_parse", index_filename);
    exit(EXIT_FAILURE);
  }

  if(chdir(out_dir) < 0){ fferror_print(__FILE__, __LINE__, argv[0], out_dir);  exit(EXIT_FAILURE); }

  size_t range_start = 0;
  size_t range_end = index->n_entries;
  size_t num_batches = 500;
  size_t batch_size = index->n_entries / num_batches; // Is size_t auto truncated like integers?

  printf("%ld entries, in %ld batches, batch_size: %ld\n", index->n_entries, num_batches, batch_size);

  char *content = NULL;
  char *content_ = NULL;
  size_t num_allocated = 0;
  size_t batch_end = 0;

  for(size_t entry_index=range_start; entry_index < range_end; entry_index+=batch_size) {
    content = NULL;
    content_ = NULL;
    num_allocated = 0;
    batch_end = entry_index + batch_size < range_end ? entry_index + batch_size : range_end;
    printf("%ld, from %ld to %ld\n", entry_index / batch_size, entry_index, batch_end);
    for (size_t i=entry_index; i<batch_end; ++i) {
      ffindex_entry_t *entry = ffindex_get_entry_by_index(index, i);
      if (NULL == content) {
//        printf("try to allocate content\n");
        content = malloc(sizeof(char) * entry->length-1);
        memcpy(content, data+entry->offset, entry->length-1);
//        printf("content allocated and copied\n");
      } else {
//        printf("try to reallocate content\n");
        content_ = realloc(content, num_allocated+entry->length-1);
        if (NULL == content_) {
          printf("failed to reallocate\n");
        }
        memcpy(content_+num_allocated, data+entry->offset, entry->length-1);
        content = content_;
      }
      num_allocated += entry->length - 1;
    }

    char output_filename[256];
    snprintf(output_filename, 256, "%ld.txt", entry_index / batch_size);
    FILE *output_file = fopen(output_filename, "w");
    if (NULL == output_file) {
      printf("Failed to create file %s, errno: %d, error: %s", output_filename, errno, strerror(errno));
      continue;
    }
    size_t written = fwrite(content, sizeof(char), num_allocated, output_file);
    printf("%ld, written: %ld\n", (entry_index/batch_size), written);
    fclose(output_file);

//    printf("content addr: %p, content_ addr: %p\n", content, content_);

    // content and content_ are very likely to point to the same addr
    // take it carefully
    free(content);

//    free(content_); // Don't free the same addr twice.
  }

  return 0;
}

/* vim: ts=2 sw=2 et
 */
