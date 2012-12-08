/* This code is based on the fine code written by Joseph Pfeiffer for his
   fuse system tutorial. */

#include "s3fs.h"
#include "libs3_wrapper.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>

#define GET_PRIVATE_DATA ((s3context_t *) fuse_get_context()->private_data)

/*
 * For each function below, if you need to return an error,
 * read the appropriate man page for the call and see what
 * error codes make sense for the type of failure you want
 * to convey.  For example, many of the calls below return
 * -EIO (an I/O error), since there are no S3 calls yet
 * implemented.  (Note that you need to return the negative
 * value for an error code.)
 */



int add_object(const char * path, s3dirent_t * object){
//Only adds to parent directory, doesn't create the new object under pathname
    s3context_t * ctx = GET_PRIVATE_DATA;
    char * temp = dirname(strdup(path));
    
    printf("Adding %s to %s!\n", path, temp);
    //Get parent directory object from s3
    s3dirent_t * retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, (const char *)temp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }
    //Find size/number of entries
    int objsize = rv;
    int numdirent = objsize / sizeof(s3dirent_t);

    //Search for a free entry and check to make sure dir doesn't exist
    int i = 0;
    int freeentry = -1;
    for (; i < numdirent; i++){
	if (retrieved_object[i].name == path){
		free(retrieved_object);
		return -EEXIST; }
	else if (retrieved_object[i].use == 0){
		freeentry = i; }
	}
    int newsize;
    //If have a free entry, place object into this spot
     if (freeentry >= 0){
	retrieved_object[freeentry] = *object;
	newsize = sizeof(s3dirent_t) * numdirent;
	}
    //If not, allocate more memory and place in directory
    else {
	newsize = sizeof(s3dirent_t)*(numdirent + 1);
	retrieved_object = realloc(retrieved_object, newsize);
	retrieved_object[numdirent] = *object;
	}

    retrieved_object[0].size += sizeof(s3dirent_t);

    //Put parent directory back on s3 with changes made
    rv = s3fs_put_object((const char *)ctx->s3bucket, (const char *)temp, (const uint8_t *)retrieved_object, newsize);
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
	free(retrieved_object);
	return -1;
    } else {
        printf("Successfully put test object in s3 (s3fs_put_object)\n");
    }  
    printf("added.!!!!!!!!!!!!!!!");

    free(retrieved_object);

    return 0;  
}


/* 
 * Get file attributes.  Similar to the stat() call
 * (and uses the same structure).  The st_dev, st_blksize,
 * and st_ino fields are ignored in the struct (and 
 * do not need to be filled in).
 */

int fs_getattr(const char *path, struct stat *statbuf) {
    fprintf(stderr, "fs_getattr(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Get parent directory
    s3dirent_t *retrieved_object = NULL;
    char * temp = dirname(strdup(path));
    printf("***************\n", temp);
   printf("Gettin attributes!\n");
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, (const char *)temp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object); free(temp);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    //Find size/number of entries
    int objsize = rv;
    int numdirent = objsize / sizeof(s3dirent_t);
    int i = 0;

    //Search for object in directory
    int found = -1;
    for(; i < numdirent; i++){
	if (retrieved_object[i].name == path){
		found = i;
		break; }}
    if (found == -1){ 
	free(retrieved_object); free(temp);
	return -ENOENT; }

	//now have object- if it's a directory, the metadata is in it's own object. So go get that instead
	// Places dir object in retrieved_object instead, found now = 0
    if (retrieved_object[found].type == 'd'){
	found = 0;
	retrieved_object = NULL;
    	ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&retrieved_object, 0, 0);
    	if (rv < 0) {
    	    printf("Failure in s3fs_get_object\n");
	    free(retrieved_object); free(temp);
	    return -ENOENT;
   	 } else {
   	     printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
   	 }
    }

    //Put metadata into statbuf
    statbuf->st_mode = retrieved_object[found].mode;
    statbuf->st_uid = retrieved_object[found].user;
    statbuf->st_gid = retrieved_object[found].group;
    statbuf->st_size = retrieved_object[found].size;
    statbuf->st_atime = retrieved_object[found].a_time.tv_sec;
    statbuf->st_mtime = retrieved_object[found].m_time.tv_sec;
    statbuf->st_ctime = retrieved_object[found].c_time.tv_sec;

    free(retrieved_object); free(temp);
    printf("AtTrIbUtEs!\n");
    return 0;
}


/* 
 * Create a file "node".  When a new file is created, this
 * function will get called.  
 * This is called for creation of all non-directory, non-symlink
 * nodes.  You *only* need to handle creation of regular
 * files here.  (See the man page for mknod (2).)
 */
int fs_mknod(const char *path, mode_t mode, dev_t dev) {
    fprintf(stderr, "fs_mknod(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char * temp = strdup(path);

    //Create new node for directory, with metadata
    s3dirent_t * newentry = malloc(sizeof(s3dirent_t));
    strcpy(newentry->name, path);
    newentry->type = 'f';
    newentry->use = 1;
    newentry->mode = mode;
    newentry->user = getuid();
    newentry->group = getgid();
    newentry->size = sizeof(s3dirent_t);
    newentry->block = 0;
    newentry->blocksize = 0;
    newentry->devid = 0;
    gettimeofday(&newentry->a_time, NULL);
    gettimeofday(&newentry->m_time, NULL);
    gettimeofday(&newentry->c_time, NULL);

    //Add to parent directory
    int x = add_object(path, newentry);
    if (x == 0) { printf("Successfully modified parent directory\n"); }
    else {printf("Failed to modify parent directory\n");
	free(temp); free(newentry);
	return -1; }

    //Create buffer, set it to empty
    char * buffer = NULL;

    //Put buffer onto s3
    ssize_t rv = s3fs_put_object((const char *)ctx->s3bucket, path, (const uint8_t *)buffer, 0);
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
	free(temp); free(newentry);
	return -1;
    } else {
        printf("Successfully put test object in s3 (s3fs_put_object)\n");
    }  
    
    free(temp); free(newentry);
    return 1;
}

/* 
 * Create a new directory.
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits (for setting in the metadata)
 * use mode|S_IFDIR.
 */
int fs_mkdir(const char *path, mode_t mode) {
    fprintf(stderr, "fs_mkdir(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    mode |= S_IFDIR;
    printf("MAKING DIRECTORY %s\n", path);

    //Create new node for the directory in which the new dir is to be placed
    //No metadata here
    s3dirent_t * newentry = malloc(sizeof(s3dirent_t));
    strcpy(newentry->name, path);
    newentry->type = 'd';
    newentry->use = 1;
    newentry->mode = mode;
    newentry->user = 0;
    newentry->group = 0;
    newentry->size = 0;
    newentry->block = 0;
    newentry->blocksize = 0;
    newentry->devid = 0;
    gettimeofday(&newentry->a_time, NULL);
    gettimeofday(&newentry->m_time, NULL);
    gettimeofday(&newentry->c_time, NULL);

    char * temp = strdup(path);

    //Call helper function to add directory to parent dir
    int x = add_object(path, newentry); 
    if (x == 0) { printf("Success adding object\n"); }//success 
    else {printf("failed to add object\n");
	free(newentry); free(temp);
	return -1; }

    //Create standalone directory object, including metadata
    s3dirent_t * newdir = malloc(sizeof(s3dirent_t));
    strcpy(newdir->name, ".");
    newdir->type = 'd';
    newdir->use = 1;
    newdir->mode = mode;
    newdir->user = getuid();
    newdir->group = getgid();
    newdir->size = sizeof(s3dirent_t);
    newdir->block = 0;
    newdir->blocksize = 0;
    newdir->devid = 0;
    gettimeofday(&newdir->a_time, NULL);
    gettimeofday(&newdir->m_time, NULL);
    gettimeofday(&newdir->c_time, NULL);

    //Put object on s3
    ssize_t rv = s3fs_put_object((const char *)ctx->s3bucket, path, (const uint8_t *)newdir, sizeof(s3dirent_t));
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
	free(newdir); free(newentry); free(temp);
	return -1;
    } else {
        printf("Successfully put object in s3 (s3fs_put_object)\n");
    }  
    printf("Dir made.**********\n");
    free(newdir); free(newentry); free(temp);
 
    return 0;
}

/*
 * Remove a file.
 */
int fs_unlink(const char *path) {
    fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Get parent directory from s3
    char * temp = strdup(path);
    temp = dirname(temp);
    s3dirent_t * retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, (const char *)temp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(temp);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    //Change use bit so can be replaced
    int objsize = rv;
    int numdirent = objsize / sizeof(s3dirent_t);
    int i = 0;
    for (; i < numdirent; i++){
	if (retrieved_object[i].name == path){
		retrieved_object[i].use = 0;
		break; }
	}
    if (retrieved_object[i].type != 'f') {	
	free(temp); 
	return -1; }

    //Fix size of parent directory to reflect number of current objects
    retrieved_object[0].size = retrieved_object[0].size - sizeof(s3dirent_t);

    rv = s3fs_put_object((const char *)ctx->s3bucket, (const char *)temp, (const uint8_t *)retrieved_object, sizeof(s3dirent_t *));
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
	free(temp);
	return -1;
    } else {
        printf("Successfully put object in s3 (s3fs_put_object)\n");
    }  

    //Remove file itself
    if (s3fs_remove_object((const char *)ctx->s3bucket, path) < 0) {
        printf("Failure to remove object (s3fs_remove_object)\n");
	free(temp);
	return -1;
    } else {
        printf("Success in removing object (s3fs_remove_object)\n");
    }
    
    free(temp);
    return 0;
}

/*
 * Remove a directory. 
 */
int fs_rmdir(const char *path) {
    fprintf(stderr, "fs_rmdir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //get directory from s3 
    s3dirent_t *cur_dir = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&cur_dir, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(cur_dir);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    int cdsize = rv;
    int numentries = cdsize / sizeof(s3dirent_t);

   //ensure there are no entries in directory- cannot delete if there are
    if (numentries > 1) {
	free(cur_dir);
	return -ENOTEMPTY;
	}
    else if (cur_dir[0].type != 'd'){
	free(cur_dir);
	return -ENOTDIR;
	}

   //if none, remove object
    if (s3fs_remove_object(ctx->s3bucket, path) < 0) {
        printf("Failure to remove test object (s3fs_remove_object)\n");
	free(cur_dir);
	return -1;
    } else {
        printf("Success in removing test object (s3fs_remove_object)\n");
    }
    //get parent directory to remove from there
    char * temp = strdup(path);
    temp = dirname(temp);

    s3dirent_t *retrieved_object = NULL;
    ssize_t rv2 = s3fs_get_object((const char *)ctx->s3bucket, (const char *)temp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv2 < 0) {
        printf("Failure in s3fs_get_object\n");
	free(cur_dir); free(temp); free(retrieved_object);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }
    
    int objsize = rv2;
    int numdirent = objsize / sizeof(s3dirent_t);
    int j = 0;

    //Find directory in parent
    //change use to 0 so it can be used for another object
    for (; j < numdirent; j++){
	if (retrieved_object[j].name == path){
		retrieved_object[j].use = 0; 
		break;	}
	}

    //put back on s3
    rv = s3fs_put_object((const char *)ctx->s3bucket, path, (const uint8_t *)retrieved_object, sizeof(s3dirent_t)*numdirent);
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
	free(cur_dir); free(temp); free(retrieved_object);
	return -1;
    } else {
        printf("Successfully put object in s3 (s3fs_put_object)\n");
    } 

    free(cur_dir); free(temp); free(retrieved_object);
    return 0;
}

/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath) {
    fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char * oldtemp = strdup(path);
    char * newtemp = strdup(newpath);
    newtemp = dirname(newtemp);

    //get parent directory of the old to change this accordingly
    s3dirent_t *retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, (const char *)oldtemp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);  free(oldtemp);  free(newtemp);
	return -ENOENT; } 

    int objsize = rv;
    int numdirent = objsize/sizeof(s3dirent_t);

    //find object in parent, put object in new s3dirent_t
    s3dirent_t * new = malloc(sizeof(s3dirent_t));
    int i = 0;
    for (; i< numdirent; i++){
	if (retrieved_object[i].name == path){
		*new = retrieved_object[i]; 
		break; }
	}
	
    //if they are in the same parent directory, just rename
    if (newtemp == oldtemp){
	strcpy(retrieved_object[i].name, newpath); }
    //If the new name also has a new directory
    else {
	strcpy(new->name, newpath);
	retrieved_object[i].use = 0;
	retrieved_object[i].size = retrieved_object[i].size - sizeof(s3dirent_t);

        //Add renamed object to it's proper directory with the same information as before
	int x = add_object(newpath, new);
	if (x == 0){
		printf("Sucessfully added new object (add_object)\n");
	} else {
		printf("Failure in add_object\n");  
		free(retrieved_object);  free(new);  free(oldtemp);  free(newtemp);
		return -1;}
	}

	//Put changes onto s3
	rv = s3fs_put_object((const char *)ctx->s3bucket, (const char *)oldtemp, (const uint8_t *)retrieved_object, sizeof(s3dirent_t)*numdirent);
   	 if (rv < 0) {
    	    printf("Failure in s3fs_put_object\n");
	    free(retrieved_object);  free(new);  free(oldtemp);  free(newtemp);
	    return -1;
   	 } else {
   	     printf("Successfully put object in s3 (s3fs_put_object)\n");
   	 }
	
    //get path object in order to rename the object itself
    uint8_t * object = NULL;
    rv = s3fs_get_object((const char *)ctx->s3bucket, path, &object, 0, 0);
    if (rv < 0) {
	printf("Failure in s3fs_get_object\n");
	free(retrieved_object);  free(new);  free(oldtemp);  free(newtemp);  free(object);
	return -ENOENT;
    } else {
  	printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
  	}

    //put the object up now renamed as the new pathname
    ssize_t rv2 = s3fs_put_object((const char *)ctx->s3bucket, newpath, object, rv);
    if (rv2 < 0) {
        printf("Failure in s3fs_put_object\n");
    } else {
        printf("Successfully put object in s3 (s3fs_put_object)\n");
    } 

    //remove the old object
    if (s3fs_remove_object((const char *)ctx->s3bucket, path) < 0){
	printf("Failure to remove object (s3fs_remove_object)\n"); 
	free(retrieved_object);  free(new);  free(oldtemp);  free(newtemp);  free(object);
	return -1;}
    else {
	printf("Successfully removed object (s3fs_remove_object)\n"); 
    }

    free(retrieved_object);  free(new);  free(oldtemp);  free(newtemp);  free(object);
    return 0;
}

/*
 * Change the permission bits of a file.
 */
int fs_chmod(const char *path, mode_t mode) {
    fprintf(stderr, "fs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the owner and group of a file.
 */
int fs_chown(const char *path, uid_t uid, gid_t gid) {
    fprintf(stderr, "fs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
   // s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the size of a file.
 */
int fs_truncate(const char *path, off_t newsize) {
    fprintf(stderr, "fs_truncate(path=\"%s\", newsize=%d)\n", path, (int)newsize);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char * temp = strdup(path);
    temp = dirname(temp);
    //Get the parent directory
    s3dirent_t *retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, temp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object); free(temp);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    //Get the file from s3
    char * file = NULL;
    rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&file, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);  free(temp);  free(file);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    //Find the file in the parent directory
    int objsize = rv;
    int numdirent = objsize/sizeof(s3dirent_t);
    int i = 0;
    for (; i<numdirent; i++){
	if (retrieved_object[i].name == path){
		break;	}	}
    char * new = malloc(newsize);

   //Make bigger or smaller depending on command
    if (retrieved_object[i].size < newsize) {
	file = realloc(file, newsize); }
    if (retrieved_object[i].size == newsize) { return 0; }
    else {
	int i = 0;
	for(; i<newsize; i++){
		new[i] = file[i]; }
	}
    retrieved_object[i].size = newsize;	

    //Put parent directory back on s3
    rv = s3fs_put_object((const char *)ctx->s3bucket, temp, (const uint8_t *)retrieved_object, sizeof(retrieved_object));
    if (rv < 0) {
	printf("Failure in s3fs_put_object\n");
	free(retrieved_object);  free(new);  free(file);  free(temp);
	return -1;
    } else {
   	printf("Successfully put object in s3 (s3fs_put_object)\n"); } 
    //Put file back on directory
    rv = s3fs_put_object((const char *)ctx->s3bucket, path, (const uint8_t *)new, newsize);
    if (rv < 0) {
	printf("Failure in s3fs_put_object\n");
	free(retrieved_object);  free(new);  free(file);  free(temp);
	return -1;
    } else {
   	printf("Successfully put object in s3 (s3fs_put_object)\n"); }      

    free(retrieved_object);  free(new);  free(file);  free(temp);
    return 0;
}

/*
 * Change the access and/or modification times of a file. 
 */
int fs_utime(const char *path, struct utimbuf *ubuf) {
    fprintf(stderr, "fs_utime(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/* 
 * File open operation
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  
 * 
 * Optionally open may also return an arbitrary filehandle in the 
 * fuse_file_info structure (fi->fh).
 * which will be passed to all file operations.
 * (In stages 1 and 2, you are advised to keep this function very,
 * very simple.)
 */
int fs_open(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_open(path\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Get file from s3 to see if it exists
    uint8_t *retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, path, &retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    free(retrieved_object);
    return 0;
}


/* 
 * Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  
 */
int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_read(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
	        path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char * temp = strdup(path);
    temp = dirname(temp);
    s3dirent_t * ret_obj = NULL;
    //Get parent directory
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, (const char *)temp, (uint8_t **)&ret_obj, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(ret_obj);  free(temp);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    int objsize = rv;
    int numdirent = objsize / sizeof(s3dirent_t);
    int k = -1;
    for (; k< numdirent; k++){
	if (ret_obj[k].name == path){
		break;	}	}
    if (k < 0) { 
	free(ret_obj);  free(temp);
	return -ENOENT;   }
    if (ret_obj[k].type != 'f') { return -EISDIR; }

    //Get file from s3
    char * retrieved_object = NULL;
    rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);  free(ret_obj);  free(temp);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    gettimeofday(&ret_obj[k].a_time, NULL); 

    //read retrieved_object into buf, starting at offset. 
    int i = offset;
    int j = 0;
    for (; i<(size+offset); i++){
	if (retrieved_object[i] == '\0'){
		break;	}
	buf[j] = retrieved_object[i];
	j++;
    }    

    //Put parent back on s3
    rv = s3fs_put_object((const char *)ctx->s3bucket, temp, (const uint8_t *)ret_obj, sizeof(s3dirent_t)*numdirent);
    if (rv < 0) {
	printf("Failure in s3fs_put_object\n");
	free(retrieved_object);  free(ret_obj);  free(temp);
	return -1;
    } else {
        printf("Successfully put object in s3 (s3fs_put_object)\n"); }  

    free(retrieved_object);  free(ret_obj);  free(temp);
    return j;
}

/*
 * Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.
 */
int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_write(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
	        path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char * temp = strdup(path);
    temp = dirname(temp);
    s3dirent_t * ret_obj = NULL;
    //Get parent directory
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, (const char *)temp, (uint8_t **)&ret_obj, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(ret_obj);  free(temp); 
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    int objsize = rv;
    int numdirent = objsize / sizeof(s3dirent_t);
    int k = -1;
    for (; k< numdirent; k++){
	if (ret_obj[k].name == path){
		break;	}	}
    if (k < 0) { return -ENOENT;   }
    if (ret_obj[k].type != 'd') { return -1; }

    //Get object from s3
    char *retrieved_object = NULL;
    rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(ret_obj);  free(temp);  free(retrieved_object);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n"); }
    gettimeofday(&ret_obj[k].a_time, NULL); 

    //Make proper size, using truncate
    if ((size + offset) > ret_obj[k].size){
	fs_truncate((const char *)retrieved_object, (size + offset));  }
	
    //Write starting at offset
    int i = offset;
    int j = 0;
    for(; i<(size+offset); i++){
	if (buf[j] == '\0'){
		break;	}
	retrieved_object[i] = buf[j];
	j++;
    }

    ret_obj[k].size = sizeof(retrieved_object);
    gettimeofday(&ret_obj[k].m_time, NULL); 
    
    //Put back on s3
    rv = s3fs_put_object((const char *)ctx->s3bucket, path, (const uint8_t *)retrieved_object, sizeof(retrieved_object));
   	 if (rv < 0) {
    	    printf("Failure in s3fs_put_object\n");
	    free(ret_obj);  free(temp);  free(retrieved_object);
	    return -1;
   	 } else {
   	     printf("Successfully put test object in s3 (s3fs_put_object)\n");
   	 }    

    //Put parent directory back
    rv = s3fs_put_object((const char *)ctx->s3bucket, (const char *)temp, (const uint8_t *)ret_obj, sizeof(retrieved_object));
   	 if (rv < 0) {
    	    printf("Failure in s3fs_put_object\n");
	    free(ret_obj);  free(temp);  free(retrieved_object);
	    return -1;
   	 } else {
   	     printf("Successfully put test object in s3 (s3fs_put_object)\n");
   	 }


    free(ret_obj);  free(temp);  free(retrieved_object);
    return j;
}


/* 
 * Possibly flush cached data for one file.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 */
int fs_flush(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_flush(path=\"%s\", fi=%p)\n", path, fi);
   // s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.  
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 */
int fs_release(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_release(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Synchronize file contents; any cached data should be written back to 
 * stable storage.
 */
int fs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_fsync(path=\"%s\")\n", path);
	return 1;

}

/*
 * Open directory
 *
 * This method should check if the open operation is permitted for
 * this directory
 */
int fs_opendir(const char *path, struct fuse_file_info *fi) {
    //fprintf(stderr, "fs_opendir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Check if directory exists. Return error if not.
    uint8_t *retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, path, &retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    free(retrieved_object);
    return 0;
}

/*
 * Read directory.  See the project description for how to use the filler
 * function for filling in directory items.
 */
int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{
    fprintf(stderr, "fs_readdir(path=\"%s\", buf=%p, offset=%d)\n",
	        path, buf, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Get the object from s3- know it exists because it was opened, but check again just in case
    s3dirent_t *retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    int objsize = rv;

    int numdirent = objsize / sizeof(s3dirent_t);
    int i = 0;
    //put information into buffer from the directory info
    for (; i<numdirent ; i++){
	if(filler(buf, retrieved_object[i].name, NULL, 0) != 0) {
		return -ENOMEM;
		}
	}
    free(retrieved_object);
    return 0;
}

/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_releasedir(path=\"%s\")\n", path);
   // s3context_t *ctx = GET_PRIVATE_DATA;
   //No state saved, so nothing to do- just return success
    
    return 0;
}

/*
 * Synchronize directory contents; cached data should be saved to 
 * stable storage.
 */
int fs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_fsyncdir(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Initialize the file system.  This is called once upon
 * file system startup.
 */
void *fs_init(struct fuse_conn_info *conn)
{
    fprintf(stderr, "fs_init --- initializing file system.\n");
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Clear all contents
    if (s3fs_clear_bucket(ctx->s3bucket) < 0) {
        printf("Failed to clear bucket (s3fs_clear_bucket)\n");
    } else {
        printf("Successfully cleared the bucket (removed all objects)\n");
    }

    //Create root directory 
    s3dirent_t * newentry = (s3dirent_t *)malloc(sizeof(s3dirent_t));
    strcpy(newentry->name, ".");
    newentry->type = 'd';
    newentry->use = 1;
    newentry->mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
    newentry->user = getuid();
    newentry->group = getgid();
    newentry->size = sizeof(s3dirent_t);
    newentry->block = 0;
    newentry->blocksize = 0;
    newentry->devid = 0;
    gettimeofday(&newentry->a_time, NULL); 
    gettimeofday(&newentry->m_time, NULL);
    gettimeofday(&newentry->c_time, NULL);

    //put onto s3
    ssize_t rv = s3fs_put_object((const char *)(ctx->s3bucket), "/", (const uint8_t *)newentry, (sizeof(s3dirent_t)));
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
	free(newentry);
	return NULL;
    } else {
        printf("Successfully put object in s3 (s3fs_put_object)\n");
    } 
    free(newentry);
    
    return ctx; 
}

/*
 * Clean up filesystem -- free any allocated data.
 * Called once on filesystem exit.
 */
void fs_destroy(void *userdata) {
    fprintf(stderr, "fs_destroy --- shutting down file system.\n");
    free(userdata);
    return;
}

/*
 * Check file access permissions.  For now, just return 0 (success!)
 * Later, actually check permissions (don't bother initially).
 */
int fs_access(const char *path, int mask) {
    fprintf(stderr, "fs_access(path=\"%s\", mask=0%o)\n", path, mask);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Change the size of an open file.  Very similar to fs_truncate (and,
 * depending on your implementation), you could possibly treat it the
 * same as fs_truncate.
 */
int fs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_ftruncate(path=\"%s\", offset=%d)\n", path, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
   
    char * temp = strdup(path);
    temp = dirname(temp);
    //Get the parent directory
    s3dirent_t *retrieved_object = NULL;
    ssize_t rv = s3fs_get_object((const char *)ctx->s3bucket, temp, (uint8_t **)&retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object); free(temp);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    //Get the file from s3
    char * file = NULL;
    rv = s3fs_get_object((const char *)ctx->s3bucket, path, (uint8_t **)&file, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
	free(retrieved_object);  free(temp);  free(file);
	return -ENOENT;
    } else {
        printf("Successfully retrieved object from s3 (s3fs_get_object)\n");
    }

    //Find the file in the parent directory
    int objsize = rv;
    int numdirent = objsize/sizeof(s3dirent_t);
    int i = 0;
    for (; i<numdirent; i++){
	if (retrieved_object[i].name == path){
		break;	}	}
    char * new = malloc(offset);

   //Make bigger or smaller depending on command
    if (retrieved_object[i].size < offset) {
	file = realloc(file, offset); }
    if (retrieved_object[i].size == offset) { return 0; }
    else {
	int i = 0;
	for(; i<offset; i++){
		new[i] = file[i]; }
	}
    retrieved_object[i].size = offset;	

    //Put parent directory back on s3
    rv = s3fs_put_object((const char *)ctx->s3bucket, temp, (const uint8_t *)retrieved_object, sizeof(retrieved_object));
    if (rv < 0) {
	printf("Failure in s3fs_put_object\n");
	free(retrieved_object);  free(new);  free(file);  free(temp);
	return -1;
    } else {
   	printf("Successfully put object in s3 (s3fs_put_object)\n"); } 
    //Put file back on directory
    rv = s3fs_put_object((const char *)ctx->s3bucket, path, (const uint8_t *)new, offset);
    if (rv < 0) {
	printf("Failure in s3fs_put_object\n");
	free(retrieved_object);  free(new);  free(file);  free(temp);
	return -1;
    } else {
   	printf("Successfully put object in s3 (s3fs_put_object)\n"); }      

    free(retrieved_object);  free(new);  free(file);  free(temp);
    return 0;
}

/*
 * The struct that contains pointers to all our callback
 * functions.  Those that are currently NULL aren't 
 * intended to be implemented in this project.
 */
struct fuse_operations s3fs_ops = {
  .getattr     = fs_getattr,    // get file attributes
  .readlink    = NULL,          // read a symbolic link
  .getdir      = NULL,          // deprecated function
  .mknod       = fs_mknod,      // create a file
  .mkdir       = fs_mkdir,      // create a directory
  .unlink      = fs_unlink,     // remove/unlink a file
  .rmdir       = fs_rmdir,      // remove a directory
  .symlink     = NULL,          // create a symbolic link
  .rename      = fs_rename,     // rename a file
  .link        = NULL,          // we don't support hard links
  .chmod       = fs_chmod,      // change mode bits
  .chown       = fs_chown,      // change ownership
  .truncate    = fs_truncate,   // truncate a file's size
  .utime       = fs_utime,      // update stat times for a file
  .open        = fs_open,       // open a file
  .read        = fs_read,       // read contents from an open file
  .write       = fs_write,      // write contents to an open file
  .statfs      = NULL,          // file sys stat: not implemented
  .flush       = fs_flush,      // flush file to stable storage
  .release     = fs_release,    // release/close file
  .fsync       = fs_fsync,      // sync file to disk
  .setxattr    = NULL,          // not implemented
  .getxattr    = NULL,          // not implemented
  .listxattr   = NULL,          // not implemented
  .removexattr = NULL,          // not implemented
  .opendir     = fs_opendir,    // open directory entry
  .readdir     = fs_readdir,    // read directory entry
  .releasedir  = fs_releasedir, // release/close directory
  .fsyncdir    = fs_fsyncdir,   // sync dirent to disk
  .init        = fs_init,       // initialize filesystem
  .destroy     = fs_destroy,    // cleanup/destroy filesystem
  .access      = fs_access,     // check access permissions for a file
  .create      = NULL,          // not implemented
  .ftruncate   = fs_ftruncate,  // truncate the file
  .fgetattr    = NULL           // not implemented
};



/* 
 * You shouldn't need to change anything here.  If you need to
 * add more items to the filesystem context object (which currently
 * only has the S3 bucket name), you might want to initialize that
 * here (but you could also reasonably do that in fs_init).
 */
int main(int argc, char *argv[]) {
    // don't allow anything to continue if we're running as root.  bad stuff.
    if ((getuid() == 0) || (geteuid() == 0)) {
    	fprintf(stderr, "Don't run this as root.\n");
    	return -1;
    }
    s3context_t *stateinfo = malloc(sizeof(s3context_t));
    memset(stateinfo, 0, sizeof(s3context_t));

    char *s3key = getenv(S3ACCESSKEY);
    if (!s3key) {
        fprintf(stderr, "%s environment variable must be defined\n", S3ACCESSKEY);
    }
    char *s3secret = getenv(S3SECRETKEY);
    if (!s3secret) {
        fprintf(stderr, "%s environment variable must be defined\n", S3SECRETKEY);
    }
    char *s3bucket = getenv(S3BUCKET);
    if (!s3bucket) {
        fprintf(stderr, "%s environment variable must be defined\n", S3BUCKET);
    }
    strncpy((*stateinfo).s3bucket, s3bucket, BUFFERSIZE);

    fprintf(stderr, "Initializing s3 credentials\n");
    s3fs_init_credentials(s3key, s3secret);

    fprintf(stderr, "Totally clearing s3 bucket\n");
    s3fs_clear_bucket(s3bucket);

    fprintf(stderr, "Starting up FUSE file system.\n");
    int fuse_stat = fuse_main(argc, argv, &s3fs_ops, stateinfo);
    fprintf(stderr, "Startup function (fuse_main) returned %d\n", fuse_stat);
    
    return fuse_stat;
}
