// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir,note that the name of the ROOT dir is "".
}

chfs_client::inum
chfs_client::n2i(std::string n)
{//a function that is used to convert a string to the corresponding inode num.
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{//a function that is used to change the inum to the corresponding string, which is useful when storing the mapping-pairs.
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{//used to check whether a file is a regular file.
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

/**parent:parent dir 's inode num.
 * name:inum-mapping name(the name that is used to hard link to the NEW (different from the slinked one) inode.)
 * slink:symbolic link name(the name/path that is directed to the slinked file's inode.)
 * ino_out:a inum that is used to store the new inum of the newly created file.
 * */
int
chfs_client::createsymlink(inum parent, const char* name, const char* slink, inum& ino_out)
{
    int r = OK;
    bool checked = false;
    lookup(parent, name, checked, ino_out);
    if(checked == true) return EXIST;//the slink has already existed.
    if(ec->create(extent_protocol::T_SYMLINK, ino_out) != OK) return IOERR;//allocate a new inode number to the symbolic link file.
    ec->put(ino_out, std::string(slink));//put the cotent(mapped file name) into the newly allocated file.
    std::string buf;
    ec->get(parent, buf);
    buf += std::string(name);
    buf += ":";
    buf += filename(ino_out);
    buf += ",";
    ec->put(parent, buf);//add a new pair in the parent file(dir) content.
    //ec->put(ino_out, std::string(slink));//put the slink path(file) name as the slink file content into the corresponding inode.
    return r;
}

/*inum:the slink-corresponding inode number.*/
bool 
chfs_client::issymlink(inum inum)
{//used to check whether a file is a symbolic link file.
    extent_protocol::attr attr;
    extent_protocol::status ret = ec->getattr(inum, attr);
    if(ret != extent_protocol::OK) return false;
    if(attr.type == extent_protocol::T_SYMLINK) return true;
    return false; 
}

/*inum:file's inode number,buf:the buffer used to store the got data.*/
int
chfs_client::readlink(inum inum, syminfo& sym)
{//used to get the linked file name and other basic attributs that will be used by the FUSE level.
    int r = OK;
    std::string buf;
    if(ec->get(inum, buf) != OK) return IOERR;
    sym.slink = buf;
    extent_protocol::attr a;
    if(ec->getattr(inum, a) != extent_protocol::OK) return IOERR;
    sym.atime = a.atime;
    sym.ctime = a.ctime;
    sym.mtime = a.mtime;
    sym.size = a.size;
    return r;
}

bool
chfs_client::isdir(inum inum)
{//used to check whether a file is a dir.
    // Oops! is this still correct when you implement symlink?
    //return ! isfile(inum);
    extent_protocol::attr attr;
    extent_protocol::status ret = ec->getattr(inum, attr);
    if(ret != extent_protocol::OK) return false;
    if(attr.type == extent_protocol::T_DIR) return true;
    return false;    
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{//similar to the readlink() function.
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int
chfs_client::getsymlink(inum inum, syminfo& sym) {
	int r = OK;
	extent_protocol::attr a;
	if(ec->getattr(inum, a) != extent_protocol::OK) return IOERR;
	sym.size = a.size;
	sym.atime = a.atime;
	sym.mtime = a.mtime;
	sym.ctime = a.ctime;
	return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{//mainly used to reset the size and the content of the file.
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    ec->get(ino, buf);
    if(buf.size() == size) return r;//equal, return directly.
    /*if(buf.size() > size) {//smaller, append more bytes
    	int more = size - buf.size();
	for(int i = 0; i < more; ++i) buf += "\0";
	ec->put(ino, buf);
	return r;
    }
    buf = buf.substr(0, size);//bigger, truncate the current string to a shorter one.*/
    if(buf.size() > size) {//truncate
    	buf = buf.substr(0, size);
	ec->put(ino, buf);
	return r;
    }
    //buf.resize(size);
    std::string tmp = std::string(size, '\0');//size is larger, add '\0' to fill the "holes".
    for(int i = 0; i < buf.size(); ++i) tmp[i] = buf[i];
    buf = tmp;
    ec->put(ino, buf);
    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    //what's the mode used to do??
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string target = std::string(name);
    if(target.find('/') != std::string::npos || target.find('\0') != std::string::npos) return -1;//detect the filename is illegal, return directly.
    bool checked = false;
    lookup(parent, name, checked, ino_out);//check whether the name existing in the FS.
    if(checked == true) return EXIST;
    ec->create(extent_protocol::T_FILE, ino_out);//create the new file
    std::string buf;
    //ec->get(parent, buf);//read the parent file(dir) content.
    if(ec->get(parent, buf) != OK) return IOERR;
    //change the parent information.
    buf += std::string(name);
    buf += ":";
    buf += filename(ino_out);
    buf += ",";//change the parent content according to the dir format.
    ec->put(parent, buf);//put the changed parent's content bakc to the inode. 
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{//similar to the implementation of the create() function.
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool checked = false;
    lookup(parent, name, checked, ino_out);
    if(checked == true) return EXIST;//handle the case that the dir has existed.
    ec->create(extent_protocol::T_DIR, ino_out);
    std::string buf;
    ec->get(parent, buf);
    buf += std::string(name);
    buf += ":";
    buf += filename(ino_out);
    buf += ",";//add new pair
    ec->put(parent, buf);
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;//set the found to the false at the start.
    std::string target = std::string(name);
    std::string buf;
    ec->get(parent, buf);//get the content of the parent dir.
    if(buf.size() == 0) return r;//if the inode is empty, return directly.
    std::string tmp = buf;
    //if(tmp.size() != 0) tmp -= ',';
    while(true) {
    	int pos = tmp.find_first_of(',');
	std::string s1 = tmp.substr(0, pos);
	int pos1 = s1.find(':');
	std::string s2 = s1.substr(0, pos1);
	std::string s3 = s1.substr(pos1 + 1);
	ino_out = atoi(s3.c_str());
	if(s2 == target) {
	    found = true;
	    break;//find the target file name, break directly.
	}
	if(pos == tmp.size() - 1) break;//meet the end of the string, aka the last ','.
	tmp = tmp.substr(pos + 1);
	
    }
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    //printf("\tim: readdir %d\n", dir);
    int r = OK;
    /*my chosen format of the dir content: filename:inum,filename:inum,filename:inum,.....*/
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    ec->get(dir, buf);//get the info of the dir file.
    if(buf.size() == 0) return r;
    std::string tmp = buf;
    while(true) {
    	int pos = tmp.find_first_of(',');
	std::string s1 = tmp.substr(0, pos);
	int pos1 = s1.find(':');
	std::string s2 = s1.substr(0, pos1);
	std::string s3 = s1.substr(pos1 + 1);
	struct dirent ent;
	ent.name = s2;
	ent.inum = n2i(s3);
	list.push_back(ent);//add the got pair into the list.
	if(pos == tmp.size() - 1) break;
	tmp = tmp.substr(pos + 1);
    }
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    //printf("\tim: chfs->read: size: %d, off: %d\n", size, off);
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    ec->get(ino, buf);
    if(off > buf.size()) {
    	data = "";
	return r;
    }//the case that the off > size, can get nothing.
    if(off + size <= buf.size()) data = buf.substr(off, size);
    if(off + size > buf.size()) {
    	//if(off > buf.size()) data = "";
	data = buf.substr(off);
    }
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    //printf("\tim: chfs->write: size: %d, off: %d\n", size, off);
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    ec->get(ino, buf);
    int cur = buf.size();
    if(off + size <= buf.size()) {
    	bytes_written = size;
	for(int i = off; i < off + size; ++i) buf[i] = data[i - off];
    }
    else {
    	/*if(off < buf.size()) {
	    //int cur = buf.size();
	    bytes_written = size;
	    for(int i = off; i < buf.size(); ++i) buf[i] = data[i - off];
	    for(int i = cur; i <= off + size; ++i) buf += data[i - off];
	}
	else {//off >= buf.size()
	    for(int i = buf.size(); i < off; ++i) buf += '\0';
	    for(int i = 0; i < size; ++i) buf += data[i];
	    //bytes_written = off + size - cur;
	    bytes_written = size;
	}*/
	//buf.resize(off + size);
	std::string tmp = std::string(off + size, '\0');
	for(int i = 0; i < buf.size(); ++i) tmp[i] = buf[i];
	buf = tmp;
	for(int i = off; i < off + size; ++i) buf[i] = data[i - off];
	bytes_written = size;
    }
    ec->put(ino, buf);//put the changed content back to the inode.
    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    printf("\tchfs->unlink:name: %s\n", name);
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool checked = false;
    inum inum;
    lookup(parent, name, checked, inum);
    if(checked == false) return NOENT;//the file is not found in the parent dir, return directly.
    ec->remove(inum);
    std::string buf;
    ec->get(parent, buf);
    printf("\told-got buf=%s\n", buf.c_str());
    /*buf = buf.substr(0, buf.size() - 1);
    int pos = buf.find_last_of(',');
    buf = buf.substr(0, pos + 1);*/
    int start = buf.find(name);//find the start of the remove file content in the dir content.
    std::string left_part = buf.substr(0, start);
    buf = buf.substr(start + 1);
    int end = buf.find(',');//find the end of the content that will be deleted.
    std::string right_part = buf.substr(end + 1);
    buf = "";
    buf = left_part + right_part;
    printf("got new buf= %s\n", buf.c_str());
    ec->put(parent, buf);//put the changed content back to the inode.
    return r;
}

