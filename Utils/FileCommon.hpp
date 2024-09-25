//
// Created by anonymous on 6/10/23.
//

#ifndef OLVP_FILECOMMON_HPP
#define OLVP_FILECOMMON_HPP
#include <sys/stat.h>
#include <iostream>

class FileCommon
{
public:

    static size_t getFileSize(const char *fileName) {

        if (fileName == NULL) {
            return 0;
        }

        struct stat statbuf;

        stat(fileName, &statbuf);

        size_t filesize = statbuf.st_size;
        return filesize;
    }


};


#endif //OLVP_FILECOMMON_HPP
