#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>

#define READ_IO_BUFFER_SIZE 64*1024
#define MAX_SUPPORTED_FILE_PARTS 100000  

#define LOG_ERR     0
#define LOG_WARN    1
#define LOG_INFO    2
#define LOG_DBG     3
#define LOG_VERBOSE 4

#define LOG_CUR     4 

void TraceLog(int logLevel, const char *fmt, ...)
{
    const char *strLogLevel[] = {"ERR","WARN", "INFO", "DBG", "VERBOSE"};

    char szDataBufferTitle[32] = {0};

    char szDataBuffer[1024] = {0};   
    va_list ap;

    if (logLevel > LOG_CUR) {
        return;
    }  

    sprintf(szDataBufferTitle, "[%s]", strLogLevel[logLevel]);
    strcat(szDataBuffer, szDataBufferTitle);

    va_start(ap, fmt);
    vsnprintf(&szDataBuffer[strlen(szDataBufferTitle)], 1024, fmt, ap);
    va_end(ap);

    printf("%s", szDataBuffer);
}

int GetFileSize(FILE *fp)
{
    int fileSize = 0;

    int nRet = 0;

    nRet = fseek(fp, 0, SEEK_END);

    if (nRet == -1) {
        return 0;
    }

    fileSize = ftell(fp);

    if (fileSize == -1) {
        printf("ftell failed with %d\n", errno);
        fileSize = 0;
    }

    return fileSize;
} 

int SplitFile(const char *filePath, const char *baseFileName, int nMaxBytesPerFile)
{
    int ret = 0;
    FILE *fp = NULL;
    FILE *fpNew = NULL;
    int fileSize = 0;
    int lastPartSize = 0; 
    int i = 0;
    int nFilesCount = 0; 
    int nStartOffset = 0;
    int nBytesToRead = 0;
    int nBytesRead = 0;

    char szNewFileName[256] = {0};
    char szNewFilePath[256] = {0};

    char szDataBuffer[READ_IO_BUFFER_SIZE] = {0};

    if (nMaxBytesPerFile == 0) {
        return -1;
    }

    sprintf(szNewFilePath, "%s/%s", filePath, baseFileName);

    fp = fopen(szNewFilePath, "rb");

    if (!fp) {
        TraceLog(LOG_ERR, "failed open file %s, errno = %d, line = %d\n", szNewFilePath, errno, __LINE__);
        return -1;  
    } 

    fileSize = GetFileSize(fp);

    if (nMaxBytesPerFile >= fileSize) {
        // No need to split the file
        TraceLog(LOG_VERBOSE, "not split, fileSize: %d Max bytes per file: %d\n", fileSize, nMaxBytesPerFile);
        return 0;
    } 

    nFilesCount = (fileSize + 1) / nMaxBytesPerFile + 1; 

    lastPartSize = fileSize % nMaxBytesPerFile; 


    for (i = 0; i < nFilesCount; i++) {

        int splitFailed = 0;

        if (i == nFilesCount - 1) {
            nBytesToRead = lastPartSize;
        } else {
            nBytesToRead = nMaxBytesPerFile;
        }

        sprintf(szNewFilePath, "%s/%s.%d", filePath, baseFileName, i); 

        fpNew = fopen(szNewFilePath, "w+");
        if (!fpNew) {
            TraceLog(LOG_ERR, "failed to open %s for write, error = %d\n", szNewFilePath, errno);
            ret = -1;
            break;
        } 

        nStartOffset = i * nMaxBytesPerFile;

        fseek(fp, nStartOffset, SEEK_SET); 

        while (nBytesToRead > 0) {
            int tmpRet = 0;
            
            int nSubBytesToRead = 0;

            if (nBytesToRead > READ_IO_BUFFER_SIZE) { 
                nSubBytesToRead = READ_IO_BUFFER_SIZE;
            } else {
                nSubBytesToRead = nBytesToRead;
                
                // just in case the buffer is larger than the max bytes per file
                if (nSubBytesToRead > nMaxBytesPerFile) {
                    nSubBytesToRead = nMaxBytesPerFile;
                }
            }

            tmpRet = fread(szDataBuffer, 1, nSubBytesToRead, fp);

            if (tmpRet == 0 ) {
                TraceLog(LOG_ERR, "fread failed with error: %d\n", errno);
                splitFailed = 1;
                break;
            }

            TraceLog(LOG_DBG, "fread read %d, returned %d bytes\n", nSubBytesToRead, tmpRet);

            tmpRet = fwrite(szDataBuffer, 1, nSubBytesToRead, fpNew);
            if (tmpRet != nSubBytesToRead) {
                TraceLog(LOG_ERR, "fwrite failed with error: %d\n", errno);
                break;
            }

            TraceLog(LOG_DBG, "fwrite write %d, returned %d bytes\n", nSubBytesToRead, tmpRet);

            nBytesToRead -= nSubBytesToRead;      
        }

        fclose(fpNew);
        fpNew = NULL;

        // Failed, need to exit right now
        if (splitFailed == 1) {
            ret = 1;
            break;
        }
    } 

    fclose(fp); 

    return ret;
}

int MergeFiles(const char *filePath, const char *baseFileName, bool createNewFile)
{
    FILE *fp = NULL;
    int ret = 0;

    char szFilePath[256] = {0};
    char szDataBuffer[READ_IO_BUFFER_SIZE] = {0};

    int i = 0;

    if (createNewFile) {

        sprintf(szFilePath, "%s/%s", filePath, baseFileName);

        fp = fopen(szFilePath, "w+");

        if (!fp) {
            TraceLog(LOG_ERR, "fopen failed, error = %d\n", errno);
            return -1;
        }
    }


    for (i = 0; i < MAX_SUPPORTED_FILE_PARTS; i++) {

        bool bFailed = false;

        int nReadRecord = 0;

        int nBytesToRead = 0;

        int nBytesWritten = 0;

        FILE *fpExisting = NULL;

        sprintf(szFilePath, "%s/%s.%d", filePath, baseFileName, i);    
        
        fpExisting = fopen(szFilePath, "r+");

        if (!fpExisting) {
            TraceLog(LOG_INFO, "file: %s not exists\n", szFilePath);
            // no more part existing, just break out
            break;
        }

        if (!createNewFile && i == 0) {
            // Use the first one as the destination file
            fp = fpExisting;
            continue;
        }

        // Always make the file position is set to the END of file
        fseek(fp, 0, SEEK_END); 
 
        do { 
            nBytesToRead = fread(szDataBuffer, 1, READ_IO_BUFFER_SIZE, fpExisting); 

            if (nBytesToRead <= 0) {
                TraceLog(LOG_INFO, "fread read 0, errno = %d\n", errno);
                break;
            }

            nBytesWritten = fwrite(szDataBuffer, 1, nBytesToRead, fp);

            if (nBytesWritten != nBytesToRead) {
                TraceLog(LOG_ERR, "fwrite failed, err = %d\n", errno);
                bFailed = true;
                break;
            }  

        } while (1);

        fclose(fpExisting);
        fpExisting = NULL;

        if (bFailed) {
            ret = -1;
            break;
        }
    }

    if (createNewFile) {

        fclose(fp);
        fp = NULL;
    } 

    return ret;
}

void PrintUsage()
{
    TraceLog(LOG_ERR, "Usage: splitter <split|merge> <filePath> <fileName> [<split file size>]\n");
}

int main(int argc, char *argv[])
{

#define MAX_SPLIT_FILE_SZE 49 * 1024 * 1024 

    int ret = 0;

    int nOneFileSize = MAX_SPLIT_FILE_SZE;
   
    if (argc < 4) {

        PrintUsage();
        return -1;
    }

    TraceLog(LOG_INFO, "Split file path: %s name: %s\n", argv[1], argv[2]);

    if (argc >= 5) {
        nOneFileSize = atoi(argv[4]);

        if (nOneFileSize <= 0) {
            nOneFileSize = MAX_SPLIT_FILE_SZE;
        }
    }

    if (strcmp(argv[1], "split") == 0) {

        // Split
        SplitFile(argv[2], argv[3], nOneFileSize);

    } else if (strcmp(argv[1], "merge") == 0) {

        // Merge
        MergeFiles(argv[2], argv[3], true);

    } else {

        PrintUsage();
    }     
 
    return ret;

#undef MAX_SPLIT_FILE_SZE
}


