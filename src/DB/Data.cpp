
#include "Data.h"

using namespace MonoDB;

Data::Data(const Char* path, Uint64 space)
	:_path(path), _count(0), _spaceUsed(0), _spaceSize(space), _cacheEnabled(true) {
	struct dirent* currDir;
	DIR* rootDp = opendir(_path.c_str());
	if(rootDp) {
		while(currDir = readdir(rootDp)) {
			if(strcmp(currDir->d_name, ".") == 0 || strcmp(currDir->d_name, "..") == 0)
				continue;
			_count++;
		}
		closedir(rootDp);
	}
	else mkdir(_path.c_str(), 0755);

	_spaceUsed = _count * Data::DefaultFileSize;
	if(_count == 0) {
		std::string initPath = GetPath(0);
		fclose(fopen(initPath.c_str(), "wb"));
		_count++;
	}
	pthread_mutex_init(&_dataMutex, NULL);

	_cache = new Cache<Uint64, DataItem*>(Data::DefaultCacheSize);
	_cache->SetFinalizer(Hashmap<Uint64, DataItem*>::DefaultFreeFinalizer, NULL);
}
Data::~Data() {
	FREE(_cache);
	pthread_mutex_destroy(&_dataMutex);
}

std::string Data::GetPath(const Uint32 file) {
	std::stringstream outPath;
	outPath << _path << "db." << file << ".block";
	return outPath.str();
}
Uint32 Data::ReadFromFile(const Uint64 loc, OUTPUT NEW Byte** buffer) {
	Uint32 file = (Uint32)(loc >> (8 * sizeof(Uint32)));
	Uint32 offset = (Uint32)(loc);
	*buffer = NULL;
	
	std::string path = GetPath(file);
	FILE* fp = fopen(path.c_str(), "rb");
	if(fp) {
		Uint32 size = 0;
		fseek(fp, offset, SEEK_SET);
		fread(&size, sizeof(Uint32), 1, fp);
		if(size > 0 && buffer) {
			*buffer = new Byte[size];
			fread(*buffer, sizeof(Byte), size, fp);
		}
		fclose(fp);
		return size;
	}
	else return 0;
}
Uint64 Data::WriteToFile(Uint64 loc, Byte* buffer, Uint32 size) {
	Uint32 file = 0;
	Uint32 offset = 0;
	if(loc == NotFound) loc = NextLocation(size);
	
	file = (Uint32)(loc >> (8 * sizeof(Uint32)));
	offset = (Uint32)(loc);

	// TODO: check space limit
	///if(_spaceUsed > _spaceSize) return Fail;
	
	Uint32 blockSize = Data::DefaultHeaderSize + size;
	Byte* block = new Byte[blockSize];
	
	Block stream(block, blockSize);
	stream.Write((Byte*)&size, sizeof(Uint32));
	stream.Write(buffer, size);
	
	std::string dataPath = GetPath(file);
	FILE* dataFp = fopen(dataPath.c_str(), "rb+");
	if(dataFp) {
		fseek(dataFp, offset, SEEK_SET);
		fwrite(block, sizeof(Byte), blockSize, dataFp);
		fclose(dataFp);
	}
	else {
		dataFp = fopen(dataPath.c_str(), "wb");
		fwrite(block, sizeof(Byte), blockSize, dataFp);
		fclose(dataFp);
	}
	CLEAR(block);
	return loc;
}

Uint32 Data::Read(const Uint64 loc, OUTPUT NEW Byte** buffer) {
	pthread_mutex_lock(&_dataMutex);
	if(_cacheEnabled) {
		DataItem* item = NULL;
		if(_cache->Get(loc, &item)) {
			if(item->Size() > 0) {
				if(buffer) {
					*buffer = new Byte[item->Size()];
					memcpy(*buffer, item->Get(), item->Size());
				}
			}
			pthread_mutex_unlock(&_dataMutex);
			return item->Size();
		}
	}
	Uint32 size = ReadFromFile(loc, buffer);
	if(size > 0 && _cacheEnabled) {
		if(buffer) {
			DataItem* item = new DataItem(*buffer, size);
			_cache->Add(loc, item, size);
		}
	}
	pthread_mutex_unlock(&_dataMutex);
	return size;
}
Uint64 Data::Write(Byte* buffer, Uint32 size) {
	return Write(NotFound, buffer, size);
}
Uint64 Data::Write(Uint64 loc, Byte* buffer, Uint32 size) {
	pthread_mutex_lock(&_dataMutex);
	Uint64 result = WriteToFile(loc, buffer, size);
	if(_cacheEnabled && result != NotFound) {
		DataItem* item = new DataItem(buffer, size);
		_cache->Add(result, item, size);
	}
	pthread_mutex_unlock(&_dataMutex);
	return result;
}
Uint64 Data::NextLocation(Uint32 size) {
	Uint32 file = _count - 1;
	Uint32 offset = 0;

	std::string lastPath = GetPath(file);
	FILE* lastFp = fopen(lastPath.c_str(), "ab+");
	if(lastFp) {
		fseek(lastFp, 0, SEEK_END);
		offset = ftell(lastFp);
		fclose(lastFp);
	}		
	/// file overflow case
	if(offset + size > Data::DefaultFileSize) {
		file++;
		offset = 0;
		_count++;
		_spaceUsed += Data::DefaultFileSize;
	}
	Uint64 loc = file;
	loc = (loc << (8 * sizeof(Uint32))) + offset;
	return loc;
}
