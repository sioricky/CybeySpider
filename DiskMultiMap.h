#ifndef DISKMULTIMAP_H_
#define DISKMULTIMAP_H_

#include <string>
#include "MultiMapTuple.h"
#include"BinaryFile.h"
const int MAX_DATA_LENGTH = 120;

class DiskMultiMap
{
public:

	class Iterator
	{
	public:
		Iterator();
		Iterator(bool valid, BinaryFile::Offset nodeOff, DiskMultiMap* map);
		// You may add additional constructors
		bool isValid() const;
		Iterator& operator++();
		MultiMapTuple operator*();

	private:
		// Your private member declarations will go here
		bool m_Valid;
		BinaryFile::Offset m_Off;
		DiskMultiMap * m_map;
	};

	DiskMultiMap();
	~DiskMultiMap();
	bool createNew(const std::string& filename, unsigned int numBuckets);
	bool openExisting(const std::string& filename);
	void close();
	bool insert(const std::string& key, const std::string& value, const std::string& context);
	Iterator search(const std::string& key);
	int erase(const std::string& key, const std::string& value, const std::string& context);

private:
	// Your private member declarations will go here
	unsigned int m_buckets;
	struct Header
	{
		BinaryFile::Offset m_nBuckets;
		BinaryFile::Offset m_freeHead;
	};
	struct Node
	{
		char key[MAX_DATA_LENGTH + 1];
		char value[MAX_DATA_LENGTH + 1];
		char context[MAX_DATA_LENGTH + 1];
		BinaryFile::Offset next;
	};

	Header m_header;
	unsigned int HashFunction(const std::string& hashMe);
	BinaryFile m_hashTable;
	BinaryFile::Offset acquireNode();
};

#endif // DISKMULTIMAP_H_