#define _CRT_SECURE_NO_WARNINGS
#include "BinaryFile.h"
#include "DiskMultiMap.h"
#include "MultiMapTuple.h"
#include <iostream>
#include <functional>
#include <string>
using namespace std;
unsigned int DiskMultiMap::HashFunction(const std::string& hashMe)
{

	std::hash<std::string> str_hash;
	unsigned int hashValue = str_hash(hashMe);
	Header h_t;
	m_hashTable.read(h_t, 0);
	unsigned int nBuckets = h_t.m_nBuckets;
	unsigned int bucketNumber = hashValue % nBuckets;
	return 4 * bucketNumber + 8;

}

DiskMultiMap::DiskMultiMap()
{
	m_header.m_nBuckets = 0;
	m_header.m_freeHead = 0;
}

DiskMultiMap::~DiskMultiMap()
{
	m_hashTable.close();
}

bool DiskMultiMap::createNew(const std::string& filename, unsigned int numBuckets)
{
	m_hashTable.close();
	if (!m_hashTable.createNew(filename))
	{
		cout << "Errorrr creating new file!\n";
		return false;
	}
	Header h_t;
	h_t.m_nBuckets = numBuckets;
	h_t.m_freeHead = 0;
	if (!m_hashTable.write(h_t, 0))
	{
		cout << "Error writing Header to file!\n";
		return false;
	}
	for (int i = 0; i < numBuckets; i++)
	{
		BinaryFile::Offset curOff = m_hashTable.fileLength();
		BinaryFile::Offset iniOff = 0;
		if (!m_hashTable.write(iniOff, curOff))
		{
			cout << "Error writing Bucket offset to file!\n";
			return false;
		}
	}
	return true;
}

bool DiskMultiMap::openExisting(const std::string& filename)
{
	m_hashTable.close();
	if (!m_hashTable.openExisting(filename))
		return false;
	return true;
}

void DiskMultiMap::close()
{
	m_hashTable.close();
}

BinaryFile::Offset DiskMultiMap::acquireNode()
{
	Header h_t;
	m_hashTable.read(h_t, 0);
	if (h_t.m_freeHead == 0)
		return m_hashTable.fileLength();

	BinaryFile::Offset newNodeOff = h_t.m_freeHead;
	Node t_node;
	m_hashTable.read(t_node, newNodeOff);
	h_t.m_freeHead = t_node.next;
	m_hashTable.write(h_t, 0);
	return newNodeOff;
}

bool DiskMultiMap::insert(const std::string& key, const std::string& value, const std::string& context)
{
	if (key.size() > MAX_DATA_LENGTH || value.size() > MAX_DATA_LENGTH || context.size() > MAX_DATA_LENGTH)
		return false;

	BinaryFile::Offset NewOff = acquireNode();
	BinaryFile::Offset bucketOff = HashFunction(key);
	Node t_node;
	strcpy(t_node.key, key.c_str());
	strcpy(t_node.value, value.c_str());
	strcpy(t_node.context, context.c_str());
	m_hashTable.read(t_node.next, bucketOff);
	m_hashTable.write(t_node, NewOff);
	m_hashTable.write(NewOff, bucketOff);


}

DiskMultiMap::Iterator DiskMultiMap::search(const std::string & key)
{
	BinaryFile::Offset bucketOff = HashFunction(key);
	BinaryFile::Offset cur;
	m_hashTable.read(cur, bucketOff);
	if (cur == 0)
	{
		DiskMultiMap::Iterator it(false, 0, this);
		return it;
	}
	Node t_node;
	m_hashTable.read(t_node, cur);
	while (cur != 0)
	{
		if (strcmp(t_node.key, key.c_str()) == 0)
		{
			Iterator it(true, cur, this);
			return it;
		}
		cur = t_node.next;
		if (cur != 0)
			m_hashTable.read(t_node, t_node.next);
	}
	Iterator it(false, 0, this);
	return it;
}

int DiskMultiMap::erase(const std::string& key, const std::string& value, const std::string& context)
{
	int num = 0;
	BinaryFile::Offset bucketOff;
	bucketOff = HashFunction(key);
	Node t_node;
	BinaryFile::Offset cur;
	m_hashTable.read(cur, bucketOff);
	if (cur == 0)
		return num;
	m_hashTable.read(t_node, cur);
	BinaryFile::Offset pre = 0;
	do
	{
		BinaryFile::Offset next = t_node.next;
		if (strcmp(t_node.key, key.c_str()) == 0 && strcmp(t_node.value, value.c_str()) == 0 && strcmp(t_node.context, context.c_str()) == 0)
		{
			num++;
			if (pre == 0)
			{
				m_hashTable.write(next, bucketOff);//updata bucket
				Header h_t;
				m_hashTable.read(h_t, 0);
				t_node.next = h_t.m_freeHead;
				h_t.m_freeHead = cur;
				m_hashTable.write(t_node, cur);
				m_hashTable.write(h_t, 0);//update header					
				cur = next;
				if (cur == 0)
					break;
				else
				{
					m_hashTable.read(t_node, cur);//update cur_node
				}
			}
			else
			{
				Node preNode;
				m_hashTable.read(preNode, pre);
				preNode.next = next;
				m_hashTable.write(preNode, pre);//pre node points to next node

				Header h_t;
				m_hashTable.read(h_t, 0);
				t_node.next = h_t.m_freeHead;
				h_t.m_freeHead = cur;
				m_hashTable.write(h_t, 0);
				m_hashTable.write(t_node, cur);//update freelist, cur node points to freelist
				pre = cur;
				cur = next;
				if (cur == 0)
					break;
				else
					m_hashTable.read(t_node, cur);
			}

		}
		else
		{
			pre = cur;
			cur = next;
			if (cur == 0)
				break;
			else
				m_hashTable.read(t_node, cur);
		}
	} while (true);
	return num;
}
DiskMultiMap::Iterator::Iterator(bool valid, BinaryFile::Offset nodeOff, DiskMultiMap* map)
	:m_Valid(valid), m_Off(nodeOff), m_map(map)
{
}
DiskMultiMap::Iterator::Iterator()
	: m_Valid(false)
{}
bool DiskMultiMap::Iterator::isValid() const
{
	return m_Valid;
}

DiskMultiMap::Iterator& DiskMultiMap::Iterator::operator++()
{
	BinaryFile::Offset cur = m_Off;
	Node cur_node;
	Node t_node;
	m_map->m_hashTable.read(cur_node, cur);//read in current node;
	cur = cur_node.next;
	m_map->m_hashTable.read(t_node, cur);//start to read in next node;
	do
	{
		if (strcmp(t_node.key, cur_node.key) == 0)
		{
			m_Off = cur;
			return *this;//move to next valid iterator
		}
		cur = t_node.next;
		m_map->m_hashTable.read(t_node, cur);

	} while (t_node.next != 0);
	m_Valid = false;
	return *this;//change m_valid to false
}
MultiMapTuple DiskMultiMap::Iterator::operator*()
{
	MultiMapTuple m;
	if (isValid())
	{
		BinaryFile::Offset cur = m_Off;
		Node cur_node;
		m_map->m_hashTable.read(cur_node, cur);//read in current node;
		m.key = cur_node.key;
		m.context = cur_node.context;
		m.value = cur_node.value;
	}
	else
	{
		m.context = "";
		m.key = "";
		m.value = "";
	}
	return m;
}