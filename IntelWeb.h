#ifndef INTELWEB_H_
#define INTELWEB_H_

#include "DiskMultiMap.h"
#include "InteractionTuple.h"
#include <string>
#include <vector>

class IntelWeb
{
public:
	IntelWeb();
	~IntelWeb();
	bool createNew(const std::string& filePrefix, unsigned int maxDataItems);
	bool openExisting(const std::string& filePrefix);
	void close();
	bool ingest(const std::string& telemetryFile);
	unsigned int crawl(const std::vector<std::string>& indicators,
		unsigned int minPrevalenceToBeGood,
		std::vector<std::string>& badEntitiesFound,
		std::vector<InteractionTuple>& interactions
		);
	bool purge(const std::string& entity);

private:
	// Your private member declarations will go here
	DiskMultiMap m_forwardMultiMap;
	DiskMultiMap m_reverseMultiMap;
	std::string m_name1;
	std::string m_name2;
	DiskMultiMap::Iterator f_Iter;
	DiskMultiMap::Iterator r_Iter;
	int IntelWeb::count(std::string& value);
};

#endif // INTELWEB_H_
